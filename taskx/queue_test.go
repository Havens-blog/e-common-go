package taskx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotomicro/ego/core/elog"
)

// fakeRepo 内存任务仓储,满足 TaskRepository
type fakeRepo struct {
	mu    sync.Mutex
	tasks map[string]Task
}

func newFakeRepo(tasks ...Task) *fakeRepo {
	m := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		m[t.ID] = t
	}
	return &fakeRepo{tasks: m}
}

func (f *fakeRepo) Create(_ context.Context, task Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[task.ID] = task
	return nil
}

func (f *fakeRepo) GetByID(_ context.Context, id string) (Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return Task{}, fmt.Errorf("task not found: %s", id)
	}
	return t, nil
}

func (f *fakeRepo) Update(_ context.Context, task Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[task.ID] = task
	return nil
}

func (f *fakeRepo) UpdateStatus(_ context.Context, id string, status TaskStatus, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.tasks[id]
	if !ok {
		return fmt.Errorf("task not found: %s", id)
	}
	t.Status = status
	f.tasks[id] = t
	return nil
}

func (f *fakeRepo) UpdateProgress(_ context.Context, _ string, _ int, _ string) error { return nil }

func (f *fakeRepo) List(_ context.Context, filter TaskFilter) ([]Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Task, 0)
	for _, t := range f.tasks {
		if filter.Status != "" && t.Status != filter.Status {
			continue
		}
		out = append(out, t)
	}
	// 简单按创建时间排序,保证可重复
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].CreatedAt.Before(out[i].CreatedAt) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	if filter.Limit > 0 && int64(len(out)) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (f *fakeRepo) Count(_ context.Context, filter TaskFilter) (int64, error) {
	tasks, _ := f.List(context.Background(), filter)
	return int64(len(tasks)), nil
}

func (f *fakeRepo) Delete(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.tasks, id)
	return nil
}

// noopExecutor 立即完成的执行器
type noopExecutor struct{ taskType TaskType }

func (n *noopExecutor) Execute(_ context.Context, _ *Task) error { return nil }
func (n *noopExecutor) GetType() TaskType                        { return n.taskType }

func newTestTask(id string, status TaskStatus) Task {
	return Task{ID: id, Type: "test", Status: status, CreatedAt: time.Now()}
}

// 回归:恢复循环不能把同一批 pending 任务重复入队。
// 历史缺陷:每 5s 重扫 pending 且不去重,worker 被长时间任务占满时,
// 重复副本迅速灌满缓冲 channel(Buffer 100),此后所有 Submit 都报「任务队列已满」。
func TestRecoverPendingDoesNotDuplicate(t *testing.T) {
	repo := newFakeRepo(
		newTestTask("t1", TaskStatusPending),
		newTestTask("t2", TaskStatusPending),
		newTestTask("t3", TaskStatusPending),
	)
	q := NewQueue(repo, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 100})
	q.RegisterExecutor(&noopExecutor{taskType: "test"})

	// 模拟两个恢复周期(真实实现为 5s ticker)
	q.enqueuePendingBatch(context.Background())
	if got := len(q.taskChan); got != 3 {
		t.Fatalf("第一轮入队后 channel=%d, want 3", got)
	}
	q.enqueuePendingBatch(context.Background())
	if got := len(q.taskChan); got != 3 {
		t.Fatalf("第二轮重复恢复后 channel=%d, want 3(同一任务不得重复入队)", got)
	}
}

// 任务被 worker 取走执行后,允许该任务再次进入恢复流程
// (例如失败重试重新入队后再次落库 pending 的场景)
func TestEnqueueMarkReleasedOnExecute(t *testing.T) {
	repo := newFakeRepo()
	q := NewQueue(repo, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 100})
	q.RegisterExecutor(&noopExecutor{taskType: "test"})

	task := newTestTask("t1", TaskStatusPending)
	if err := q.Submit(&task); err != nil {
		t.Fatalf("Submit 失败: %v", err)
	}
	if !q.isEnqueued(task.ID) {
		t.Fatal("入队后应处于已标记状态")
	}

	// 模拟 worker 取走并执行
	q.executeTask(0, &task)
	if q.isEnqueued(task.ID) {
		t.Fatal("任务被执行后应解除标记")
	}
}

// 队列满时 Submit 报错,且不产生重复标记
func TestSubmitFullQueue(t *testing.T) {
	repo := newFakeRepo()
	q := NewQueue(repo, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 1})
	q.RegisterExecutor(&noopExecutor{taskType: "test"})

	t1 := newTestTask("t1", TaskStatusPending)
	if err := q.Submit(&t1); err != nil {
		t.Fatalf("第一次 Submit 不应失败: %v", err)
	}
	t2 := newTestTask("t2", TaskStatusPending)
	if err := q.Submit(&t2); err == nil {
		t.Fatal("队列满时 Submit 应报错")
	}
}

// countingExecutor 统计被调用次数,用于验证"任务恰执行一次"
type countingExecutor struct {
	taskType TaskType
	calls    int32
}

func (c *countingExecutor) Execute(_ context.Context, _ *Task) error {
	atomic.AddInt32(&c.calls, 1)
	return nil
}

func (c *countingExecutor) GetType() TaskType { return c.taskType }

func (c *countingExecutor) count() int { return int(atomic.LoadInt32(&c.calls)) }

// flakyRepo 让对 running 的认领失败 failTimes 次,模拟运行期 Mongo 抖动
type flakyRepo struct {
	*fakeRepo
	fmu         sync.Mutex
	failTimes   int
	statusCalls int
}

func (f *flakyRepo) UpdateStatus(ctx context.Context, id string, status TaskStatus, msg string) error {
	f.fmu.Lock()
	if status == TaskStatusRunning {
		f.statusCalls++
		if f.statusCalls <= f.failTimes {
			f.fmu.Unlock()
			return fmt.Errorf("模拟 Mongo 抖动: connection socket was unexpectedly closed: EOF")
		}
	}
	f.fmu.Unlock()
	return f.fakeRepo.UpdateStatus(ctx, id, status, msg)
}

func (f *flakyRepo) calls() int {
	f.fmu.Lock()
	defer f.fmu.Unlock()
	return f.statusCalls
}

// 回归:认领(running)一直失败时不得执行任务。
// 历史缺陷:UpdateStatus 失败仅记日志就继续跑,DB 里仍是 pending,
// 5s 后的恢复循环会把同一 task_id 再投一次 → 同一任务并发执行两次。
func TestExecuteTaskSkipsWhenClaimAlwaysFails(t *testing.T) {
	repo := newFakeRepo(newTestTask("t1", TaskStatusPending))
	flaky := &flakyRepo{fakeRepo: repo, failTimes: claimMaxAttempts}
	q := NewQueue(flaky, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 10})
	exec := &countingExecutor{taskType: "test"}
	q.RegisterExecutor(exec)

	task := newTestTask("t1", TaskStatusPending)
	q.executeTask(0, &task)

	if got := exec.count(); got != 0 {
		t.Fatalf("认领失败时不得执行,实测执行了 %d 次", got)
	}
	if got := flaky.calls(); got != claimMaxAttempts {
		t.Fatalf("认领尝试次数=%d, want %d", got, claimMaxAttempts)
	}
	got, _ := repo.GetByID(context.Background(), "t1")
	if got.Status != TaskStatusPending {
		t.Fatalf("认领失败后任务应保持 pending(等下一轮恢复),实际=%s", got.Status)
	}
	if q.isExecuting("t1") {
		t.Fatal("执行标记必须释放,否则恢复循环永远不敢再投这个任务")
	}
}

// 认领前 N 次失败、随后成功时,任务应恰好执行一次
func TestExecuteTaskRetriesClaimThenRunsOnce(t *testing.T) {
	repo := newFakeRepo(newTestTask("t1", TaskStatusPending))
	flaky := &flakyRepo{fakeRepo: repo, failTimes: 1}
	q := NewQueue(flaky, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 10})
	exec := &countingExecutor{taskType: "test"}
	q.RegisterExecutor(exec)

	task := newTestTask("t1", TaskStatusPending)
	q.executeTask(0, &task)

	if got := exec.count(); got != 1 {
		t.Fatalf("重试成功应恰好执行 1 次,实测 %d 次", got)
	}
	// 认领成功后 executeTask 会继续执行并落库 completed;不能停在 pending,
	// 否则恢复循环会把它当成未执行的任务再投一次
	got, _ := repo.GetByID(context.Background(), "t1")
	if got.Status != TaskStatusCompleted {
		t.Fatalf("认领成功后任务应推进到 completed,实际=%s", got.Status)
	}
}

// 正在执行(尚未落库 running)的任务不得被恢复循环重复入队
func TestEnqueuePendingBatchSkipsExecuting(t *testing.T) {
	repo := newFakeRepo(newTestTask("t1", TaskStatusPending))
	q := NewQueue(repo, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 10})
	q.RegisterExecutor(&noopExecutor{taskType: "test"})

	q.markExecuting("t1")
	q.enqueuePendingBatch(context.Background())
	if got := len(q.taskChan); got != 0 {
		t.Fatalf("执行中的任务被重复入队,channel=%d, want 0", got)
	}

	q.unmarkExecuting("t1")
	q.enqueuePendingBatch(context.Background())
	if got := len(q.taskChan); got != 1 {
		t.Fatalf("执行结束后应恢复入队,channel=%d, want 1", got)
	}
}

// 同一任务被重复投递到 worker 时,第二个副本必须被丢弃
func TestExecuteTaskDropsDuplicateWhileExecuting(t *testing.T) {
	repo := newFakeRepo(newTestTask("t1", TaskStatusPending))
	q := NewQueue(repo, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 10})
	exec := &countingExecutor{taskType: "test"}
	q.RegisterExecutor(exec)

	q.markExecuting("t1") // 模拟另一个 worker 正在执行
	task := newTestTask("t1", TaskStatusPending)
	q.executeTask(1, &task)
	q.unmarkExecuting("t1")

	if got := exec.count(); got != 0 {
		t.Fatalf("重复投递的副本不得执行,实测执行 %d 次", got)
	}
}

// blockingCreateRepo 让 Create 停在「已投递、正在落库」这一刻：
// 此时 worker 已经拿到同一个 *Task 并在执行，两边只要都碰这个对象就是竞争。
type blockingCreateRepo struct {
	*fakeRepo
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	gotMu   sync.Mutex
	got     Task
}

func (b *blockingCreateRepo) Create(ctx context.Context, task Task) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	b.gotMu.Lock()
	b.got = task
	b.gotMu.Unlock()
	return b.fakeRepo.Create(ctx, task)
}

func (b *blockingCreateRepo) stored() Task {
	b.gotMu.Lock()
	defer b.gotMu.Unlock()
	return b.got
}

// signalingExecutor 在执行器入口通知测试，并像真实执行器那样往任务上写结果。
type signalingExecutor struct {
	started chan struct{}
	once    sync.Once
}

func (s *signalingExecutor) Execute(_ context.Context, task *Task) error {
	s.once.Do(func() { close(s.started) })
	task.Result = map[string]interface{}{"done": true}
	return nil
}

func (s *signalingExecutor) GetType() TaskType { return "test" }

// 回归：任务交给队列之后，Submit 不得再读同一个 Task 对象。
// 历史缺陷：入队后仍用 *task 落库（queue.go 的 repo.Create(..., *task)），
// 与 worker 里的 task.Status = completed 构成数据竞争；
// 调用方（e-cam-service internal/cert/service）的集成测试在 CI -race 下抓到。
// 本用例只有在 -race 下才能真正判负：无竞争时它只校验落库内容取自投递瞬间的快照。
func TestSubmitDoesNotReadTaskAfterEnqueue(t *testing.T) {
	repo := &blockingCreateRepo{
		fakeRepo: newFakeRepo(newTestTask("t1", TaskStatusPending)),
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	exec := &signalingExecutor{started: make(chan struct{})}
	q := NewQueue(repo, elog.DefaultLogger, Config{WorkerNum: 1, BufferSize: 10})
	q.RegisterExecutor(exec)
	q.Start()
	defer q.Stop()

	task := newTestTask("t1", TaskStatusPending)
	done := make(chan error, 1)
	go func() { done <- q.Submit(&task) }()

	<-repo.entered // 已投递成功，正在落库
	<-exec.started // worker 已取走同一个任务并开始执行
	// 等 worker 把状态写回任务对象（executeTask 在 Execute 返回后写 completed），
	// 再放行落库：修复前 Submit 在这之后才读 *task，正好与这次写入冲突。
	time.Sleep(100 * time.Millisecond)
	close(repo.release)
	if err := <-done; err != nil {
		t.Fatalf("Submit 失败: %v", err)
	}

	// 落库的必须是提交那一刻的快照；若读到 worker 改写后的对象，状态就会漂移。
	if got := repo.stored(); got.Status != TaskStatusPending {
		t.Fatalf("落库状态被 worker 中途改写为 %q, want %q", got.Status, TaskStatusPending)
	}
}
