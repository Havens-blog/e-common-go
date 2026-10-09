package taskx

import (
	"context"
	"fmt"
	"sync"
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
