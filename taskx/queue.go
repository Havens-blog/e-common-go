package taskx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gotomicro/ego/core/elog"
)

// Queue 任务队列
type Queue struct {
	taskChan  chan *Task
	executors map[TaskType]TaskExecutor
	repo      TaskRepository
	logger    *elog.Component
	workerNum int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	// enqueued 已在 channel 中等待的任务ID。
	// DB pending 任务的恢复是周期性的,而任务状态只有被 worker 取走才会变更;
	// 不去重会把同一任务重复入队,迅速灌满缓冲 channel 并挤占新任务提交。
	enqueued sync.Map
}

// Config 任务队列配置
type Config struct {
	WorkerNum  int // worker 数量
	BufferSize int // 队列缓冲大小
}

// NewQueue 创建任务队列
func NewQueue(
	repo TaskRepository,
	logger *elog.Component,
	config Config,
) *Queue {
	if config.WorkerNum <= 0 {
		config.WorkerNum = 5 // 默认5个worker
	}

	if config.BufferSize <= 0 {
		config.BufferSize = 500 // 默认缓冲500个任务（增加以处理大量积压）
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Queue{
		taskChan:  make(chan *Task, config.BufferSize),
		executors: make(map[TaskType]TaskExecutor),
		repo:      repo,
		logger:    logger,
		workerNum: config.WorkerNum,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// RegisterExecutor 注册任务执行器
func (q *Queue) RegisterExecutor(executor TaskExecutor) {
	q.mu.Lock()
	defer q.mu.Unlock()

	taskType := executor.GetType()
	q.executors[taskType] = executor
	q.logger.Info("注册任务执行器",
		elog.String("task_type", string(taskType)))
}

// Start 启动任务队列
func (q *Queue) Start() {
	q.logger.Info("启动任务队列",
		elog.Int("worker_num", q.workerNum))

	// 启动worker
	for i := 0; i < q.workerNum; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}

	// 恢复 pending 和 running 状态的任务（服务重启后遗留的任务）
	q.recoverPendingTasks()
}

// Stop 停止任务队列
func (q *Queue) Stop() {
	q.logger.Info("停止任务队列")

	q.cancel()
	close(q.taskChan)
	q.wg.Wait()

	q.logger.Info("任务队列已停止")
}

// recoverPendingTasks 恢复 pending 和 running 状态的任务
func (q *Queue) recoverPendingTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	// 先处理遗留的 running 任务（一次性处理）
	filter := TaskFilter{
		Status: TaskStatusRunning,
		Limit:  1000,
	}
	runningTasks, err := q.repo.List(ctx, filter)
	if err != nil {
		q.logger.Error("查询 running 任务失败", elog.FieldErr(err))
	} else if len(runningTasks) > 0 {
		q.logger.Info("发现遗留的 running 任务，重置为 pending",
			elog.Int("count", len(runningTasks)))

		for _, task := range runningTasks {
			// 重置为 pending 状态
			if err := q.repo.UpdateStatus(ctx, task.ID, TaskStatusPending, "服务重启，任务重新排队"); err != nil {
				q.logger.Error("重置任务状态失败",
					elog.String("task_id", task.ID),
					elog.FieldErr(err))
				continue
			}
		}
		q.logger.Info("遗留 running 任务已重置为 pending", elog.Int("count", len(runningTasks)))
	}
	cancel()

	// 启动后台协程持续恢复 pending 任务
	q.wg.Add(1)
	go q.pendingTaskRecoverLoop()
}

// enqueuePendingBatch 把 DB 中尚未入队的 pending 任务投递一轮(每 tick 调一次)。
// 已在 channel 中的任务跳过,避免重复入队。
func (q *Queue) enqueuePendingBatch(ctx context.Context) {
	const batchSize = int64(100) // 每次处理100条

	filter := TaskFilter{
		Status: TaskStatusPending,
		Limit:  batchSize,
	}
	pendingTasks, err := q.repo.List(ctx, filter)
	if err != nil {
		q.logger.Error("查询 pending 任务失败", elog.FieldErr(err))
		return
	}
	if len(pendingTasks) == 0 {
		return
	}

	recovered := 0
	for i := range pendingTasks {
		task := pendingTasks[i]
		// 已在 channel 中等待的任务不再重复入队
		if q.isEnqueued(task.ID) {
			continue
		}

		// 检查是否已有对应的执行器
		q.mu.RLock()
		_, ok := q.executors[task.Type]
		q.mu.RUnlock()
		if !ok {
			continue
		}

		// 尝试入队（非阻塞）
		select {
		case q.taskChan <- &task:
			q.markEnqueued(task.ID)
			recovered++
		case <-q.ctx.Done():
			return
		default:
			// 队列满了，下次再试
		}
	}

	if recovered > 0 {
		q.logger.Info("本轮恢复任务", elog.Int("recovered", recovered))
	}
}

// markEnqueued 标记任务已入队
func (q *Queue) markEnqueued(id string) {
	q.enqueued.Store(id, struct{}{})
}

// unmarkEnqueued 取消入队标记(任务被 worker 取走时调用)
func (q *Queue) unmarkEnqueued(id string) {
	q.enqueued.Delete(id)
}

// isEnqueued 查询任务是否已在 channel 中等待
func (q *Queue) isEnqueued(id string) bool {
	_, ok := q.enqueued.Load(id)
	return ok
}

// pendingTaskRecoverLoop 持续从数据库恢复 pending 任务
func (q *Queue) pendingTaskRecoverLoop() {
	defer q.wg.Done()

	q.logger.Info("启动 pending 任务恢复协程")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			q.enqueuePendingBatch(ctx)
			cancel()

		case <-q.ctx.Done():
			q.logger.Info("pending 任务恢复协程退出")
			return
		}
	}
}

// Submit 提交任务
func (q *Queue) Submit(task *Task) error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	// 检查是否有对应的执行器
	if _, ok := q.executors[task.Type]; !ok {
		return fmt.Errorf("未找到任务类型 %s 的执行器", task.Type)
	}

	// 设置任务初始状态
	if task.Status == "" {
		task.Status = TaskStatusPending
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	// 先尝试入队列，避免队列满时仍写入DB产生垃圾数据
	select {
	case q.taskChan <- task:
		// 入队成功，再持久化到数据库
		if err := q.repo.Create(context.Background(), *task); err != nil {
			q.logger.Error("保存任务到数据库失败（任务已入队列）",
				elog.String("task_id", task.ID),
				elog.FieldErr(err))
		}
		q.markEnqueued(task.ID)

		q.logger.Info("提交任务",
			elog.String("task_id", task.ID),
			elog.String("task_type", string(task.Type)))
		return nil
	case <-q.ctx.Done():
		return fmt.Errorf("任务队列已关闭")
	default:
		return fmt.Errorf("任务队列已满")
	}
}

// worker 工作协程
func (q *Queue) worker(id int) {
	defer q.wg.Done()

	q.logger.Info("启动worker", elog.Int("worker_id", id))

	for {
		select {
		case task, ok := <-q.taskChan:
			if !ok {
				q.logger.Info("worker退出", elog.Int("worker_id", id))
				return
			}

			q.executeTask(id, task)

		case <-q.ctx.Done():
			q.logger.Info("worker收到停止信号", elog.Int("worker_id", id))
			return
		}
	}
}

// executeTask 执行任务
func (q *Queue) executeTask(workerID int, task *Task) {
	q.logger.Info("开始执行任务",
		elog.Int("worker_id", workerID),
		elog.String("task_id", task.ID),
		elog.String("task_type", string(task.Type)))

	// 任务已离开 channel,允许恢复流程在其重新落库 pending 时再次入队
	q.unmarkEnqueued(task.ID)

	// 更新任务状态为运行中
	if err := q.repo.UpdateStatus(context.Background(), task.ID, TaskStatusRunning, "任务开始执行"); err != nil {
		q.logger.Error("更新任务状态失败",
			elog.String("task_id", task.ID),
			elog.FieldErr(err))
	}

	// 获取执行器
	q.mu.RLock()
	executor, ok := q.executors[task.Type]
	q.mu.RUnlock()

	if !ok {
		q.logger.Error("未找到任务执行器",
			elog.String("task_id", task.ID),
			elog.String("task_type", string(task.Type)))

		q.repo.UpdateStatus(context.Background(), task.ID, TaskStatusFailed, "未找到任务执行器")
		return
	}

	// 执行任务
	ctx := context.Background()
	err := executor.Execute(ctx, task)

	if err != nil {
		q.logger.Error("任务执行失败",
			elog.String("task_id", task.ID),
			elog.Int("retry_count", task.RetryCount),
			elog.FieldErr(err))

		// 检查是否需要重试
		maxRetries := task.MaxRetries
		if maxRetries == 0 {
			maxRetries = 3 // 默认重试3次
		}

		if task.RetryCount < maxRetries {
			// 增加重试计数并重新入队
			task.RetryCount++
			task.Status = TaskStatusPending
			task.Error = err.Error()

			if updateErr := q.repo.Update(ctx, *task); updateErr != nil {
				q.logger.Error("更新任务重试计数失败",
					elog.String("task_id", task.ID),
					elog.FieldErr(updateErr))
			}

			// 重新入队
			select {
			case q.taskChan <- task:
				q.markEnqueued(task.ID)
				q.logger.Info("任务重新入队",
					elog.String("task_id", task.ID),
					elog.Int("retry_count", task.RetryCount),
					elog.Int("max_retries", maxRetries))
			case <-q.ctx.Done():
				q.logger.Warn("队列已关闭，任务无法重新入队",
					elog.String("task_id", task.ID))
			default:
				q.logger.Warn("任务队列已满，任务将延迟重试",
					elog.String("task_id", task.ID))
				// 队列满时，任务仍在数据库中，下次启动时会被恢复
			}
		} else {
			// 达到最大重试次数，标记为失败
			q.logger.Error("任务达到最大重试次数，标记为失败",
				elog.String("task_id", task.ID),
				elog.Int("retry_count", task.RetryCount))

			task.Status = TaskStatusFailed
			task.Error = fmt.Sprintf("重试%d次后仍失败: %s", task.RetryCount, err.Error())
			q.repo.UpdateStatus(ctx, task.ID, TaskStatusFailed, task.Error)
		}
	} else {
		q.logger.Info("任务执行成功",
			elog.String("task_id", task.ID))

		// 更新任务状态为完成
		task.Status = TaskStatusCompleted
		q.repo.UpdateStatus(ctx, task.ID, TaskStatusCompleted, "任务执行完成")
	}

	// 更新任务结果
	if err := q.repo.Update(ctx, *task); err != nil {
		q.logger.Error("更新任务结果失败",
			elog.String("task_id", task.ID),
			elog.FieldErr(err))
	}
}

// GetTaskStatus 获取任务状态
func (q *Queue) GetTaskStatus(taskID string) (*Task, error) {
	task, err := q.repo.GetByID(context.Background(), taskID)
	if err != nil {
		return nil, err
	}

	if task.ID == "" {
		return nil, fmt.Errorf("任务不存在")
	}

	return &task, nil
}

// ListTasks 获取任务列表
func (q *Queue) ListTasks(filter TaskFilter) ([]Task, int64, error) {
	tasks, err := q.repo.List(context.Background(), filter)
	if err != nil {
		return nil, 0, err
	}

	count, err := q.repo.Count(context.Background(), filter)
	if err != nil {
		return nil, 0, err
	}

	return tasks, count, nil
}

// CancelTask 取消任务
func (q *Queue) CancelTask(taskID string) error {
	// 获取任务
	task, err := q.repo.GetByID(context.Background(), taskID)
	if err != nil {
		return err
	}

	if task.ID == "" {
		return fmt.Errorf("任务不存在")
	}

	// 只能取消待执行的任务
	if task.Status != TaskStatusPending {
		return fmt.Errorf("只能取消待执行的任务，当前状态: %s", task.Status)
	}

	// 更新任务状态为已取消
	return q.repo.UpdateStatus(context.Background(), taskID, TaskStatusCancelled, "任务已取消")
}
