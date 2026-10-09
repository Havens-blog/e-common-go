# TaskX - 通用异步任务框架

## 概述

TaskX 是一个通用的异步任务框架，提供任务队列、任务执行、状态跟踪等完整功能。它独立于业务逻辑，可以在任何需要异步处理的场景中使用。

## 特性

- ✅ 基于 channel 的高性能任务队列
- ✅ 可配置的 worker 池
- ✅ 任务状态持久化
- ✅ 实时进度跟踪
- ✅ 插件化的执行器机制
- ✅ 优雅的启动和关闭
- ✅ 支持任务取消
- ✅ 完整的任务历史记录

## 快速开始

### 1. 创建任务仓储

```go
import (
    "github.com/Havens-blog/e-cam-service/pkg/taskx"
    "github.com/Havens-blog/e-cam-service/pkg/mongox"
)

// 使用 MongoDB 作为存储
db := &mongox.Mongo{...}
repo := taskx.NewMongoRepository(db, "tasks")

// 初始化索引
repo.InitIndexes(context.Background())
```

### 2. 创建任务队列

```go
import "github.com/gotomicro/ego/core/elog"

logger := elog.DefaultLogger

config := taskx.Config{
    WorkerNum:  5,   // 5个worker
    BufferSize: 100, // 缓冲100个任务
}

queue := taskx.NewQueue(repo, logger, config)
```

### 3. 实现任务执行器

```go
type MyTaskExecutor struct {
    // 依赖的服务
}

func (e *MyTaskExecutor) GetType() taskx.TaskType {
    return "my_task"
}

func (e *MyTaskExecutor) Execute(ctx context.Context, task *taskx.Task) error {
    // 1. 解析任务参数
    params := task.Params

    // 2. 执行业务逻辑
    // ...

    // 3. 更新进度（可选）
    repo.UpdateProgress(ctx, task.ID, 50, "处理中...")

    // 4. 设置任务结果
    task.Result = map[string]interface{}{
        "count": 100,
    }

    return nil
}
```

### 4. 注册执行器并启动队列

```go
// 注册执行器
executor := &MyTaskExecutor{}
queue.RegisterExecutor(executor)

// 启动队列
queue.Start()

// 程序退出时停止队列
defer queue.Stop()
```

### 5. 提交任务

```go
import "github.com/google/uuid"

task := &taskx.Task{
    ID:   uuid.New().String(),
    Type: "my_task",
    Params: map[string]interface{}{
        "key": "value",
    },
    CreatedBy: "user123",
}

err := queue.Submit(task)
if err != nil {
    log.Fatal(err)
}
```

### 6. 查询任务状态

```go
// 获取任务状态
task, err := queue.GetTaskStatus(taskID)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Status: %s, Progress: %d%%\n", task.Status, task.Progress)
```

## 核心概念

### 任务状态

- `pending` - 待执行
- `running` - 执行中
- `completed` - 已完成
- `failed` - 失败
- `cancelled` - 已取消

### 任务结构

```go
type Task struct {
    ID          string                 // 任务ID
    Type        TaskType               // 任务类型
    Status      TaskStatus             // 任务状态
    Params      map[string]interface{} // 任务参数
    Result      map[string]interface{} // 任务结果
    Error       string                 // 错误信息
    Progress    int                    // 进度 (0-100)
    Message     string                 // 当前消息
    CreatedBy   string                 // 创建者
    CreatedAt   time.Time              // 创建时间
    StartedAt   *time.Time             // 开始时间
    CompletedAt *time.Time             // 完成时间
    Duration    int64                  // 执行时长（秒）
}
```

### 任务执行器接口

```go
type TaskExecutor interface {
    Execute(ctx context.Context, task *Task) error
    GetType() TaskType
}
```

### 任务仓储接口

```go
type TaskRepository interface {
    Create(ctx context.Context, task Task) error
    GetByID(ctx context.Context, id string) (Task, error)
    Update(ctx context.Context, task Task) error
    UpdateStatus(ctx context.Context, id string, status TaskStatus, message string) error
    UpdateProgress(ctx context.Context, id string, progress int, message string) error
    List(ctx context.Context, filter TaskFilter) ([]Task, error)
    Count(ctx context.Context, filter TaskFilter) (int64, error)
    Delete(ctx context.Context, id string) error
}
```

## 高级用法

### 自定义仓储实现

如果不使用 MongoDB，可以实现自己的仓储：

```go
type MyRepository struct {
    // 你的存储实现
}

func (r *MyRepository) Create(ctx context.Context, task taskx.Task) error {
    // 实现创建逻辑
}

// 实现其他接口方法...
```

### 任务进度更新

在执行器中更新任务进度：

```go
func (e *MyTaskExecutor) Execute(ctx context.Context, task *taskx.Task) error {
    // 开始处理
    repo.UpdateProgress(ctx, task.ID, 10, "开始处理")

    // 处理中
    repo.UpdateProgress(ctx, task.ID, 50, "处理中...")

    // 即将完成
    repo.UpdateProgress(ctx, task.ID, 90, "即将完成")

    // 任务框架会自动设置为100%
    return nil
}
```

### 任务取消

```go
err := queue.CancelTask(taskID)
if err != nil {
    log.Printf("取消任务失败: %v", err)
}
```

### 查询任务列表

```go
filter := taskx.TaskFilter{
    Type:   "my_task",
    Status: taskx.TaskStatusCompleted,
    Limit:  20,
    Offset: 0,
}

tasks, total, err := queue.ListTasks(filter)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("找到 %d 个任务\n", total)
```

## 配置选项

### Worker 数量

根据任务类型选择合适的 worker 数量：

```go
// CPU 密集型任务
config := taskx.Config{
    WorkerNum: runtime.NumCPU(),
}

// IO 密集型任务
config := taskx.Config{
    WorkerNum: runtime.NumCPU() * 2,
}
```

### 队列缓冲大小

根据任务提交频率调整：

```go
config := taskx.Config{
    BufferSize: 1000, // 高频提交场景
}
```

## 最佳实践

### 1. 任务幂等性

确保任务可以安全重试：

```go
func (e *MyTaskExecutor) Execute(ctx context.Context, task *taskx.Task) error {
    // 使用唯一标识避免重复执行
    if alreadyProcessed(task.ID) {
        return nil
    }

    // 执行业务逻辑
    // ...

    return nil
}
```

### 2. 错误处理

区分可重试和不可重试错误：

```go
func (e *MyTaskExecutor) Execute(ctx context.Context, task *taskx.Task) error {
    err := doSomething()
    if err != nil {
        if isRetryable(err) {
            return fmt.Errorf("临时错误，可重试: %w", err)
        }
        return fmt.Errorf("永久错误: %w", err)
    }
    return nil
}
```

### 3. 资源清理

使用 defer 确保资源释放：

```go
func (e *MyTaskExecutor) Execute(ctx context.Context, task *taskx.Task) error {
    resource := acquireResource()
    defer resource.Release()

    // 执行业务逻辑
    // ...

    return nil
}
```

### 4. 超时控制

为任务设置超时：

```go
func (e *MyTaskExecutor) Execute(ctx context.Context, task *taskx.Task) error {
    ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
    defer cancel()

    // 执行业务逻辑
    // ...

    return nil
}
```

## 示例

完整示例请参考：

- `internal/cam/task/executor/sync_assets.go` - 同步资产执行器示例

## 性能

- 基于 channel 的高性能队列
- 支持并发执行
- 最小化锁竞争
- 高效的任务分发

## 限制

- 任务参数和结果必须可序列化为 JSON
- 单个任务不应该执行过长时间（建议 < 1 小时）
- 队列满时会拒绝新任务

## 未来计划

- [ ] 支持任务优先级
- [ ] 支持任务依赖
- [ ] 支持定时任务
- [ ] 支持任务重试策略
- [ ] 支持死信队列
- [ ] 支持任务链
- [ ] 提供 Prometheus 监控指标

## License

MIT
