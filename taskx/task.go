package taskx

import (
	"context"
	"time"
)

// TaskType 任务类型
type TaskType string

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"   // 待执行
	TaskStatusRunning   TaskStatus = "running"   // 执行中
	TaskStatusCompleted TaskStatus = "completed" // 已完成
	TaskStatusFailed    TaskStatus = "failed"    // 失败
	TaskStatusCancelled TaskStatus = "cancelled" // 已取消
)

// Task 任务
type Task struct {
	ID          string                 `json:"id" bson:"_id"`                  // 任务ID
	Type        TaskType               `json:"type" bson:"type"`               // 任务类型
	Status      TaskStatus             `json:"status" bson:"status"`           // 任务状态
	Params      map[string]interface{} `json:"params" bson:"params"`           // 任务参数
	Result      map[string]interface{} `json:"result,omitempty" bson:"result"` // 任务结果
	Error       string                 `json:"error,omitempty" bson:"error"`   // 错误信息
	Progress    int                    `json:"progress" bson:"progress"`       // 进度 (0-100)
	Message     string                 `json:"message" bson:"message"`         // 当前消息
	CreatedBy   string                 `json:"created_by" bson:"created_by"`   // 创建者
	CreatedAt   time.Time              `json:"created_at" bson:"created_at"`   // 创建时间
	// 生命周期字段 bson 必须 omitempty:整结构体 Update 会以 $set 写回内存副本,
	// 若 nil/0 被写成 null,将抹掉 UpdateStatus 刚落库的开始/完成时间与时长
	StartedAt   *time.Time             `json:"started_at,omitempty" bson:"started_at,omitempty"`     // 开始时间
	CompletedAt *time.Time             `json:"completed_at,omitempty" bson:"completed_at,omitempty"` // 完成时间
	Duration    int64                  `json:"duration,omitempty" bson:"duration,omitempty"`         // 执行时长（秒）
	RetryCount  int                    `json:"retry_count,omitempty" bson:"retry_count"`   // 重试次数
	MaxRetries  int                    `json:"max_retries,omitempty" bson:"max_retries"`   // 最大重试次数（默认3）
}

// DefaultTaskTimeout 执行器未声明 TaskTimeoutProvider 时的任务预算。
//
// 队列必须有预算:worker 数量固定(默认 5),一个卡在厂商 API 长尾上的执行器
// 会永久吃掉 1/5 吞吐,五个挂住任务面即停摆,而日志里只有「开始执行」没有
// 「失败」——监控上一切正常,是最难排查的故障形态。
const DefaultTaskTimeout = 30 * time.Minute

// TaskExecutor 任务执行器接口
type TaskExecutor interface {
	// Execute 执行任务
	Execute(ctx context.Context, task *Task) error

	// GetType 获取任务类型
	GetType() TaskType
}

// TaskTimeoutProvider 可选接口:执行器按任务类型声明自己的执行预算。
//
// 队列用类型断言探测(未实现则回落 DefaultTaskTimeout),因此给 TaskExecutor
// 直接加方法会破坏所有既有实现,这里保持可选。返回 <=0 视为未声明。
type TaskTimeoutProvider interface {
	Timeout() time.Duration
}

// TaskRepository 任务仓储接口
type TaskRepository interface {
	// Create 创建任务
	Create(ctx context.Context, task Task) error

	// GetByID 根据ID获取任务
	GetByID(ctx context.Context, id string) (Task, error)

	// Update 更新任务
	Update(ctx context.Context, task Task) error

	// UpdateStatus 更新任务状态
	UpdateStatus(ctx context.Context, id string, status TaskStatus, message string) error

	// ClaimRunning 原子认领任务:仅当任务仍为 pending 时推进到 running,
	// 返回是否认领成功。
	//
	// 实现必须是条件更新(filter 同时匹配 _id 与 status=pending),不能是
	// 只按 _id 的盲写:盲写时两个副本都会认领成功,同一 task_id 被并发执行
	// 两次;Queue 进程内的 executing 标记只能挡住单副本内的重复投递。
	// 返回 (false, nil) 表示已被他人认领或状态已变,调用方应静默退场。
	ClaimRunning(ctx context.Context, id string) (bool, error)

	// UpdateProgress 更新任务进度
	UpdateProgress(ctx context.Context, id string, progress int, message string) error

	// List 获取任务列表
	List(ctx context.Context, filter TaskFilter) ([]Task, error)

	// Count 统计任务数量
	Count(ctx context.Context, filter TaskFilter) (int64, error)

	// Delete 删除任务
	Delete(ctx context.Context, id string) error
}

// TaskFilter 任务查询过滤器
type TaskFilter struct {
	Type      TaskType   `json:"type"`
	Status    TaskStatus `json:"status"`
	CreatedBy string     `json:"created_by"`
	StartDate time.Time  `json:"start_date"`
	EndDate   time.Time  `json:"end_date"`
	Offset    int64      `json:"offset"`
	Limit     int64      `json:"limit"`
}
