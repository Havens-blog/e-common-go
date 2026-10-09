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

// TaskExecutor 任务执行器接口
type TaskExecutor interface {
	// Execute 执行任务
	Execute(ctx context.Context, task *Task) error

	// GetType 获取任务类型
	GetType() TaskType
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
