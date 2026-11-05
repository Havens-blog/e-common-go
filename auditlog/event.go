// Package auditlog 统一审计日志 SDK。
//
// 背景：系统内曾存在 4 套割裂的审计实现（e-cam-service 的 AuditLog/ChangeRecord/
// ChangeOrderAuditEntry 与 e-iam 的 OperationLog/AuthLog），操作者/时间/动作/结果
// 五类字段命名互不一致，无法跨域统一检索与出报告。本包提供统一的事件信封与写入
// SDK，让各服务的审计触发点（HTTP 中间件、资产同步、变更单、IAM 操作）产出同一
// 结构的审计事件。
//
// 设计权衡（为什么这样设计）：
//  1. 统一 envelope + 弱类型 Detail：不强行扁平化所有域字段，避免单集合几十个
//     空列；各域差异字段收进 Detail，由调用方保证 Detail 内键的语义。
//  2. 写入失败不阻塞业务：SDK 只返回 error，由调用方（或 NonBlocking 包装器）
//     决定降级策略——审计是旁路，不能因为审计挂掉而让业务请求失败。
//  3. 幂等：DedupKey 非空时依赖 MongoDB 唯一稀疏索引去重，重复 Emit 安全。
package auditlog

import (
	"errors"
	"fmt"
)

// ActorType 操作者类型：区分人与系统触发源。
type ActorType string

const (
	ActorTypeHuman     ActorType = "human"     // 人工操作（登录会话）
	ActorTypeSystem    ActorType = "system"    // 系统内部逻辑
	ActorTypeExecutor  ActorType = "executor"  // 任务执行器
	ActorTypeScheduler ActorType = "scheduler" // 定时调度
)

// Result 审计结果（统一三态，替代此前 Result/Status/Success*bool 三种表达）。
type Result string

const (
	ResultSuccess Result = "success"
	ResultFailed  Result = "failed"
	// ResultPartial 部分成功：联邦/批量操作至少一个源成功且至少一个失败。
	ResultPartial Result = "partial"
)

// 服务的合法取值（写入侧校验，防止 Service 字段被随意填字符串导致检索失真）。
const (
	ServiceCAM    = "e-cam-service"
	ServiceIAM    = "e-iam"
	ServiceCMDB   = "e-cmdb-service"
)

// 常用 Domain 取值（MVP 范围：audit / operation；后续 P1 接入 change 等）。
const (
	DomainAudit     = "audit"     // 操作审计（原 AuditLog）
	DomainOperation = "operation" // IAM 管理操作审计（原 OperationLog）
	DomainAuth      = "auth"      // 认证安全审计（原 AuthLog）
	DomainChange    = "change"    // 资产变更（原 ChangeRecord，P1）
)

// AuditEvent 统一审计事件。
//
// 字段分五组：谁（Who）/ 何时（When）/ 何处（Where）/ 做了什么（What）/
// 结果（Result）；各域差异字段收进 Detail 弱类型载荷。
type AuditEvent struct {
	// —— 谁（Who）——
	ActorType ActorType `json:"actor_type" bson:"actor_type"`
	// ActorID 统一用字符串承载：e-iam 的 OperatorID 是 int64、e-cam-service 是
	// string，统一为 string 才能在同一集合里跨服务检索。
	ActorID   string `json:"actor_id" bson:"actor_id"`
	ActorName string `json:"actor_name" bson:"actor_name"`

	// —— 何时（When）——
	At int64 `json:"at" bson:"at"` // Unix 毫秒

	// —— 何处（Where）——
	TenantID  int64  `json:"tenant_id" bson:"tenant_id"`
	ClientIP  string `json:"client_ip,omitempty" bson:"client_ip,omitempty"`
	UserAgent string `json:"user_agent,omitempty" bson:"user_agent,omitempty"`

	// —— 做了什么（What）——
	Service      string `json:"service" bson:"service"`                 // e-cam-service / e-iam / …
	Domain       string `json:"domain" bson:"domain"`                   // audit / operation / change / …
	Action       string `json:"action" bson:"action"`                   // create/update/delete/sync/execute/…
	ResourceType string `json:"resource_type" bson:"resource_type"`     // api/asset/user/role/change_order/…
	ResourceID   string `json:"resource_id,omitempty" bson:"resource_id,omitempty"`
	ResourceURN  string `json:"resource_urn,omitempty" bson:"resource_urn,omitempty"`

	// —— 结果（Result）——
	Result     Result `json:"result" bson:"result"`
	StatusCode int    `json:"status_code,omitempty" bson:"status_code,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty" bson:"duration_ms,omitempty"`

	// —— 详情（Detail，域扩展）——
	// 各域自己的结构化载荷。写入前会被 Sanitize 递归脱敏。
	Detail map[string]any `json:"detail,omitempty" bson:"detail,omitempty"`

	// DedupKey 幂等去重键；空 = 不参与去重。非空时依赖唯一稀疏索引，
	// 重复 Emit 同键事件会被安全忽略（DuplicateKeyError 吞掉）。
	DedupKey string `json:"dedup_key,omitempty" bson:"dedup_key,omitempty"`
}

// Validate 校验事件的必填骨架。返回 error 的场景都是调用方代码错误
//（字段漏填/枚举非法），应在开发期暴露而非静默写入残缺审计。
func (e *AuditEvent) Validate() error {
	switch {
	case e == nil:
		return errors.New("auditlog: event 为 nil")
	case e.ActorType == "":
		return errors.New("auditlog: actor_type 必填")
	case e.At <= 0:
		return errors.New("auditlog: at 必填（Unix 毫秒）")
	case e.Service == "":
		return errors.New("auditlog: service 必填")
	case e.Domain == "":
		return errors.New("auditlog: domain 必填")
	case e.Action == "":
		return errors.New("auditlog: action 必填")
	case e.ResourceType == "":
		return errors.New("auditlog: resource_type 必填")
	case e.Result == "":
		return errors.New("auditlog: result 必填")
	}
	switch e.ActorType {
	case ActorTypeHuman, ActorTypeSystem, ActorTypeExecutor, ActorTypeScheduler:
	default:
		return fmt.Errorf("auditlog: 非法 actor_type %q", e.ActorType)
	}
	switch e.Result {
	case ResultSuccess, ResultFailed, ResultPartial:
	default:
		return fmt.Errorf("auditlog: 非法 result %q", e.Result)
	}
	return nil
}
