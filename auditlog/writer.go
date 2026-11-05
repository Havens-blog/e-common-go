package auditlog

import "context"

// Writer 统一审计写入端口。各服务的审计触发点依赖此接口而非具体存储，
// 便于单测替换与后续更换存储。
type Writer interface {
	// Emit 写入一条或多条审计事件。
	// 返回 error 表示「有事件未落审计」；调用方应记录该错误但不应让业务
	// 请求失败（审计是旁路）。DedupKey 冲突不算错误（幂等语义）。
	Emit(ctx context.Context, events ...*AuditEvent) error
}

// NonBlocking 把任意 Writer 包装成「审计失败不阻塞业务」的安全 writer：
// Emit 出错时只回调 onError（通常接错误日志），不向业务传播。
//
// 为什么提供这个包装：四个既有审计触发点的共同约定是「审计失败记日志、
// 不阻塞主流程」（ChangeOrderAuditEntry 已明确 Hard Rule）。收敛到 SDK
// 后由包装器统一实现，避免每个触发点各写一遍 try-catch。
type NonBlocking struct {
	inner   Writer
	onError func(err error)
}

func NewNonBlocking(inner Writer, onError func(err error)) *NonBlocking {
	return &NonBlocking{inner: inner, onError: onError}
}

func (n *NonBlocking) Emit(ctx context.Context, events ...*AuditEvent) error {
	err := n.inner.Emit(ctx, events...)
	if err != nil && n.onError != nil {
		n.onError(err)
	}
	return nil
}
