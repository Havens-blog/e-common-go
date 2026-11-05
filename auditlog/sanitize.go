package auditlog

import (
	"fmt"
	"strings"
)

// sensitiveKeyFragments 命中即脱敏的键名片段（大小写不敏感）。
// 审计 Detail 里常见的安全载荷：云账号 AK/SK、各类 token、密码与私钥。
// 安全边界：审计日志绝不能落明文凭证。
var sensitiveKeyFragments = []string{
	"password", "passwd", "secret", "token", "private_key",
	"access_key", "apikey", "api_key", "credential", "authorization",
}

// DefaultSanitizeKeys 默认脱敏的键名匹配器。
func DefaultSanitizeKeys(key string) bool {
	lk := strings.ToLower(key)
	for _, frag := range sensitiveKeyFragments {
		if strings.Contains(lk, frag) {
			return true
		}
	}
	return false
}

// Sanitize 原地递归脱敏 Detail 中命中 matcher 的键（值替换为固定占位符）。
//
// 为什么在 SDK 内做而不是各调用方自己做：脱敏规则分散在 4 个审计写入点时
// 必然漂移（某处漏了 token、某处漏了私钥）；收敛到 SDK 才能保证「审计不落
// 明文凭证」这条安全边界只实现一次。
func (e *AuditEvent) Sanitize() {
	if e == nil || len(e.Detail) == 0 {
		return
	}
	sanitizeMap(e.Detail, DefaultSanitizeKeys, 0)
}

const redactedPlaceholder = "[REDACTED]"

// maxSanitizeDepth 递归上限：防御 Detail 里出现环引用或恶意深嵌套把写入拖死。
const maxSanitizeDepth = 8

func sanitizeMap(m map[string]any, match func(string) bool, depth int) {
	if depth > maxSanitizeDepth {
		return
	}
	for k, v := range m {
		if match(k) {
			m[k] = redactedPlaceholder
			continue
		}
		switch tv := v.(type) {
		case map[string]any:
			sanitizeMap(tv, match, depth+1)
		case []any:
			sanitizeSlice(tv, match, depth+1)
		}
	}
}

func sanitizeSlice(s []any, match func(string) bool, depth int) {
	if depth > maxSanitizeDepth {
		return
	}
	for _, v := range s {
		switch tv := v.(type) {
		case map[string]any:
			sanitizeMap(tv, match, depth+1)
		case []any:
			sanitizeSlice(tv, match, depth+1)
		}
	}
}

// SanitizeWithKeys 用自定义键匹配器脱敏（供各服务补充业务私有敏感键，
// 如某域的「签名串」「凭证片段」）。先跑默认规则再追加自定义规则。
func (e *AuditEvent) SanitizeWithKeys(match func(string) bool) {
	if e == nil || len(e.Detail) == 0 || match == nil {
		return
	}
	// 委托：先默认脱敏，再用自定义匹配器过一遍。
	sanitizeMap(e.Detail, func(k string) bool {
		return DefaultSanitizeKeys(k) || match(k)
	}, 0)
}

// String 便于日志排查的紧凑表示（不含 Detail，避免日志二次泄露敏感值）。
func (e *AuditEvent) String() string {
	if e == nil {
		return "<nil audit event>"
	}
	return fmt.Sprintf("%s/%s %s actor=%s(%s) resource=%s:%s result=%s at=%d dedup=%s",
		e.Service, e.Domain, e.Action, e.ActorName, e.ActorID,
		e.ResourceType, e.ResourceID, e.Result, e.At, e.DedupKey)
}
