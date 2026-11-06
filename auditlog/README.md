# auditlog — 统一审计日志 SDK

跨服务统一审计的写入端 SDK：统一事件模型（envelope）+ 存储无关的写入端口 +
Mongo 实现 + 脱敏/校验。当前消费方：e-cam-service（操作审计/字段变更/变更单
双写 + 统一查询 API）、e-iam（OperationLog 双写 + 读侧端点）。

## 事件模型（envelope）

`AuditEvent` 五组字段：

| 组 | 字段 |
|---|---|
| Who | `ActorType`（human/system/executor/scheduler）、`ActorID`（统一 string）、`ActorName` |
| When | `At`（Unix 毫秒） |
| Where | `TenantID`、`ClientIP`、`UserAgent` |
| What | `Service`（e-cam-service/e-iam）、`Domain`（audit/operation/auth/change）、`Action`、`ResourceType/ID/URN` |
| Result | `Result`（success/failed/partial）、`StatusCode`、`DurationMs` |

外加 `Detail map[string]any`（域扩展载荷，写入前被递归脱敏）与 `DedupKey`
（幂等去重键，空 = 不参与去重）。

`Validate()` 必填：actor_type / at / service / domain / action / resource_type /
result；`TenantID` 可为 0（跨租户场景由调用方自行保证边界）。

## 快速上手

```go
ev := auditlog.AuditEvent{
    ActorType: auditlog.ActorTypeHuman,
    ActorID:   strconv.FormatInt(op.OperatorID, 10),
    At:        op.Ctime,
    TenantID:  op.TenantID,
    Service:   auditlog.ServiceCAM,
    Domain:    auditlog.DomainChange,
    Action:    "update",
    ResourceType: op.Module,
    Result:    auditlog.ResultSuccess,
    Detail:    map[string]any{"field_name": op.FieldName},
}
if err := ev.Validate(); err != nil { /* 跳过或上报 */ }
ev.Sanitize() // 递归脱敏 password/secret/token/private_key/access_key… → [REDACTED]
writer.Emit(ctx, &ev)
```

## Writer 端口

```go
type Writer interface {
    Emit(ctx context.Context, events ...*AuditEvent) error
}
```

- `NewMongoWriter(db *mongo.Database)`：生产实现，`EnsureIndexes` 建四索引
  （`uk_dedup_key` 稀疏唯一 + tenant_at / actor_at / service_domain_at 检索索引）；
  批量写 `ordered=false`，DedupKey 冲突不算失败（幂等语义）。
- `NewNonBlocking(inner, onError)`：包装器，Emit 出错仅回调 onError（接错误日志），
  **恒返回 nil** —— 供「审计失败不阻塞业务」的触发点使用。无队列、无静默丢弃。

## 已知语义与坑（实测沉淀）

1. **DedupKey 的存储方言**：Mongo 侧 `bson:"dedup_key,omitempty"` + 稀疏唯一索引
   —— 空键字段缺失，多条共存安全；**MySQL/SQLite 侧必须指针化（空串→NULL）**，
   否则唯一索引把空串当值，多条无键事件互相冲突且容易被静默跳过
   （e-iam#6 曾因此数据丢失，回归测试见其仓库）。
2. **DedupKey 冲突不算错误**：幂等命中 = 事件已存在，调用方不应重试。
3. **Detail 写入前必须 Sanitize**：自由文本可能携带敏感配置片段。
4. **ActorID 统一 string**：兼容 int64 账号与外部 ID（`strconv.FormatInt`）；
   来源不可知时如实标 `unknown`，不要伪造。
