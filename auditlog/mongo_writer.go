package auditlog

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CollectionName 统一审计事件集合名。单集合多域：跨域检索的核心前提是
// 「同一个集合 + 统一 envelope」，否则无法一次查询覆盖所有审计来源。
const CollectionName = "audit_events"

// MongoWriter 基于 MongoDB 的审计写入实现。
//
// Append-only 约定（与 ChangeOrderAuditEntry 的 Hard Rule 一致）：只提供
// 写入与查询索引维护，不存在任何 update/delete 路径——审计记录不允许被
// 篡改或抹除，这是审计可信度的底线。
type MongoWriter struct {
	coll *mongo.Collection
}

func NewMongoWriter(db *mongo.Database) *MongoWriter {
	return &MongoWriter{coll: db.Collection(CollectionName)}
}

// EnsureIndexes 建立检索与幂等所需的索引。幂等索引必须稀疏唯一：
// DedupKey 为空的事件不参与去重（MongoDB 稀疏索引才允许多个空值共存）。
func (w *MongoWriter) EnsureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			// 幂等去重：唯一 + 稀疏（空 DedupKey 不受唯一约束）。
			Keys:    bson.D{bson.E{Key: "dedup_key", Value: 1}},
			Options: options.Index().SetUnique(true).SetSparse(true).SetName("uk_dedup_key"),
		},
		{
			// 租户 + 时间：绝大多数审计查询按租户时间窗检索。
			Keys:    bson.D{bson.E{Key: "tenant_id", Value: 1}, bson.E{Key: "at", Value: -1}},
			Options: options.Index().SetName("idx_tenant_at"),
		},
		{
			// 操作者 + 时间：「某人做过什么」是审计最高频的问题。
			Keys:    bson.D{bson.E{Key: "actor_id", Value: 1}, bson.E{Key: "at", Value: -1}},
			Options: options.Index().SetName("idx_actor_at"),
		},
		{
			// 服务 + 域 + 时间：按业务域过滤（如只看资产变更）。
			Keys: bson.D{
				bson.E{Key: "service", Value: 1},
				bson.E{Key: "domain", Value: 1},
				bson.E{Key: "at", Value: -1},
			},
			Options: options.Index().SetName("idx_service_domain_at"),
		},
	}
	_, err := w.coll.Indexes().CreateMany(ctx, models)
	if err != nil {
		return fmt.Errorf("auditlog: 创建审计索引失败: %w", err)
	}
	return nil
}

// Emit 校验并写入事件。流程：校验骨架 → 脱敏 → 批量插入 → 幂等冲突吞掉。
func (w *MongoWriter) Emit(ctx context.Context, events ...*AuditEvent) error {
	if len(events) == 0 {
		return nil
	}
	docs := make([]any, 0, len(events))
	for _, e := range events {
		if err := e.Validate(); err != nil {
			return err
		}
		e.Sanitize()
		docs = append(docs, e)
	}
	// ordered=false：同批里某条因 DedupKey 冲突失败不阻断其余条目。
	res, err := w.coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	if err != nil {
		// 幂等语义：整批都因 DedupKey 冲突时 BulkWriteException 可能没有
		// 成功写入项，此时视为「重复提交，安全忽略」而非错误。
		if isDupOnly(err, len(docs), len(res.InsertedIDs)) {
			return nil
		}
		return fmt.Errorf("auditlog: 写入审计事件失败: %w", err)
	}
	return nil
}

// isDupOnly 判断插入失败是否全部由 DedupKey 唯一索引冲突导致。
// 逐条错查 BulkWriteException.WriteErrors 的 code（11000=E11000 duplicate key）。
func isDupOnly(err error, total, inserted int) bool {
	if inserted > 0 {
		return false
	}
	var bwErr mongo.BulkWriteException
	if !errors.As(err, &bwErr) {
		return false
	}
	if len(bwErr.WriteErrors) == 0 {
		return false
	}
	for _, we := range bwErr.WriteErrors {
		if we.Code != 11000 {
			return false
		}
	}
	// 全部失败且全部是重复键：幂等命中。
	return true
}
