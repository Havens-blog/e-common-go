package auditlog

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ==================== Validate 表驱动测试 ====================

func TestAuditEvent_Validate(t *testing.T) {
	base := func(mut func(*AuditEvent)) *AuditEvent {
		e := &AuditEvent{
			ActorType:    ActorTypeHuman,
			ActorID:      "42",
			ActorName:    "管理员",
			At:           time.Now().UnixMilli(),
			TenantID:     3,
			Service:      ServiceCAM,
			Domain:       DomainAudit,
			Action:       "create",
			ResourceType: "api",
			Result:       ResultSuccess,
		}
		if mut != nil {
			mut(e)
		}
		return e
	}

	testCases := []struct {
		name    string
		event   *AuditEvent
		wantErr bool
	}{
		{name: "合法事件通过", event: base(nil), wantErr: false},
		{name: "nil 事件报错", event: nil, wantErr: true},
		{name: "缺 actor_type 报错", event: base(func(e *AuditEvent) { e.ActorType = "" }), wantErr: true},
		{name: "非法 actor_type 报错", event: base(func(e *AuditEvent) { e.ActorType = "robot" }), wantErr: true},
		{name: "缺 at 报错", event: base(func(e *AuditEvent) { e.At = 0 }), wantErr: true},
		{name: "缺 service 报错", event: base(func(e *AuditEvent) { e.Service = "" }), wantErr: true},
		{name: "缺 domain 报错", event: base(func(e *AuditEvent) { e.Domain = "" }), wantErr: true},
		{name: "缺 action 报错", event: base(func(e *AuditEvent) { e.Action = "" }), wantErr: true},
		{name: "缺 resource_type 报错", event: base(func(e *AuditEvent) { e.ResourceType = "" }), wantErr: true},
		{name: "缺 result 报错", event: base(func(e *AuditEvent) { e.Result = "" }), wantErr: true},
		{name: "非法 result 报错", event: base(func(e *AuditEvent) { e.Result = "ok" }), wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.event.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// ==================== Sanitize 表驱动测试 ====================

func TestAuditEvent_Sanitize(t *testing.T) {
	testCases := []struct {
		name       string
		detail     map[string]any
		wantKey    string
		wantVals   map[string]any // 期望脱敏后的键值（只校验列出的键）
		skipRedact bool           // true = 期望该键保持原值
	}{
		{
			name:     "顶层 secret 键被脱敏",
			detail:   map[string]any{"access_key": "AKIA123", "plain": "ok"},
			wantVals: map[string]any{"access_key": redactedPlaceholder, "plain": "ok"},
		},
		{
			name:     "嵌套 map 里的 token 被脱敏",
			detail:   map[string]any{"request": map[string]any{"token": "t-abc", "user": "u1"}},
			wantVals: map[string]any{"request": map[string]any{"token": redactedPlaceholder, "user": "u1"}},
		},
		{
			name:     "数组内嵌套对象的 private_key 被脱敏",
			detail:   map[string]any{"items": []any{map[string]any{"private_key": "pk", "name": "n"}}},
			wantVals: map[string]any{"items": []any{map[string]any{"private_key": redactedPlaceholder, "name": "n"}}},
		},
		{
			name:     "大小写不敏感命中（Password）",
			detail:   map[string]any{"Password": "p1"},
			wantVals: map[string]any{"Password": redactedPlaceholder},
		},
		{
			name:     "普通键不误伤",
			detail:   map[string]any{"action": "create", "asset_name": "vm-1"},
			wantVals: map[string]any{"action": "create", "asset_name": "vm-1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			e := &AuditEvent{Detail: tc.detail}
			e.Sanitize()
			for k, want := range tc.wantVals {
				got := e.Detail[k]
				if fmtAny(got) != fmtAny(want) {
					t.Fatalf("Detail[%s] = %v, want %v", k, got, want)
				}
			}
		})
	}
}

// fmtAny 统一转字符串比较（避免 map/slice 直接 == 不可比较的问题）。
func fmtAny(v any) string {
	b, _ := bson.Marshal(v)
	return string(b)
}

// ==================== isDupOnly 幂等判定 表驱动测试 ====================

func TestIsDupOnly(t *testing.T) {
	mkDupErr := func(codes ...int) error {
		bwe := mongo.BulkWriteException{}
		for _, c := range codes {
			bwe.WriteErrors = append(bwe.WriteErrors, mongo.BulkWriteError{WriteError: mongo.WriteError{Index: 0, Code: c}})
		}
		return bwe
	}

	testCases := []struct {
		name     string
		err      error
		total    int
		inserted int
		want     bool
	}{
		{name: "全为重复键且无成功 → 幂等命中", err: mkDupErr(11000, 11000), total: 2, inserted: 0, want: true},
		{name: "混入非重复键错误 → 不是幂等", err: mkDupErr(11000, 2), total: 2, inserted: 0, want: false},
		{name: "有成功写入 → 不是幂等", err: mkDupErr(11000), total: 2, inserted: 1, want: false},
		{name: "非 BulkWrite 异常 → 不是幂等", err: context.DeadlineExceeded, total: 1, inserted: 0, want: false},
		{name: "BulkWrite 但无写错误 → 不是幂等", err: mkDupErr(), total: 1, inserted: 0, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDupOnly(tc.err, tc.total, tc.inserted); got != tc.want {
				t.Fatalf("isDupOnly() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ==================== Mongo 集成测试（环境变量门控） ====================

// mongoTestDSN 本地集成测试的 DSN；未设置则跳过（单元测试不依赖外部资源）。
func mongoTestDSN() string {
	return os.Getenv("AUDITLOG_TEST_MONGODB_DSN")
}

func TestMongoWriter_Emit_Integration(t *testing.T) {
	dsn := mongoTestDSN()
	if dsn == "" {
		t.Skip("未设置 AUDITLOG_TEST_MONGODB_DSN，跳过 Mongo 集成测试")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatalf("连接 mongo 失败: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	db := client.Database("eiam_auditlog_test")
	w := NewMongoWriter(db)
	if err := w.EnsureIndexes(ctx); err != nil {
		t.Fatalf("建索引失败: %v", err)
	}

	t.Run("写入合法事件并可检索", func(t *testing.T) {
		coll := db.Collection(CollectionName)
		_ = coll.Drop(ctx)
		if err := w.EnsureIndexes(ctx); err != nil {
			t.Fatalf("重建索引失败: %v", err)
		}

		e := &AuditEvent{
			ActorType:    ActorTypeHuman,
			ActorID:      "1",
			ActorName:    "系统管理员",
			At:           time.Now().UnixMilli(),
			TenantID:     3,
			Service:      ServiceIAM,
			Domain:       DomainOperation,
			Action:       "create",
			ResourceType: "user",
			ResourceID:   "u-1",
			Result:       ResultSuccess,
			Detail:       map[string]any{"access_key": "AKIA-should-be-redacted", "module": "user"},
		}
		if err := w.Emit(ctx, e); err != nil {
			t.Fatalf("Emit 失败: %v", err)
		}

		var got AuditEvent
		if err := coll.FindOne(ctx, bson.M{"actor_id": "1"}).Decode(&got); err != nil {
			t.Fatalf("回读失败: %v", err)
		}
		if got.ActorName != "系统管理员" || got.Action != "create" {
			t.Fatalf("回读字段不符: %+v", got)
		}
		// 安全边界断言：审计里绝不能有明文凭证。
		if got.Detail["access_key"] != redactedPlaceholder {
			t.Fatalf("access_key 未脱敏: %v", got.Detail["access_key"])
		}
	})

	t.Run("DedupKey 幂等：重复写入不报错也不重复落库", func(t *testing.T) {
		coll := db.Collection(CollectionName)
		_ = coll.Drop(ctx)
		if err := w.EnsureIndexes(ctx); err != nil {
			t.Fatalf("重建索引失败: %v", err)
		}

		e := &AuditEvent{
			ActorType: ActorTypeScheduler, ActorID: "sys", ActorName: "调度器",
			At: time.Now().UnixMilli(), TenantID: 3,
			Service: ServiceCAM, Domain: DomainAudit, Action: "sync",
			ResourceType: "asset", ResourceID: "a-1", Result: ResultSuccess,
			DedupKey: "dedup-test-key-1",
		}
		if err := w.Emit(ctx, e); err != nil {
			t.Fatalf("首次 Emit 失败: %v", err)
		}
		// 重复 Emit：应安全忽略（幂等），不返回 error。
		if err := w.Emit(ctx, e); err != nil {
			t.Fatalf("重复 Emit 应幂等而非报错: %v", err)
		}
		count, err := coll.CountDocuments(ctx, bson.M{"dedup_key": "dedup-test-key-1"})
		if err != nil {
			t.Fatalf("计数失败: %v", err)
		}
		if count != 1 {
			t.Fatalf("期望去重后仅 1 条，实际 %d", count)
		}
	})

	t.Run("非法事件被拒绝且不落库", func(t *testing.T) {
		coll := db.Collection(CollectionName)
		bad := &AuditEvent{ActorType: ActorTypeHuman} // 缺一大半必填
		if err := w.Emit(ctx, bad); err == nil {
			t.Fatal("非法事件应返回 error")
		}
		count, err := coll.CountDocuments(ctx, bson.M{"actor_id": ""})
		if err != nil {
			t.Fatalf("计数失败: %v", err)
		}
		if count != 0 {
			t.Fatalf("非法事件不应落库，实际 %d 条", count)
		}
	})
}
