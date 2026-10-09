package taskx

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// 回归:粗粒度 Update($set 整个结构体)会把 nil 指针写成 bson null,
// 抹掉 UpdateStatus 刚写入的 started_at/completed_at/duration。
// 生命周期字段必须带 omitempty——nil 时不得出现在 bson 文档里。
func TestTaskBsonOmitsNilLifecycleFields(t *testing.T) {
	b, err := bson.Marshal(Task{})
	if err != nil {
		t.Fatalf("marshal 失败: %v", err)
	}
	var m map[string]any
	if err := bson.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal 失败: %v", err)
	}
	for _, field := range []string{"started_at", "completed_at", "duration"} {
		if _, ok := m[field]; ok {
			t.Errorf("零值任务的 %s 不应写入 bson(会被整结构体 Update 以 null/0 覆盖)", field)
		}
	}
}

// 有值时字段必须正常落库
func TestTaskBsonKeepsLifecycleFields(t *testing.T) {
	now := time.Now()
	b, err := bson.Marshal(Task{StartedAt: &now, CompletedAt: &now, Duration: 42})
	if err != nil {
		t.Fatalf("marshal 失败: %v", err)
	}
	var m map[string]any
	if err := bson.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal 失败: %v", err)
	}
	for _, field := range []string{"started_at", "completed_at", "duration"} {
		if _, ok := m[field]; !ok {
			t.Errorf("有值字段 %s 应写入 bson", field)
		}
	}
}
