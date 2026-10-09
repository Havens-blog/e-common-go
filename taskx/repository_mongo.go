package taskx

import (
	"context"
	"time"

	"github.com/Havens-blog/e-common-go/mongox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultCollectionName = "ecam_task"
)

// MongoRepository MongoDB 任务仓储实现
type MongoRepository struct {
	db             *mongox.Mongo
	collectionName string
}

// NewMongoRepository 创建 MongoDB 任务仓储
func NewMongoRepository(db *mongox.Mongo, collectionName string) *MongoRepository {
	if collectionName == "" {
		collectionName = DefaultCollectionName
	}

	return &MongoRepository{
		db:             db,
		collectionName: collectionName,
	}
}

// Create 创建任务
func (r *MongoRepository) Create(ctx context.Context, task Task) error {
	collection := r.db.Collection(r.collectionName)
	_, err := collection.InsertOne(ctx, task)
	return err
}

// GetByID 根据ID获取任务
func (r *MongoRepository) GetByID(ctx context.Context, id string) (Task, error) {
	collection := r.db.Collection(r.collectionName)

	var task Task
	err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&task)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return Task{}, nil
		}
		return Task{}, err
	}

	return task, nil
}

// Update 更新任务
func (r *MongoRepository) Update(ctx context.Context, task Task) error {
	collection := r.db.Collection(r.collectionName)

	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": task.ID},
		bson.M{"$set": task},
	)

	return err
}

// UpdateStatus 更新任务状态
func (r *MongoRepository) UpdateStatus(ctx context.Context, id string, status TaskStatus, message string) error {
	collection := r.db.Collection(r.collectionName)

	update := bson.M{
		"status":  status,
		"message": message,
	}

	// 如果状态是运行中，设置开始时间
	if status == TaskStatusRunning {
		now := time.Now()
		update["started_at"] = now
	}

	// 如果状态是完成或失败，设置完成时间和计算时长
	if status == TaskStatusCompleted || status == TaskStatusFailed || status == TaskStatusCancelled {
		now := time.Now()
		update["completed_at"] = now

		// 获取任务以计算时长
		var task Task
		err := collection.FindOne(ctx, bson.M{"_id": id}).Decode(&task)
		if err == nil && task.StartedAt != nil {
			duration := now.Sub(*task.StartedAt).Seconds()
			update["duration"] = int64(duration)
		}
	}

	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.M{"$set": update},
	)

	return err
}

// UpdateProgress 更新任务进度
func (r *MongoRepository) UpdateProgress(ctx context.Context, id string, progress int, message string) error {
	collection := r.db.Collection(r.collectionName)

	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{
			"progress": progress,
			"message":  message,
		}},
	)

	return err
}

// List 获取任务列表
func (r *MongoRepository) List(ctx context.Context, filter TaskFilter) ([]Task, error) {
	collection := r.db.Collection(r.collectionName)

	// 构建查询条件
	query := bson.M{}

	if filter.Type != "" {
		query["type"] = filter.Type
	}

	if filter.Status != "" {
		query["status"] = filter.Status
	}

	if filter.CreatedBy != "" {
		query["created_by"] = filter.CreatedBy
	}

	if !filter.StartDate.IsZero() || !filter.EndDate.IsZero() {
		dateQuery := bson.M{}
		if !filter.StartDate.IsZero() {
			dateQuery["$gte"] = filter.StartDate
		}
		if !filter.EndDate.IsZero() {
			dateQuery["$lte"] = filter.EndDate
		}
		query["created_at"] = dateQuery
	}

	// 设置分页和排序
	opts := options.Find()
	opts.SetSort(bson.D{{Key: "created_at", Value: -1}}) // 按创建时间倒序

	if filter.Limit > 0 {
		opts.SetLimit(filter.Limit)
	}

	if filter.Offset > 0 {
		opts.SetSkip(filter.Offset)
	}

	cursor, err := collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var tasks []Task
	if err = cursor.All(ctx, &tasks); err != nil {
		return nil, err
	}

	return tasks, nil
}

// Count 统计任务数量
func (r *MongoRepository) Count(ctx context.Context, filter TaskFilter) (int64, error) {
	collection := r.db.Collection(r.collectionName)

	// 构建查询条件
	query := bson.M{}

	if filter.Type != "" {
		query["type"] = filter.Type
	}

	if filter.Status != "" {
		query["status"] = filter.Status
	}

	if filter.CreatedBy != "" {
		query["created_by"] = filter.CreatedBy
	}

	if !filter.StartDate.IsZero() || !filter.EndDate.IsZero() {
		dateQuery := bson.M{}
		if !filter.StartDate.IsZero() {
			dateQuery["$gte"] = filter.StartDate
		}
		if !filter.EndDate.IsZero() {
			dateQuery["$lte"] = filter.EndDate
		}
		query["created_at"] = dateQuery
	}

	return collection.CountDocuments(ctx, query)
}

// Delete 删除任务
func (r *MongoRepository) Delete(ctx context.Context, id string) error {
	collection := r.db.Collection(r.collectionName)
	_, err := collection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// InitIndexes 初始化索引
func (r *MongoRepository) InitIndexes(ctx context.Context) error {
	collection := r.db.Collection(r.collectionName)

	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "type", Value: 1},
				{Key: "status", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "created_by", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "created_at", Value: -1},
			},
		},
	}

	_, err := collection.Indexes().CreateMany(ctx, indexes)
	return err
}
