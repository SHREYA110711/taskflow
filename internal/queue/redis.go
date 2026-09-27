package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/redis/go-redis/v9"
)

const (
	QueuePrefix       = "taskflow:queue:"
	KeyScheduled      = "taskflow:scheduled"
	KeyDeadLetter     = "taskflow:queue:dlq"
	KeyProcessingHash = "taskflow:processing"
)

var (
	ErrNilJob       = errors.New("cannot enqueue nil job")
	ErrEmptyPayload = errors.New("job payload cannot be empty")
)

// RedisBroker implements Broker using Redis Lists, Sorted Sets, and Lua scripts.
type RedisBroker struct {
	client *redis.Client
}

// NewRedisBroker creates and connects a new RedisBroker.
func NewRedisBroker(addr, password string, db int) (*RedisBroker, error) {
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
		MinIdleConns: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed at %s: %w", addr, err)
	}

	return &RedisBroker{client: client}, nil
}

// NewRedisBrokerFromClient creates a broker with an existing redis.Client (useful for testing).
func NewRedisBrokerFromClient(client *redis.Client) *RedisBroker {
	return &RedisBroker{client: client}
}

// QueueKey returns the Redis key for a given priority queue.
func QueueKey(priority model.JobPriority) string {
	if priority == "" {
		priority = model.PriorityDefault
	}
	return QueuePrefix + string(priority)
}

// Enqueue puts a job into either the scheduled set (if RunAt is future) or the ready list.
func (b *RedisBroker) Enqueue(ctx context.Context, job *model.Job) error {
	if job == nil {
		return ErrNilJob
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	now := time.Now().UTC()
	// If the job is scheduled for the future, place in delayed ZSET
	if job.RunAt.After(now.Add(100 * time.Millisecond)) {
		return b.ScheduleDelayed(ctx, job)
	}

	// Immediate execution: push to the front of the list (LPush)
	queueName := QueueKey(job.Priority)
	if err := b.client.LPush(ctx, queueName, data).Err(); err != nil {
		return fmt.Errorf("failed to LPush job to %s: %w", queueName, err)
	}

	return nil
}

// Dequeue atomically pops a job from priority queues in order (High -> Default -> Low).
func (b *RedisBroker) Dequeue(ctx context.Context, timeout time.Duration, priorities ...model.JobPriority) (*model.Job, error) {
	if len(priorities) == 0 {
		priorities = []model.JobPriority{
			model.PriorityHigh,
			model.PriorityDefault,
			model.PriorityLow,
		}
	}

	queueKeys := make([]string, len(priorities))
	for i, p := range priorities {
		queueKeys[i] = QueueKey(p)
	}

	// BRPop checks keys in order: if critical has items, it pops from critical first
	res, err := b.client.BRPop(ctx, timeout, queueKeys...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // Timeout, no jobs available
		}
		return nil, err
	}

	if len(res) < 2 {
		return nil, nil
	}

	var job model.Job
	if err := json.Unmarshal([]byte(res[1]), &job); err != nil {
		return nil, fmt.Errorf("failed to deserialize job from %s: %w", res[0], err)
	}

	return &job, nil
}

// ScheduleDelayed adds a job to the Redis Sorted Set (ZSET) with score = Unix timestamp.
func (b *RedisBroker) ScheduleDelayed(ctx context.Context, job *model.Job) error {
	if job == nil {
		return ErrNilJob
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	score := float64(job.RunAt.UnixMilli())
	if err := b.client.ZAdd(ctx, KeyScheduled, redis.Z{
		Score:  score,
		Member: string(data),
	}).Err(); err != nil {
		return fmt.Errorf("failed to ZADD delayed job: %w", err)
	}

	return nil
}

// migrateScheduledLuaScript atomically moves due items from ZSET into their respective Priority LPush queues.
var migrateScheduledLuaScript = redis.NewScript(`
	local scheduledKey = KEYS[1]
	local now = tonumber(ARGV[1])
	local limit = tonumber(ARGV[2])

	-- Fetch items with score <= now
	local items = redis.call('ZRANGEBYSCORE', scheduledKey, 0, now, 'LIMIT', 0, limit)
	local migrated = 0

	for _, item in ipairs(items) do
		local job = cjson.decode(item)
		local priority = job.priority or "default"
		local targetQueue = "taskflow:queue:" .. priority

		-- Atomically remove from ZSET and push to ready queue
		redis.call('ZREM', scheduledKey, item)
		redis.call('LPUSH', targetQueue, item)
		migrated = migrated + 1
	end

	return migrated
`)

// PollScheduledJobs runs the atomic Lua migration script to move due jobs into ready queues.
func (b *RedisBroker) PollScheduledJobs(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	nowMilli := time.Now().UTC().UnixMilli()
	res, err := migrateScheduledLuaScript.Run(ctx, b.client, []string{KeyScheduled}, nowMilli, batchSize).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to run migrate scheduled script: %w", err)
	}

	migrated, ok := res.(int64)
	if !ok {
		return 0, nil
	}

	return migrated, nil
}

// MoveToDLQ pushes a permanently failed job to the Dead Letter Queue.
func (b *RedisBroker) MoveToDLQ(ctx context.Context, job *model.Job, reason string) error {
	if job == nil {
		return ErrNilJob
	}

	job.Status = model.StatusFailed
	job.LastError = reason
	job.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to serialize DLQ job: %w", err)
	}

	if err := b.client.LPush(ctx, KeyDeadLetter, data).Err(); err != nil {
		return fmt.Errorf("failed to push to DLQ: %w", err)
	}

	return nil
}

// GetQueueStats inspects current lengths of ready, scheduled, and dead-letter queues.
func (b *RedisBroker) GetQueueStats(ctx context.Context) (*QueueStats, error) {
	pipe := b.client.Pipeline()

	highCmd := pipe.LLen(ctx, QueueKey(model.PriorityHigh))
	defCmd := pipe.LLen(ctx, QueueKey(model.PriorityDefault))
	lowCmd := pipe.LLen(ctx, QueueKey(model.PriorityLow))
	schedCmd := pipe.ZCard(ctx, KeyScheduled)
	dlqCmd := pipe.LLen(ctx, KeyDeadLetter)

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("failed to fetch queue lengths: %w", err)
	}

	high := highCmd.Val()
	def := defCmd.Val()
	low := lowCmd.Val()
	sched := schedCmd.Val()
	dlq := dlqCmd.Val()

	return &QueueStats{
		High:       high,
		Default:    def,
		Low:        low,
		Scheduled:  sched,
		DeadLetter: dlq,
		TotalReady: high + def + low,
	}, nil
}

// Ping checks the connection to Redis.
func (b *RedisBroker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

// Close disconnects the Redis client.
func (b *RedisBroker) Close() error {
	return b.client.Close()
}
