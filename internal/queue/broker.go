package queue

import (
	"context"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
)

// QueueStats contains current lengths for all queues in the broker.
type QueueStats struct {
	High       int64 `json:"high"`
	Default    int64 `json:"default"`
	Low        int64 `json:"low"`
	Scheduled  int64 `json:"scheduled"`
	DeadLetter int64 `json:"dead_letter"`
	Processing int64 `json:"processing"`
	TotalReady int64 `json:"total_ready"`
}

// Broker defines the contract for queue operations.
type Broker interface {
	// Enqueue pushes a job into the appropriate ready queue or scheduled ZSET based on its RunAt time.
	Enqueue(ctx context.Context, job *model.Job) error

	// Dequeue pulls the next available job prioritizing queues in order (e.g. critical -> default -> low).
	// Blocks up to timeout duration if all queues are empty.
	Dequeue(ctx context.Context, timeout time.Duration, priorities ...model.JobPriority) (*model.Job, error)

	// ScheduleDelayed inserts a job into the delayed sorted set.
	ScheduleDelayed(ctx context.Context, job *model.Job) error

	// PollScheduledJobs atomically migrates due jobs from the scheduled ZSET to their ready queues.
	PollScheduledJobs(ctx context.Context, batchSize int) (int64, error)

	// MoveToDLQ places a permanently failed job into the Dead Letter Queue.
	MoveToDLQ(ctx context.Context, job *model.Job, reason string) error

	// GetQueueStats returns current item counts across all queues.
	GetQueueStats(ctx context.Context) (*QueueStats, error)

	// Ping checks broker connectivity.
	Ping(ctx context.Context) error

	// Close cleans up broker client connections.
	Close() error
}
