package model

import (
	"encoding/json"
	"time"
)

// JobStatus represents the state of a job in the lifecycle.
type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusRetrying   JobStatus = "retrying"
	StatusCancelled  JobStatus = "cancelled"
)

// JobPriority defines queue priority levels.
type JobPriority string

const (
	PriorityHigh    JobPriority = "high"
	PriorityDefault JobPriority = "default"
	PriorityLow     JobPriority = "low"
)

// Job represents a background task in the system.
type Job struct {
	ID                string          `json:"id"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Status            JobStatus       `json:"status"`
	Priority          JobPriority     `json:"priority"`
	Attempts          int             `json:"attempts"`
	MaxRetries        int             `json:"max_retries"`
	RetryDelaySeconds int             `json:"retry_delay_seconds"`
	TimeoutSeconds    int             `json:"timeout_seconds"`
	RunAt             time.Time       `json:"run_at"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
	FailedAt          *time.Time      `json:"failed_at,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	LastError         string          `json:"last_error,omitempty"`
	LockedBy          string          `json:"locked_by,omitempty"`
	LockedUntil       *time.Time      `json:"locked_until,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

// JobLog captures point-in-time execution logs and failure stack traces for observability.
type JobLog struct {
	ID         int64     `json:"id"`
	JobID      string    `json:"job_id"`
	Attempt    int       `json:"attempt"`
	Status     JobStatus `json:"status"`
	WorkerID   string    `json:"worker_id"`
	Message    string    `json:"message"`
	Error      string    `json:"error,omitempty"`
	DurationMs int64     `json:"duration_ms"`
	CreatedAt  time.Time `json:"created_at"`
}

// JobFilter defines criteria for searching, filtering, and paginating jobs.
type JobFilter struct {
	Status   *JobStatus   `json:"status,omitempty"`
	Type     *string      `json:"type,omitempty"`
	Priority *JobPriority `json:"priority,omitempty"`
	Search   *string      `json:"search,omitempty"`
	Limit    int          `json:"limit"`
	Offset   int          `json:"offset"`
}

// JobStats summarizes current queue status counts across the system.
type JobStats struct {
	Total      int64 `json:"total"`
	Pending    int64 `json:"pending"`
	Queued     int64 `json:"queued"`
	Processing int64 `json:"processing"`
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
	Retrying   int64 `json:"retrying"`
	Cancelled  int64 `json:"cancelled"`
}

// CreateJobRequest defines the API payload for creating/enqueueing a job.
type CreateJobRequest struct {
	ID                string          `json:"id,omitempty"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Priority          JobPriority     `json:"priority,omitempty"`
	MaxRetries        *int            `json:"max_retries,omitempty"`
	RetryDelaySeconds *int            `json:"retry_delay_seconds,omitempty"`
	TimeoutSeconds    *int            `json:"timeout_seconds,omitempty"`
	DelaySeconds      *int            `json:"delay_seconds,omitempty"`
	RunAt             *time.Time      `json:"run_at,omitempty"`
}
