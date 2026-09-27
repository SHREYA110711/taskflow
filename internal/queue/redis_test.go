package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
)

func TestQueueKey(t *testing.T) {
	tests := []struct {
		priority model.JobPriority
		expected string
	}{
		{model.PriorityHigh, "taskflow:queue:high"},
		{model.PriorityDefault, "taskflow:queue:default"},
		{model.PriorityLow, "taskflow:queue:low"},
		{"", "taskflow:queue:default"},
	}

	for _, tt := range tests {
		result := QueueKey(tt.priority)
		if result != tt.expected {
			t.Errorf("expected QueueKey(%q) = %q, got %q", tt.priority, tt.expected, result)
		}
	}
}

func TestNilJobEnqueue(t *testing.T) {
	broker := &RedisBroker{}
	err := broker.Enqueue(context.Background(), nil)
	if err != ErrNilJob {
		t.Errorf("expected ErrNilJob, got %v", err)
	}
}

func TestJobSerializationForQueue(t *testing.T) {
	job := &model.Job{
		ID:                "job_queue_test_1",
		Type:              "process_video",
		Payload:           json.RawMessage(`{"resolution":"1080p"}`),
		Status:            model.StatusPending,
		Priority:          model.PriorityHigh,
		Attempts:          0,
		MaxRetries:        3,
		RetryDelaySeconds: 10,
		TimeoutSeconds:    120,
		RunAt:             time.Now().UTC(),
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var decoded model.Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if decoded.ID != job.ID {
		t.Errorf("expected ID %s, got %s", job.ID, decoded.ID)
	}
	if decoded.Priority != model.PriorityHigh {
		t.Errorf("expected Priority %s, got %s", model.PriorityHigh, decoded.Priority)
	}
}
