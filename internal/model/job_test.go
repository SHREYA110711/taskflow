package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestJobJSONSerialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	job := Job{
		ID:                "job_test_123",
		Type:              "send_welcome_email",
		Payload:           json.RawMessage(`{"user_id":42,"email":"user@example.com"}`),
		Status:            StatusPending,
		Priority:          PriorityHigh,
		Attempts:          1,
		MaxRetries:        5,
		RetryDelaySeconds: 15,
		TimeoutSeconds:    60,
		RunAt:             now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("failed to marshal job: %v", err)
	}

	var unmarshaled Job
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal job: %v", err)
	}

	if unmarshaled.ID != job.ID {
		t.Errorf("expected ID %s, got %s", job.ID, unmarshaled.ID)
	}
	if unmarshaled.Priority != PriorityHigh {
		t.Errorf("expected Priority %s, got %s", PriorityHigh, unmarshaled.Priority)
	}
	if unmarshaled.Status != StatusPending {
		t.Errorf("expected Status %s, got %s", StatusPending, unmarshaled.Status)
	}
}
