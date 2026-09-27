package worker

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/SHREYA110711/taskflow/internal/model"
)

func TestHandlerRegistry(t *testing.T) {
	registry := NewRegistry()

	// Verify default handler exists
	h, err := registry.Get("send_email")
	if err != nil || h == nil {
		t.Fatalf("expected send_email handler to be registered by default")
	}

	// Register custom handler
	registry.Register("custom_task", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"status":"custom_done"}`), nil
	})

	customH, err := registry.Get("custom_task")
	if err != nil || customH == nil {
		t.Fatalf("failed to retrieve registered custom handler")
	}

	// Execute custom handler
	res, err := customH(context.Background(), &model.Job{})
	if err != nil {
		t.Fatalf("unexpected handler execution error: %v", err)
	}

	if string(res) != `{"status":"custom_done"}` {
		t.Errorf("expected result `{\"status\":\"custom_done\"}`, got %s", string(res))
	}

	// Verify unregistered handler returns ErrHandlerNotFound
	_, err = registry.Get("non_existent_task")
	if err == nil {
		t.Errorf("expected error for unregistered task, got nil")
	}
}
