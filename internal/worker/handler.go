package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
)

var (
	ErrHandlerNotFound = errors.New("no handler registered for job type")
)

// Handler defines the function signature for executing a background task.
type Handler func(ctx context.Context, job *model.Job) (json.RawMessage, error)

// Registry manages registered handlers by job type.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry creates a new handler registry with default handlers registered.
func NewRegistry() *Registry {
	r := &Registry{
		handlers: make(map[string]Handler),
	}
	r.registerDefaultHandlers()
	return r
}

// Register binds a job type string to a specific execution handler.
func (r *Registry) Register(jobType string, handler Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = handler
}

// Get returns the handler for a given job type.
func (r *Registry) Get(jobType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	h, exists := r.handlers[jobType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotFound, jobType)
	}
	return h, nil
}

// registerDefaultHandlers registers sample task handlers.
func (r *Registry) registerDefaultHandlers() {
	// 1. Email Sender Handler
	r.Register("send_email", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
		var payload struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
			Body    string `json:"body"`
		}

		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return nil, fmt.Errorf("invalid send_email payload: %w", err)
		}

		if payload.To == "" {
			return nil, errors.New("recipient email 'to' is required")
		}

		log.Printf("[EmailHandler] Sending email to: %s with subject: %s", payload.To, payload.Subject)
		time.Sleep(100 * time.Millisecond) // Simulate network I/O

		return json.Marshal(map[string]interface{}{
			"status":   "delivered",
			"to":       payload.To,
			"sent_at":  time.Now().UTC().Format(time.RFC3339),
		})
	})

	// 2. Webhook Notification Handler
	r.Register("webhook_trigger", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
		var payload struct {
			URL   string                 `json:"url"`
			Event string                 `json:"event"`
			Data  map[string]interface{} `json:"data"`
		}

		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return nil, fmt.Errorf("invalid webhook payload: %w", err)
		}

		if payload.URL == "" {
			return nil, errors.New("webhook 'url' is required")
		}

		log.Printf("[WebhookHandler] Firing webhook to: %s for event: %s", payload.URL, payload.Event)
		time.Sleep(150 * time.Millisecond) // Simulate HTTP POST

		return json.Marshal(map[string]interface{}{
			"status":      "ok",
			"status_code": 200,
			"url":         payload.URL,
		})
	})

	// 3. Image Processing Handler
	r.Register("process_image", func(ctx context.Context, job *model.Job) (json.RawMessage, error) {
		var payload struct {
			ImageURL string `json:"image_url"`
			Width    int    `json:"width"`
			Height   int    `json:"height"`
		}

		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return nil, fmt.Errorf("invalid process_image payload: %w", err)
		}

		log.Printf("[ImageHandler] Resizing image %s to %dx%d", payload.ImageURL, payload.Width, payload.Height)
		time.Sleep(200 * time.Millisecond) // Simulate CPU processing

		return json.Marshal(map[string]interface{}{
			"status":        "processed",
			"thumbnail_url": payload.ImageURL + "-thumb.jpg",
		})
	})
}
