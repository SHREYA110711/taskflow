package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

// MockJobRepository implements JobRepositoryInterface in-memory for testing.
type MockJobRepository struct {
	jobs map[string]model.Job
}

func NewMockJobRepository() *MockJobRepository {
	return &MockJobRepository{
		jobs: make(map[string]model.Job),
	}
}

func (m *MockJobRepository) Create(ctx context.Context, job model.Job) error {
	m.jobs[job.ID] = job
	return nil
}

func (m *MockJobRepository) GetByID(ctx context.Context, id string) (*model.Job, error) {
	j, exists := m.jobs[id]
	if !exists {
		return nil, repository.ErrJobNotFound
	}
	return &j, nil
}

func (m *MockJobRepository) ResetForRetry(ctx context.Context, id string) error {
	j, exists := m.jobs[id]
	if !exists {
		return repository.ErrJobNotFound
	}
	j.Status = model.StatusPending
	m.jobs[id] = j
	return nil
}

func (m *MockJobRepository) MarkCancelled(ctx context.Context, id string) error {
	j, exists := m.jobs[id]
	if !exists {
		return repository.ErrJobNotFound
	}
	j.Status = model.StatusCancelled
	m.jobs[id] = j
	return nil
}

func (m *MockJobRepository) MarkProcessing(ctx context.Context, id string, workerID string, timeoutSeconds int) error {
	j, exists := m.jobs[id]
	if !exists {
		return repository.ErrJobNotFound
	}
	j.Status = model.StatusProcessing
	j.LockedBy = workerID
	m.jobs[id] = j
	return nil
}

func (m *MockJobRepository) MarkCompleted(ctx context.Context, id string, result json.RawMessage) error {
	j, exists := m.jobs[id]
	if !exists {
		return repository.ErrJobNotFound
	}
	j.Status = model.StatusCompleted
	j.Result = result
	m.jobs[id] = j
	return nil
}

func (m *MockJobRepository) MarkFailed(ctx context.Context, id string, errMsg string, willRetry bool, nextRunAt time.Time) error {
	j, exists := m.jobs[id]
	if !exists {
		return repository.ErrJobNotFound
	}
	if willRetry {
		j.Status = model.StatusRetrying
	} else {
		j.Status = model.StatusFailed
	}
	j.LastError = errMsg
	m.jobs[id] = j
	return nil
}

func (m *MockJobRepository) CreateLog(ctx context.Context, log model.JobLog) error {
	return nil
}

func (m *MockJobRepository) List(ctx context.Context, filter model.JobFilter) ([]model.Job, int64, error) {
	result := make([]model.Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		result = append(result, j)
	}
	return result, int64(len(result)), nil
}

func (m *MockJobRepository) GetStats(ctx context.Context) (*model.JobStats, error) {
	return &model.JobStats{
		Total:   int64(len(m.jobs)),
		Pending: int64(len(m.jobs)),
	}, nil
}

func (m *MockJobRepository) GetLogsByJobID(ctx context.Context, jobID string) ([]model.JobLog, error) {
	return []model.JobLog{
		{
			ID:         1,
			JobID:      jobID,
			Attempt:    1,
			Status:     model.StatusCompleted,
			WorkerID:   "mock-worker",
			Message:    "Success",
			DurationMs: 50,
			CreatedAt:  time.Now().UTC(),
		},
	}, nil
}

func (m *MockJobRepository) Delete(ctx context.Context, id string) error {
	delete(m.jobs, id)
	return nil
}

func TestCreateJobHandler(t *testing.T) {
	mockRepo := NewMockJobRepository()
	h := NewJobHandler(mockRepo, nil)

	body := `{"type":"send_email","payload":{"to":"user@test.com"},"priority":"high"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d. Body: %s", w.Code, w.Body.String())
	}

	var created model.Job
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("failed to decode response JSON: %v", err)
	}

	if created.Type != "send_email" {
		t.Errorf("expected type send_email, got %s", created.Type)
	}
	if created.Priority != model.PriorityHigh {
		t.Errorf("expected priority high, got %s", created.Priority)
	}
}

func TestGetJobHandler(t *testing.T) {
	mockRepo := NewMockJobRepository()
	mockRepo.Create(context.Background(), model.Job{
		ID:        "job_test_fetch",
		Type:      "export_report",
		Status:    model.StatusPending,
		CreatedAt: time.Now().UTC(),
	})

	h := NewJobHandler(mockRepo, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job_test_fetch", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", w.Code)
	}

	var resp struct {
		Job  model.Job        `json:"job"`
		Logs []model.JobLog   `json:"logs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Job.ID != "job_test_fetch" {
		t.Errorf("expected job ID job_test_fetch, got %s", resp.Job.ID)
	}
	if len(resp.Logs) != 1 {
		t.Errorf("expected 1 log entry, got %d", len(resp.Logs))
	}
}

func TestStatsHandler(t *testing.T) {
	mockRepo := NewMockJobRepository()
	statsH := NewStatsHandler(mockRepo, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	w := httptest.NewRecorder()

	statsH.GetStats(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if _, ok := resp["database"]; !ok {
		t.Errorf("expected database key in stats response")
	}
	if _, ok := resp["queues"]; !ok {
		t.Errorf("expected queues key in stats response")
	}
}
