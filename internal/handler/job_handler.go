package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/SHREYA110711/taskflow/internal/queue"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

type JobHandler struct {
	jobRepo repository.JobRepositoryInterface
	broker  queue.Broker
}

func NewJobHandler(jobRepo repository.JobRepositoryInterface, broker queue.Broker) *JobHandler {
	return &JobHandler{
		jobRepo: jobRepo,
		broker:  broker,
	}
}

// ServeHTTP handles routing for /api/v1/jobs and /api/v1/jobs/{id}...
func (h *JobHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/jobs")
	path = strings.Trim(path, "/")

	if path == "" {
		switch r.Method {
		case http.MethodPost:
			h.CreateJob(w, r)
		case http.MethodGet:
			h.ListJobs(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	parts := strings.Split(path, "/")
	jobID := parts[0]

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			h.GetJob(w, r, jobID)
		case http.MethodDelete:
			h.CancelJob(w, r, jobID)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	if len(parts) == 2 && parts[1] == "retry" && r.Method == http.MethodPost {
		h.RetryJob(w, r, jobID)
		return
	}

	http.NotFound(w, r)
}

// CreateJob enqueues a new background task.
func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req model.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		renderError(w, http.StatusBadRequest, "Invalid JSON request body")
		return
	}

	if strings.TrimSpace(req.Type) == "" {
		renderError(w, http.StatusBadRequest, "Field 'type' is required")
		return
	}

	now := time.Now().UTC()
	jobID := req.ID
	if jobID == "" {
		jobID = fmt.Sprintf("job_%d_%04d", now.UnixNano()/1e6, time.Now().Nanosecond()%10000)
	}

	priority := req.Priority
	if priority == "" {
		priority = model.PriorityDefault
	}

	maxRetries := 3
	if req.MaxRetries != nil && *req.MaxRetries >= 0 {
		maxRetries = *req.MaxRetries
	}

	retryDelay := 10
	if req.RetryDelaySeconds != nil && *req.RetryDelaySeconds > 0 {
		retryDelay = *req.RetryDelaySeconds
	}

	timeoutSec := 300
	if req.TimeoutSeconds != nil && *req.TimeoutSeconds > 0 {
		timeoutSec = *req.TimeoutSeconds
	}

	runAt := now
	if req.DelaySeconds != nil && *req.DelaySeconds > 0 {
		runAt = now.Add(time.Duration(*req.DelaySeconds) * time.Second)
	} else if req.RunAt != nil && !req.RunAt.IsZero() {
		runAt = req.RunAt.UTC()
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	job := model.Job{
		ID:                jobID,
		Type:              req.Type,
		Payload:           payload,
		Status:            model.StatusPending,
		Priority:          priority,
		Attempts:          0,
		MaxRetries:        maxRetries,
		RetryDelaySeconds: retryDelay,
		TimeoutSeconds:    timeoutSec,
		RunAt:             runAt,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	// 1. Save to persistent PostgreSQL storage
	if err := h.jobRepo.Create(r.Context(), job); err != nil {
		renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist job: %v", err))
		return
	}

	// 2. Push to Redis Broker (Queue list or Delayed ZSET)
	if h.broker != nil {
		if err := h.broker.Enqueue(r.Context(), &job); err != nil {
			renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to enqueue job in Redis: %v", err))
			return
		}
	}

	renderJSON(w, http.StatusCreated, job)
}

// ListJobs retrieves paginated jobs with status, type, and priority filtering.
func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	offset, _ := strconv.Atoi(q.Get("offset"))
	if offset < 0 {
		offset = 0
	}

	var statusFilter *model.JobStatus
	if s := q.Get("status"); s != "" {
		st := model.JobStatus(s)
		statusFilter = &st
	}

	var typeFilter *string
	if t := q.Get("type"); t != "" {
		typeFilter = &t
	}

	var priorityFilter *model.JobPriority
	if p := q.Get("priority"); p != "" {
		pr := model.JobPriority(p)
		priorityFilter = &pr
	}

	var searchFilter *string
	if search := q.Get("search"); search != "" {
		searchFilter = &search
	}

	filter := model.JobFilter{
		Status:   statusFilter,
		Type:     typeFilter,
		Priority: priorityFilter,
		Search:   searchFilter,
		Limit:    limit,
		Offset:   offset,
	}

	jobs, total, err := h.jobRepo.List(r.Context(), filter)
	if err != nil {
		renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list jobs: %v", err))
		return
	}

	renderJSON(w, http.StatusOK, map[string]interface{}{
		"data":   jobs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// GetJob fetches a job by ID and includes its execution log history.
func (h *JobHandler) GetJob(w http.ResponseWriter, r *http.Request, id string) {
	job, err := h.jobRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrJobNotFound) {
			renderError(w, http.StatusNotFound, "Job not found")
			return
		}
		renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get job: %v", err))
		return
	}

	logs, _ := h.jobRepo.GetLogsByJobID(r.Context(), id)

	renderJSON(w, http.StatusOK, map[string]interface{}{
		"job":  job,
		"logs": logs,
	})
}

// RetryJob manually re-queues a failed or dead job.
func (h *JobHandler) RetryJob(w http.ResponseWriter, r *http.Request, id string) {
	job, err := h.jobRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrJobNotFound) {
			renderError(w, http.StatusNotFound, "Job not found")
			return
		}
		renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch job: %v", err))
		return
	}

	if err := h.jobRepo.ResetForRetry(r.Context(), id); err != nil {
		renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to reset job: %v", err))
		return
	}

	job.Status = model.StatusPending
	job.RunAt = time.Now().UTC()

	if h.broker != nil {
		if err := h.broker.Enqueue(r.Context(), job); err != nil {
			renderError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to re-enqueue job in Redis: %v", err))
			return
		}
	}

	renderJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Job successfully queued for retry",
		"job_id":  id,
	})
}

// CancelJob marks a pending/scheduled job as cancelled.
func (h *JobHandler) CancelJob(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.jobRepo.MarkCancelled(r.Context(), id); err != nil {
		renderError(w, http.StatusBadRequest, err.Error())
		return
	}

	renderJSON(w, http.StatusOK, map[string]interface{}{
		"message": "Job cancelled successfully",
		"job_id":  id,
	})
}

func renderJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func renderError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
