package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/SHREYA110711/taskflow/internal/queue"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

type StatsHandler struct {
	jobRepo repository.JobRepositoryInterface
	broker  queue.Broker
}

func NewStatsHandler(jobRepo repository.JobRepositoryInterface, broker queue.Broker) *StatsHandler {
	return &StatsHandler{
		jobRepo: jobRepo,
		broker:  broker,
	}
}

// GetStats returns aggregated metrics from both PostgreSQL and Redis.
func (h *StatsHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	// 1. Fetch DB Stats
	dbStats, err := h.jobRepo.GetStats(ctx)
	if err != nil {
		dbStats = &model.JobStats{}
	}

	// 2. Fetch Redis Queue Stats
	var queueStats *queue.QueueStats
	if h.broker != nil {
		queueStats, _ = h.broker.GetQueueStats(ctx)
	}
	if queueStats == nil {
		queueStats = &queue.QueueStats{}
	}

	renderJSON(w, http.StatusOK, map[string]interface{}{
		"database": dbStats,
		"queues":   queueStats,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
