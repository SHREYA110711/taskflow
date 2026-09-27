package handler

import (
	"encoding/json"
	"net/http"

	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

type JobHandler struct {
	jobRepository *repository.JobRepository
}

func NewJobHandler(jobRepository *repository.JobRepository) *JobHandler {
	return &JobHandler{
		jobRepository: jobRepository,
	}
}

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var job model.Job

	err := json.NewDecoder(r.Body).Decode(&job)
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	err = h.jobRepository.Create(r.Context(), job)
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(job)
}
