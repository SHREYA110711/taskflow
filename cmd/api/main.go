package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/SHREYA110711/taskflow/internal/config"
	"github.com/SHREYA110711/taskflow/internal/handler"
	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

func main() {
	cfg := config.Load()

	log.Printf("Starting TaskFlow API on port :%s...", cfg.Port)

	db, err := repository.NewPostgresDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	log.Println("✅ Successfully connected to PostgreSQL & ran schema migrations")

	jobRepository := repository.NewJobRepository(db)
	jobHandler := handler.NewJobHandler(jobRepository)

	http.HandleFunc("/jobs", jobHandler.CreateJob)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "healthy",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "TaskFlow Distributed Job Queue API is running")
	})

	// Sample test job creation
	testJob := model.Job{
		ID:        fmt.Sprintf("job_%d", time.Now().Unix()),
		Type:      "send_email",
		Payload:   json.RawMessage(`{"to":"interview@taskflow.dev","subject":"Welcome to TaskFlow"}`),
		Status:    model.StatusPending,
		Priority:  model.PriorityDefault,
		Attempts:  0,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := jobRepository.Create(context.Background(), testJob); err != nil {
		log.Printf("⚠️ Note: test job creation returned: %v", err)
	} else {
		log.Printf("🚀 Test job created successfully with ID: %s", testJob.ID)
	}

	log.Printf("TaskFlow HTTP server listening on http://localhost:%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
		log.Fatalf("Server stopped unexpectedly: %v", err)
	}
}
