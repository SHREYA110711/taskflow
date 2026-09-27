package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/SHREYA110711/taskflow/internal/config"
	"github.com/SHREYA110711/taskflow/internal/handler"
	"github.com/SHREYA110711/taskflow/internal/queue"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

func main() {
	cfg := config.Load()

	log.Println("==================================================")
	log.Println("         TaskFlow Distributed API Server          ")
	log.Println("==================================================")
	log.Printf("Connecting to PostgreSQL...")

	db, err := repository.NewPostgresDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("PostgreSQL connection failed: %v", err)
	}
	defer db.Close()
	log.Println("✅ Connected to PostgreSQL & ran schema auto-migrations")

	log.Printf("Connecting to Redis Broker at %s...", cfg.RedisAddr)
	broker, err := queue.NewRedisBroker(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Printf("⚠️ Warning: Redis connection failed (%v). API running in DB-only mode.", err)
	} else {
		defer broker.Close()
		log.Println("✅ Connected to Redis Broker")
	}

	jobRepo := repository.NewJobRepository(db)
	jobHandler := handler.NewJobHandler(jobRepo, broker)
	statsHandler := handler.NewStatsHandler(jobRepo, broker)

	mux := http.NewServeMux()

	dashboardHandler := handler.NewDashboardHandler()

	// REST API Routes
	mux.HandleFunc("/api/v1/jobs", jobHandler.ServeHTTP)
	mux.HandleFunc("/api/v1/jobs/", jobHandler.ServeHTTP)
	mux.HandleFunc("/api/v1/stats", statsHandler.GetStats)

	// Health Check Route
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		dbErr := db.PingContext(r.Context())
		redisStatus := "connected"
		if broker == nil || broker.Ping(r.Context()) != nil {
			redisStatus = "unavailable"
		}

		dbStatus := "connected"
		if dbErr != nil {
			dbStatus = "unavailable"
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "healthy",
			"database": dbStatus,
			"redis":    redisStatus,
			"time":     time.Now().UTC().Format(time.RFC3339),
		})
	})

	// Monitoring Dashboard Routes
	mux.HandleFunc("/dashboard", dashboardHandler.ServeHTTP)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		dashboardHandler.ServeHTTP(w, r)
	})

	// Wrap mux with middleware chain: Recovery -> CORS -> Logger -> Mux
	handlerChain := handler.RecoveryMiddleware(
		handler.CORSMiddleware(
			handler.LoggerMiddleware(mux),
		),
	)

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handlerChain,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("🚀 TaskFlow API listening on http://localhost:%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server stopped unexpectedly: %v", err)
	}
}
