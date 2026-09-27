package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SHREYA110711/taskflow/internal/config"
	"github.com/SHREYA110711/taskflow/internal/queue"
	"github.com/SHREYA110711/taskflow/internal/repository"
	"github.com/SHREYA110711/taskflow/internal/worker"
)

func main() {
	cfg := config.Load()

	log.Println("==================================================")
	log.Println("       TaskFlow Distributed Background Worker     ")
	log.Println("==================================================")
	log.Printf("Connecting to PostgreSQL...")

	db, err := repository.NewPostgresDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("PostgreSQL connection failed: %v", err)
	}
	defer db.Close()

	log.Printf("Connecting to Redis Broker at %s...", cfg.RedisAddr)
	broker, err := queue.NewRedisBroker(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Redis connection failed: %v", err)
	}
	defer broker.Close()

	jobRepo := repository.NewJobRepository(db)
	handlerRegistry := worker.NewRegistry()

	poolCfg := worker.Config{
		Concurrency:  cfg.WorkerConcurrency,
		PollInterval: 1 * time.Second,
	}

	pool := worker.NewPool(jobRepo, broker, handlerRegistry, poolCfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		log.Fatalf("Failed to start worker pool: %v", err)
	}

	log.Println("🚀 TaskFlow Worker is active and listening for jobs...")

	// Listen for OS interrupt / termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigChan
	log.Printf("Received shutdown signal (%v). Initiating graceful termination...", sig)

	cancel()
	pool.Stop()

	log.Println("👋 TaskFlow Worker stopped cleanly.")
}
