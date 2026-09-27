package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
	"github.com/SHREYA110711/taskflow/internal/queue"
	"github.com/SHREYA110711/taskflow/internal/repository"
)

// Config defines worker pool runtime options.
type Config struct {
	Concurrency int
	PollInterval time.Duration
	WorkerIDPrefix string
}

// Pool coordinates concurrent job execution, retries, and scheduled migrations.
type Pool struct {
	repo     *repository.JobRepository
	broker   queue.Broker
	registry *Registry
	config   Config

	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
}

// NewPool initializes a new worker pool.
func NewPool(repo *repository.JobRepository, broker queue.Broker, registry *Registry, cfg Config) *Pool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.WorkerIDPrefix == "" {
		hostname, _ := os.Hostname()
		cfg.WorkerIDPrefix = fmt.Sprintf("worker-%s-%d", hostname, os.Getpid())
	}

	return &Pool{
		repo:     repo,
		broker:   broker,
		registry: registry,
		config:   cfg,
		stopChan: make(chan struct{}),
	}
}

// Start spawns the scheduled job migrator and worker goroutines.
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("worker pool is already running")
	}
	p.running = true
	p.mu.Unlock()

	log.Printf("👷 Starting TaskFlow worker pool with %d concurrent workers...", p.config.Concurrency)

	// 1. Start delayed job scheduler / poller
	p.wg.Add(1)
	go p.runScheduledPoller(ctx)

	// 2. Start worker goroutines
	for i := 1; i <= p.config.Concurrency; i++ {
		workerID := fmt.Sprintf("%s-%02d", p.config.WorkerIDPrefix, i)
		p.wg.Add(1)
		go p.runWorker(ctx, workerID)
	}

	return nil
}

// Stop initiates graceful shutdown, waiting for active jobs to finish.
func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stopChan)
	p.mu.Unlock()

	log.Println("🛑 Gracefully shutting down worker pool. Waiting for in-flight tasks to complete...")
	p.wg.Wait()
	log.Println("✅ Worker pool stopped safely.")
}

// runScheduledPoller periodically migrates due jobs from Redis ZSET to ready Lists.
func (p *Pool) runScheduledPoller(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		case <-ticker.C:
			migrated, err := p.broker.PollScheduledJobs(ctx, 100)
			if err != nil {
				log.Printf("⚠️ Error polling scheduled jobs: %v", err)
			} else if migrated > 0 {
				log.Printf("⏰ Migrated %d scheduled jobs to ready queue", migrated)
			}
		}
	}
}

// runWorker is the execution loop for an individual worker goroutine.
func (p *Pool) runWorker(ctx context.Context, workerID string) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			return
		default:
			// Blocking dequeue up to 2 seconds across High -> Default -> Low queues
			job, err := p.broker.Dequeue(ctx, 2*time.Second, model.PriorityHigh, model.PriorityDefault, model.PriorityLow)
			if err != nil {
				if ctx.Err() == nil && p.running {
					log.Printf("[%s] Dequeue error: %v", workerID, err)
					time.Sleep(500 * time.Millisecond)
				}
				continue
			}

			if job == nil {
				continue // Timeout, check stop conditions and loop
			}

			p.processJob(ctx, workerID, job)
		}
	}
}

// processJob executes a single background job with timeout, error handling, retries, and logging.
func (p *Pool) processJob(ctx context.Context, workerID string, job *model.Job) {
	timeout := time.Duration(job.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 1. Mark processing in PostgreSQL & increment attempt count
	if err := p.repo.MarkProcessing(execCtx, job.ID, workerID, int(timeout.Seconds())); err != nil {
		log.Printf("[%s] Failed to mark job %s as processing: %v", workerID, job.ID, err)
	}

	currentAttempt := job.Attempts + 1
	log.Printf("[%s] ⚡ Processing Job ID: %s (Type: %s, Attempt: %d/%d, Priority: %s)",
		workerID, job.ID, job.Type, currentAttempt, job.MaxRetries, job.Priority)

	startTime := time.Now()

	// 2. Lookup handler and execute
	var result json.RawMessage
	var execErr error

	handler, err := p.registry.Get(job.Type)
	if err != nil {
		execErr = err
	} else {
		// Execute with panic recovery
		result, execErr = p.safeExecute(execCtx, handler, job)
	}

	durationMs := time.Since(startTime).Milliseconds()

	// 3. Handle Completion or Retry/Failure
	if execErr == nil {
		p.handleSuccess(execCtx, workerID, job, currentAttempt, result, durationMs)
	} else {
		p.handleFailure(execCtx, workerID, job, currentAttempt, execErr, durationMs)
	}
}

// safeExecute executes handler function with panic recovery.
func (p *Pool) safeExecute(ctx context.Context, handler Handler, job *model.Job) (res json.RawMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task panicked: %v", r)
		}
	}()

	return handler(ctx, job)
}

// handleSuccess marks job as completed and writes audit log.
func (p *Pool) handleSuccess(ctx context.Context, workerID string, job *model.Job, attempt int, result json.RawMessage, durationMs int64) {
	if err := p.repo.MarkCompleted(ctx, job.ID, result); err != nil {
		log.Printf("[%s] Failed to mark job %s completed: %v", workerID, job.ID, err)
	}

	_ = p.repo.CreateLog(ctx, model.JobLog{
		JobID:      job.ID,
		Attempt:    attempt,
		Status:     model.StatusCompleted,
		WorkerID:   workerID,
		Message:    fmt.Sprintf("Job completed successfully in %dms", durationMs),
		DurationMs: durationMs,
	})

	log.Printf("[%s] 🎉 Job %s COMPLETED in %dms", workerID, job.ID, durationMs)
}

// handleFailure applies exponential backoff retry or moves to DLQ on max attempts.
func (p *Pool) handleFailure(ctx context.Context, workerID string, job *model.Job, attempt int, execErr error, durationMs int64) {
	errStr := execErr.Error()
	willRetry := ShouldRetry(attempt, job.MaxRetries)

	if willRetry {
		backoffDuration := CalculateBackoff(attempt, job.RetryDelaySeconds)
		nextRunAt := time.Now().UTC().Add(backoffDuration)

		log.Printf("[%s] 🔁 Job %s FAILED (Attempt %d/%d). Retrying in %v (at %s). Error: %s",
			workerID, job.ID, attempt, job.MaxRetries, backoffDuration.Round(time.Second), nextRunAt.Format("15:04:05"), errStr)

		if err := p.repo.MarkFailed(ctx, job.ID, errStr, true, nextRunAt); err != nil {
			log.Printf("[%s] Failed to update job %s retry status: %v", workerID, job.ID, err)
		}

		_ = p.repo.CreateLog(ctx, model.JobLog{
			JobID:      job.ID,
			Attempt:    attempt,
			Status:     model.StatusRetrying,
			WorkerID:   workerID,
			Message:    fmt.Sprintf("Attempt failed, scheduled retry in %v", backoffDuration.Round(time.Second)),
			Error:      errStr,
			DurationMs: durationMs,
		})

		// Re-schedule in Redis ZSET
		job.Attempts = attempt
		job.RunAt = nextRunAt
		job.Status = model.StatusRetrying
		job.LastError = errStr

		if err := p.broker.ScheduleDelayed(ctx, job); err != nil {
			log.Printf("[%s] CRITICAL: Failed to re-schedule retry in Redis: %v", workerID, err)
		}
	} else {
		log.Printf("[%s] 💥 Job %s EXHAUSTED MAX RETRIES (%d/%d). Moving to Dead Letter Queue (DLQ). Error: %s",
			workerID, job.ID, attempt, job.MaxRetries, errStr)

		if err := p.repo.MarkFailed(ctx, job.ID, errStr, false, time.Now().UTC()); err != nil {
			log.Printf("[%s] Failed to mark job %s failed: %v", workerID, job.ID, err)
		}

		_ = p.repo.CreateLog(ctx, model.JobLog{
			JobID:      job.ID,
			Attempt:    attempt,
			Status:     model.StatusFailed,
			WorkerID:   workerID,
			Message:    fmt.Sprintf("Job permanently failed after %d attempts", attempt),
			Error:      errStr,
			DurationMs: durationMs,
		})

		// Place in Redis DLQ
		if err := p.broker.MoveToDLQ(ctx, job, errStr); err != nil {
			log.Printf("[%s] Failed to push job %s to DLQ: %v", workerID, job.ID, err)
		}
	}
}
