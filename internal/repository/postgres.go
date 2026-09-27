package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// NewPostgresDB establishes and configures a pooled PostgreSQL connection.
func NewPostgresDB(databaseURL string) (*sql.DB, error) {
	dsn := databaseURL
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/taskflow?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Automatically run database migrations to create required tables and indexes
	if err := AutoMigrate(db); err != nil {
		return nil, fmt.Errorf("failed to run database auto-migration: %w", err)
	}

	return db, nil
}

// AutoMigrate creates tables, indexes, and triggers if they do not already exist.
func AutoMigrate(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id VARCHAR(128) PRIMARY KEY,
		type VARCHAR(128) NOT NULL,
		payload JSONB NOT NULL DEFAULT '{}',
		status VARCHAR(32) NOT NULL DEFAULT 'pending',
		priority VARCHAR(32) NOT NULL DEFAULT 'default',
		attempts INT NOT NULL DEFAULT 0,
		max_retries INT NOT NULL DEFAULT 3,
		retry_delay_seconds INT NOT NULL DEFAULT 10,
		timeout_seconds INT NOT NULL DEFAULT 300,
		run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		started_at TIMESTAMPTZ,
		completed_at TIMESTAMPTZ,
		failed_at TIMESTAMPTZ,
		result JSONB,
		last_error TEXT,
		locked_by VARCHAR(128),
		locked_until TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
	CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);
	CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority);
	CREATE INDEX IF NOT EXISTS idx_jobs_run_at ON jobs(run_at);
	CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC);
	CREATE INDEX IF NOT EXISTS idx_jobs_status_run_at ON jobs(status, run_at);

	CREATE TABLE IF NOT EXISTS job_logs (
		id BIGSERIAL PRIMARY KEY,
		job_id VARCHAR(128) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		attempt INT NOT NULL DEFAULT 1,
		status VARCHAR(32) NOT NULL,
		worker_id VARCHAR(128) NOT NULL,
		message TEXT NOT NULL,
		error TEXT,
		duration_ms BIGINT NOT NULL DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
	CREATE INDEX IF NOT EXISTS idx_job_logs_created_at ON job_logs(created_at DESC);
	`

	_, err := db.Exec(schema)
	return err
}
