package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SHREYA110711/taskflow/internal/model"
)

var (
	ErrJobNotFound = errors.New("job not found")
)

// JobRepositoryInterface defines repository operations for jobs and logs.
type JobRepositoryInterface interface {
	Create(ctx context.Context, job model.Job) error
	GetByID(ctx context.Context, id string) (*model.Job, error)
	MarkProcessing(ctx context.Context, id string, workerID string, timeoutSeconds int) error
	MarkCompleted(ctx context.Context, id string, result json.RawMessage) error
	MarkFailed(ctx context.Context, id string, errMsg string, willRetry bool, nextRunAt time.Time) error
	ResetForRetry(ctx context.Context, id string) error
	MarkCancelled(ctx context.Context, id string) error
	List(ctx context.Context, filter model.JobFilter) ([]model.Job, int64, error)
	GetStats(ctx context.Context) (*model.JobStats, error)
	CreateLog(ctx context.Context, log model.JobLog) error
	GetLogsByJobID(ctx context.Context, jobID string) ([]model.JobLog, error)
	Delete(ctx context.Context, id string) error
}

type JobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) *JobRepository {
	return &JobRepository{
		db: db,
	}
}

// Create inserts a new job into the database.
func (r *JobRepository) Create(ctx context.Context, job model.Job) error {
	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = now
	}
	if job.RunAt.IsZero() {
		job.RunAt = now
	}
	if job.Status == "" {
		job.Status = model.StatusPending
	}
	if job.Priority == "" {
		job.Priority = model.PriorityDefault
	}
	if job.MaxRetries == 0 {
		job.MaxRetries = 3
	}
	if job.RetryDelaySeconds == 0 {
		job.RetryDelaySeconds = 10
	}
	if job.TimeoutSeconds == 0 {
		job.TimeoutSeconds = 300
	}
	if len(job.Payload) == 0 {
		job.Payload = []byte("{}")
	}

	query := `
		INSERT INTO jobs (
			id, type, payload, status, priority, attempts, max_retries,
			retry_delay_seconds, timeout_seconds, run_at, started_at,
			completed_at, failed_at, result, last_error, locked_by, locked_until,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
	`

	var resultRaw []byte
	if len(job.Result) > 0 {
		resultRaw = job.Result
	}

	_, err := r.db.ExecContext(ctx, query,
		job.ID,
		job.Type,
		string(job.Payload),
		string(job.Status),
		string(job.Priority),
		job.Attempts,
		job.MaxRetries,
		job.RetryDelaySeconds,
		job.TimeoutSeconds,
		job.RunAt,
		job.StartedAt,
		job.CompletedAt,
		job.FailedAt,
		resultRaw,
		job.LastError,
		job.LockedBy,
		job.LockedUntil,
		job.CreatedAt,
		job.UpdatedAt,
	)

	return err
}

// GetByID fetches a job by its unique identifier.
func (r *JobRepository) GetByID(ctx context.Context, id string) (*model.Job, error) {
	query := `
		SELECT
			id, type, payload, status, priority, attempts, max_retries,
			retry_delay_seconds, timeout_seconds, run_at, started_at,
			completed_at, failed_at, result, last_error, locked_by, locked_until,
			created_at, updated_at
		FROM jobs
		WHERE id = $1
	`

	var (
		job           model.Job
		payloadStr    string
		resultBytes   []byte
		lastError     sql.NullString
		lockedBy      sql.NullString
		lockedUntil   sql.NullTime
		startedAt     sql.NullTime
		completedAt   sql.NullTime
		failedAt      sql.NullTime
	)

	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&job.ID,
		&job.Type,
		&payloadStr,
		&job.Status,
		&job.Priority,
		&job.Attempts,
		&job.MaxRetries,
		&job.RetryDelaySeconds,
		&job.TimeoutSeconds,
		&job.RunAt,
		&startedAt,
		&completedAt,
		&failedAt,
		&resultBytes,
		&lastError,
		&lockedBy,
		&lockedUntil,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}

	job.Payload = json.RawMessage(payloadStr)
	if len(resultBytes) > 0 {
		job.Result = json.RawMessage(resultBytes)
	}
	if lastError.Valid {
		job.LastError = lastError.String
	}
	if lockedBy.Valid {
		job.LockedBy = lockedBy.String
	}
	if lockedUntil.Valid {
		job.LockedUntil = &lockedUntil.Time
	}
	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}
	if failedAt.Valid {
		job.FailedAt = &failedAt.Time
	}

	return &job, nil
}

// MarkProcessing updates job state to 'processing' and increments attempt count.
func (r *JobRepository) MarkProcessing(ctx context.Context, id string, workerID string, timeoutSeconds int) error {
	now := time.Now().UTC()
	lockedUntil := now.Add(time.Duration(timeoutSeconds) * time.Second)

	query := `
		UPDATE jobs
		SET
			status = $1,
			attempts = attempts + 1,
			started_at = $2,
			locked_by = $3,
			locked_until = $4,
			updated_at = $5
		WHERE id = $6
	`

	res, err := r.db.ExecContext(ctx, query, model.StatusProcessing, now, workerID, lockedUntil, now, id)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}

// MarkCompleted marks a job as successfully completed.
func (r *JobRepository) MarkCompleted(ctx context.Context, id string, result json.RawMessage) error {
	now := time.Now().UTC()
	var resultRaw []byte
	if len(result) > 0 {
		resultRaw = result
	}

	query := `
		UPDATE jobs
		SET
			status = $1,
			result = $2,
			completed_at = $3,
			locked_by = NULL,
			locked_until = NULL,
			updated_at = $4
		WHERE id = $5
	`

	res, err := r.db.ExecContext(ctx, query, model.StatusCompleted, resultRaw, now, now, id)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}

// MarkFailed updates the job on failure, setting status to 'retrying' or 'failed'.
func (r *JobRepository) MarkFailed(ctx context.Context, id string, errMsg string, willRetry bool, nextRunAt time.Time) error {
	now := time.Now().UTC()
	status := model.StatusFailed
	if willRetry {
		status = model.StatusRetrying
	}

	query := `
		UPDATE jobs
		SET
			status = $1,
			last_error = $2,
			failed_at = $3,
			run_at = $4,
			locked_by = NULL,
			locked_until = NULL,
			updated_at = $5
		WHERE id = $6
	`

	res, err := r.db.ExecContext(ctx, query, status, errMsg, now, nextRunAt, now, id)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}

// ResetForRetry resets a job to pending status for manual retry.
func (r *JobRepository) ResetForRetry(ctx context.Context, id string) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET
			status = $1,
			run_at = $2,
			locked_by = NULL,
			locked_until = NULL,
			updated_at = $3
		WHERE id = $4
	`

	res, err := r.db.ExecContext(ctx, query, model.StatusPending, now, now, id)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}

// MarkCancelled marks a pending or scheduled job as cancelled.
func (r *JobRepository) MarkCancelled(ctx context.Context, id string) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs
		SET
			status = $1,
			locked_by = NULL,
			locked_until = NULL,
			updated_at = $2
		WHERE id = $3 AND status IN ('pending', 'queued', 'retrying')
	`

	res, err := r.db.ExecContext(ctx, query, model.StatusCancelled, now, id)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("job not found or cannot be cancelled in current state")
	}

	return nil
}

// List retrieves paginated jobs with optional filters.
func (r *JobRepository) List(ctx context.Context, filter model.JobFilter) ([]model.Job, int64, error) {
	if filter.Limit <= 0 {
		filter.Limit = 20
	}
	if filter.Limit > 100 {
		filter.Limit = 100
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	var conditions []string
	var args []interface{}
	argIdx := 1

	if filter.Status != nil && *filter.Status != "" {
		conditions = append(conditions, fmt.Sprintf("status = $%d", argIdx))
		args = append(args, *filter.Status)
		argIdx++
	}

	if filter.Type != nil && *filter.Type != "" {
		conditions = append(conditions, fmt.Sprintf("type = $%d", argIdx))
		args = append(args, *filter.Type)
		argIdx++
	}

	if filter.Priority != nil && *filter.Priority != "" {
		conditions = append(conditions, fmt.Sprintf("priority = $%d", argIdx))
		args = append(args, *filter.Priority)
		argIdx++
	}

	if filter.Search != nil && *filter.Search != "" {
		conditions = append(conditions, fmt.Sprintf("(id ILIKE $%d OR type ILIKE $%d OR last_error ILIKE $%d)", argIdx, argIdx, argIdx))
		searchPattern := "%" + *filter.Search + "%"
		args = append(args, searchPattern)
		argIdx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", whereClause)
	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Fetch page
	listQuery := fmt.Sprintf(`
		SELECT
			id, type, payload, status, priority, attempts, max_retries,
			retry_delay_seconds, timeout_seconds, run_at, started_at,
			completed_at, failed_at, result, last_error, locked_by, locked_until,
			created_at, updated_at
		FROM jobs
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	queryArgs := append(args, filter.Limit, filter.Offset)

	rows, err := r.db.QueryContext(ctx, listQuery, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	jobs := make([]model.Job, 0)
	for rows.Next() {
		var (
			job         model.Job
			payloadStr  string
			resultBytes []byte
			lastError   sql.NullString
			lockedBy    sql.NullString
			lockedUntil sql.NullTime
			startedAt   sql.NullTime
			completedAt sql.NullTime
			failedAt    sql.NullTime
		)

		if err := rows.Scan(
			&job.ID,
			&job.Type,
			&payloadStr,
			&job.Status,
			&job.Priority,
			&job.Attempts,
			&job.MaxRetries,
			&job.RetryDelaySeconds,
			&job.TimeoutSeconds,
			&job.RunAt,
			&startedAt,
			&completedAt,
			&failedAt,
			&resultBytes,
			&lastError,
			&lockedBy,
			&lockedUntil,
			&job.CreatedAt,
			&job.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}

		job.Payload = json.RawMessage(payloadStr)
		if len(resultBytes) > 0 {
			job.Result = json.RawMessage(resultBytes)
		}
		if lastError.Valid {
			job.LastError = lastError.String
		}
		if lockedBy.Valid {
			job.LockedBy = lockedBy.String
		}
		if lockedUntil.Valid {
			job.LockedUntil = &lockedUntil.Time
		}
		if startedAt.Valid {
			job.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			job.CompletedAt = &completedAt.Time
		}
		if failedAt.Valid {
			job.FailedAt = &failedAt.Time
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}

// GetStats returns counts grouped by job status.
func (r *JobRepository) GetStats(ctx context.Context) (*model.JobStats, error) {
	query := `
		SELECT
			COUNT(*) as total,
			COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) as pending,
			COALESCE(SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END), 0) as queued,
			COALESCE(SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END), 0) as processing,
			COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0) as completed,
			COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) as failed,
			COALESCE(SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END), 0) as retrying,
			COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0) as cancelled
		FROM jobs
	`

	var stats model.JobStats
	err := r.db.QueryRowContext(ctx, query).Scan(
		&stats.Total,
		&stats.Pending,
		&stats.Queued,
		&stats.Processing,
		&stats.Completed,
		&stats.Failed,
		&stats.Retrying,
		&stats.Cancelled,
	)
	if err != nil {
		return nil, err
	}

	return &stats, nil
}

// CreateLog appends an audit/execution log entry for a job attempt.
func (r *JobRepository) CreateLog(ctx context.Context, log model.JobLog) error {
	now := time.Now().UTC()
	if log.CreatedAt.IsZero() {
		log.CreatedAt = now
	}

	query := `
		INSERT INTO job_logs (job_id, attempt, status, worker_id, message, error, duration_ms, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := r.db.ExecContext(ctx, query,
		log.JobID,
		log.Attempt,
		string(log.Status),
		log.WorkerID,
		log.Message,
		log.Error,
		log.DurationMs,
		log.CreatedAt,
	)

	return err
}

// GetLogsByJobID fetches all execution logs for a job in chronological order.
func (r *JobRepository) GetLogsByJobID(ctx context.Context, jobID string) ([]model.JobLog, error) {
	query := `
		SELECT id, job_id, attempt, status, worker_id, message, error, duration_ms, created_at
		FROM job_logs
		WHERE job_id = $1
		ORDER BY id ASC
	`

	rows, err := r.db.QueryContext(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	logs := make([]model.JobLog, 0)
	for rows.Next() {
		var (
			l        model.JobLog
			errStr   sql.NullString
			statusStr string
		)

		if err := rows.Scan(
			&l.ID,
			&l.JobID,
			&l.Attempt,
			&statusStr,
			&l.WorkerID,
			&l.Message,
			&errStr,
			&l.DurationMs,
			&l.CreatedAt,
		); err != nil {
			return nil, err
		}

		l.Status = model.JobStatus(statusStr)
		if errStr.Valid {
			l.Error = errStr.String
		}

		logs = append(logs, l)
	}

	return logs, rows.Err()
}

// Delete permanently removes a job and its logs (via ON DELETE CASCADE).
func (r *JobRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM jobs WHERE id = $1`
	res, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	return nil
}
