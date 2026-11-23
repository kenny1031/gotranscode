package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrJobNotFound indicates that a job does not exist.
var ErrJobNotFound = errors.New("job not found")

// PostgresJobStore manages persistent job storage.
type PostgresJobStore struct {
	pool *pgxpool.Pool
}

// NewPostgresJobStore creates a PostgreSQL-backed job store.
func NewPostgresJobStore(pool *pgxpool.Pool) *PostgresJobStore {
	return &PostgresJobStore{
		pool: pool,
	}
}

// Create inserts a new job into PostgreSQL.
func (s *PostgresJobStore) Create(
	ctx context.Context,
	req CreateJobRequest,
) (Job, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return Job{}, fmt.Errorf("generate job ID: %w", err)
	}

	var job Job
	var status string

	err = s.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			id,
			input_path,
			profile,
			priority
		)
		VALUES ($1, $2, $3, $4)
		RETURNING
			id::text,
			input_path,
			profile,
			priority,
			status,
			created_at
	`,
		id.String(),
		req.InputPath,
		req.Profile,
		req.Priority,
	).Scan(
		&job.ID,
		&job.InputPath,
		&job.Profile,
		&job.Priority,
		&status,
		&job.CreatedAt,
	)

	if err != nil {
		return Job{}, fmt.Errorf("insert job: %w", err)
	}

	job.Status = JobStatus(status)
	return job, nil
}

// Get retrieves a job by UUID.
func (s *PostgresJobStore) Get(
	ctx context.Context,
	id string,
) (Job, error) {
	var job Job
	var status string

	err := s.pool.QueryRow(ctx, `
		SELECT
			id::text,
			input_path,
			profile,
			priority,
			status,
			created_at
		FROM jobs
		WHERE id = $1::uuid
	`, id).Scan(
		&job.ID,
		&job.InputPath,
		&job.Profile,
		&job.Priority,
		&status,
		&job.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}

	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}

	job.Status = JobStatus(status)
	return job, nil
}

// List returns all jobs ordered by creation time.
func (s *PostgresJobStore) List(
	ctx context.Context,
) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id::text,
			input_path,
			profile,
			priority,
			status,
			created_at
		FROM jobs
		ORDER BY created_at ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		var job Job
		var status string

		err := rows.Scan(
			&job.ID,
			&job.InputPath,
			&job.Profile,
			&job.Priority,
			&status,
			&job.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}

		job.Status = JobStatus(status)
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}

	return jobs, nil
}
