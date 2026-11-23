package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrLeaseLost = errors.New("job lease lost")

type Job struct {
	ID          string
	InputPath   string
	Profile     string
	Attempts    int
	MaxAttempts int
	LeaseToken  string
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// claimNextJob atomically claims one ready job.
// Every claim receives a new fencing token.
func (s *Store) claimNextJob(
	ctx context.Context,
) (Job, bool, error) {
	token, err := uuid.NewRandom()
	if err != nil {
		return Job{}, false, err
	}

	var job Job

	err = s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM jobs
			WHERE status IN ('queued', 'retrying')
			  AND available_at <= NOW()
			  AND attempts < max_attempts
			ORDER BY
				priority DESC,
				available_at ASC,
				created_at ASC,
				id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE jobs AS j
		SET
			status = 'processing',
			attempts = j.attempts + 1,
			started_at = NOW(),
			completed_at = NULL,
			error_message = NULL,
			lease_token = $1::uuid,
			lease_expires_at =
				NOW() + ($2::integer * INTERVAL '1 second')
		FROM candidate AS c
		WHERE j.id = c.id
		RETURNING
			j.id::text,
			j.input_path,
			j.profile,
			j.attempts,
			j.max_attempts,
			j.lease_token::text
	`, token.String(), leaseSeconds).Scan(
		&job.ID,
		&job.InputPath,
		&job.Profile,
		&job.Attempts,
		&job.MaxAttempts,
		&job.LeaseToken,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("claim job: %w", err)
	}

	return job, true, nil
}

// renewLease extends ownership only if the current
// worker still owns a non-expired lease.
func (s *Store) renewLease(
	ctx context.Context,
	job Job,
) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET lease_expires_at =
			NOW() + ($3::integer * INTERVAL '1 second')
		WHERE id = $1::uuid
		  AND lease_token = $2::uuid
		  AND status = 'processing'
		  AND lease_expires_at > NOW()
	`, job.ID, job.LeaseToken, leaseSeconds)

	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}

	return nil
}

// finishJob marks a successful execution as completed.
// Stale workers cannot update a job owned by another attempt.
func (s *Store) finishJob(
	ctx context.Context,
	job Job,
	outputPath string,
) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = 'completed',
			output_path = $3,
			completed_at = NOW(),
			error_message = NULL,
			lease_token = NULL,
			lease_expires_at = NULL
		WHERE id = $1::uuid
		  AND lease_token = $2::uuid
		  AND status = 'processing'
		  AND lease_expires_at > NOW()
	`, job.ID, job.LeaseToken, outputPath)

	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}

	return nil
}

// failOrRetry schedules a retry or marks terminal failure.
func (s *Store) failOrRetry(
	ctx context.Context,
	job Job,
	message string,
) (string, error) {
	var nextStatus string

	err := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET
			status = CASE
				WHEN attempts >= max_attempts
					THEN 'failed'
				ELSE 'retrying'
			END,
			available_at = CASE
				WHEN attempts >= max_attempts
					THEN NOW()
				ELSE NOW() + make_interval(
					secs => LEAST(
						60,
						2 * (1 << LEAST(attempts - 1, 5))
					)
				)
			END,
			completed_at = CASE
				WHEN attempts >= max_attempts
					THEN NOW()
				ELSE NULL
			END,
			output_path = NULL,
			error_message = LEFT($3, 2000),
			lease_token = NULL,
			lease_expires_at = NULL
		WHERE id = $1::uuid
		  AND lease_token = $2::uuid
		  AND status = 'processing'
		  AND lease_expires_at > NOW()
		RETURNING status
	`, job.ID, job.LeaseToken, message).Scan(&nextStatus)

	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrLeaseLost
	}
	if err != nil {
		return "", fmt.Errorf("fail or retry job: %w", err)
	}

	return nextStatus, nil
}

// recoverExpiredJobs finds abandoned PROCESSING jobs.
// Multiple recovery processes can safely run concurrently.
func (s *Store) recoverExpiredJobs(
	ctx context.Context,
) error {
	rows, err := s.pool.Query(ctx, `
		WITH expired AS (
			SELECT id
			FROM jobs
			WHERE status = 'processing'
			  AND (
				lease_expires_at IS NULL
				OR lease_expires_at <= NOW()
			  )
			FOR UPDATE SKIP LOCKED
			LIMIT 50
		)
		UPDATE jobs AS j
		SET
			status = CASE
				WHEN j.attempts >= j.max_attempts
					THEN 'failed'
				ELSE 'retrying'
			END,
			available_at = CASE
				WHEN j.attempts >= j.max_attempts
					THEN NOW()
				ELSE NOW() + make_interval(
					secs => LEAST(
						60,
						2 * (1 << LEAST(j.attempts - 1, 5))
					)
				)
			END,
			completed_at = CASE
				WHEN j.attempts >= j.max_attempts
					THEN NOW()
				ELSE NULL
			END,
			output_path = NULL,
			error_message = 'Worker lease expired',
			lease_token = NULL,
			lease_expires_at = NULL
		FROM expired AS e
		WHERE j.id = e.id
		RETURNING j.id::text, j.status
	`)
	if err != nil {
		return fmt.Errorf("recover expired jobs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, status string

		if err := rows.Scan(&id, &status); err != nil {
			return err
		}

		fmt.Printf(
			"[recovery] job=%s new_status=%s\n",
			id, status,
		)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// dbContext gives individual database operations
// a bounded amount of time.
func dbContext(parent context.Context) (
	context.Context,
	context.CancelFunc,
) {
	return context.WithTimeout(parent, databaseTimeout)
}
