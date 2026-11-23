package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kenny1031/gotranscode/internal/transcode"
)

const (
	pollInterval    = 2 * time.Second
	databaseTimeout = 5 * time.Second
	jobTimeout      = 2 * time.Minute
)

type Job struct {
	ID        string
	InputPath string
	Profile   string
	Attempts  int
}

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	// Configure the number of concurrent workers.
	workerCount := 2

	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 8 {
			log.Fatal("WORKER_CONCURRENCY must be between 1 and 8")
		}
		workerCount = n
	}

	// Initialise PostgreSQL connection pool.
	startupCtx, cancel := context.WithTimeout(
		context.Background(),
		databaseTimeout,
	)

	pool, err := pgxpool.New(startupCtx, dbURL)
	if err != nil {
		cancel()
		log.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(startupCtx); err != nil {
		cancel()
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	cancel()

	// Shutdown signal stops new job claims.
	stopCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	var wg sync.WaitGroup

	log.Printf(
		"starting worker pool: concurrency=%d",
		workerCount,
	)

	// Launch a fixed number of worker goroutines.
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			workerLoop(stopCtx, pool, workerID)
		}(i)
	}

	// Wait until SIGINT or SIGTERM.
	<-stopCtx.Done()

	log.Println("shutdown requested: stopping new claims")

	// Restore default signal behavior for subsequent signals.
	stop()

	// Wait for all workers to finish their current jobs.
	wg.Wait()

	log.Println("all workers stopped")
}

// workerLoop repeatedly claims and processes jobs.
func workerLoop(
	ctx context.Context,
	pool *pgxpool.Pool,
	workerID int,
) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	log.Printf("[worker-%d] started", workerID)

	for {
		// Stop before attempting another claim.
		if ctx.Err() != nil {
			break
		}

		processed, err := processOne(ctx, pool, workerID)

		if err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf(
				"[worker-%d] error: %v",
				workerID,
				err,
			)
		}

		// If a job was processed, check for more immediately.
		if processed {
			continue
		}

		// No job available: wait or exit on shutdown.
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] stopped", workerID)
			return

		case <-ticker.C:
		}
	}

	log.Printf("[worker-%d] stopped", workerID)
}

// claimNextJob atomically claims one queued job.
func claimNextJob(
	ctx context.Context,
	pool *pgxpool.Pool,
) (Job, bool, error) {
	var job Job

	err := pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM jobs
			WHERE status = 'queued'
			ORDER BY
				priority DESC,
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
			error_message = NULL
		FROM candidate AS c
		WHERE j.id = c.id
		RETURNING
			j.id::text,
			j.input_path,
			j.profile,
			j.attempts
	`).Scan(
		&job.ID,
		&job.InputPath,
		&job.Profile,
		&job.Attempts,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("claim job: %w", err)
	}

	return job, true, nil
}

// processOne claims and executes at most one job.
func processOne(
	stopCtx context.Context,
	pool *pgxpool.Pool,
	workerID int,
) (bool, error) {
	claimCtx, cancel := context.WithTimeout(
		stopCtx,
		databaseTimeout,
	)

	job, found, err := claimNextJob(claimCtx, pool)
	cancel()

	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	log.Printf(
		"[worker-%d] claimed job %s (attempt=%d)",
		workerID,
		job.ID,
		job.Attempts,
	)

	// Execution is deliberately independent of shutdown context:
	// an already-claimed job can finish before the worker exits.
	outputPath, runErr := executeJob(job)

	// Save the result even if shutdown has been requested.
	updateCtx, updateCancel := context.WithTimeout(
		context.Background(),
		databaseTimeout,
	)
	defer updateCancel()

	if runErr != nil {
		err := markFailed(
			updateCtx,
			pool,
			job.ID,
			runErr.Error(),
		)
		if err != nil {
			return true, fmt.Errorf(
				"job %s failed, but DB update failed: %w",
				job.ID,
				err,
			)
		}

		log.Printf(
			"[worker-%d] job %s failed: %v",
			workerID,
			job.ID,
			runErr,
		)
		return true, nil
	}

	if err := markCompleted(
		updateCtx,
		pool,
		job.ID,
		outputPath,
	); err != nil {
		return true, fmt.Errorf(
			"job %s finished, but DB update failed: %w",
			job.ID,
			err,
		)
	}

	log.Printf(
		"[worker-%d] completed job %s: %s",
		workerID,
		job.ID,
		outputPath,
	)

	return true, nil
}

// executeJob starts FFmpeg and publishes the output.
func executeJob(job Job) (string, error) {
	inputPath := filepath.Clean(job.InputPath)

	// Restrict input to a direct file inside input/.
	if filepath.IsAbs(inputPath) ||
		filepath.Dir(inputPath) != "input" {
		return "", errors.New(
			"input must be a file inside input/",
		)
	}

	info, err := os.Lstat(inputPath)
	if err != nil {
		return "", fmt.Errorf("input file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("input must be a regular file")
	}

	if err := os.MkdirAll("output", 0755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}

	finalPath := filepath.Join("output", job.ID+".mp4")
	tempPath := filepath.Join(
		"output",
		job.ID+".partial.mp4",
	)

	// Never intentionally overwrite an existing final result.
	if _, err := os.Stat(finalPath); err == nil {
		return "", fmt.Errorf(
			"final output already exists: %s",
			finalPath,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("check output: %w", err)
	}

	// Remove an incomplete file from a previous attempt.
	if err := os.Remove(tempPath); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("remove partial file: %w", err)
	}
	defer os.Remove(tempPath)

	jobCtx, cancel := context.WithTimeout(
		context.Background(),
		jobTimeout,
	)
	defer cancel()

	_, err = transcode.Run(
		jobCtx,
		inputPath,
		tempPath,
		transcode.Profile(job.Profile),
	)
	if err != nil {
		return "", fmt.Errorf("FFmpeg transcoding: %w", err)
	}

	// Both paths are on the same filesystem.
	if err := os.Rename(tempPath, finalPath); err != nil {
		return "", fmt.Errorf("publish output: %w", err)
	}

	return finalPath, nil
}

// markCompleted persists a successful execution.
func markCompleted(
	ctx context.Context,
	pool *pgxpool.Pool,
	id string,
	outputPath string,
) error {
	tag, err := pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = 'completed',
			output_path = $2,
			completed_at = NOW(),
			error_message = NULL
		WHERE id = $1::uuid
		  AND status = 'processing'
	`, id, outputPath)

	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("job was not in processing state")
	}
	return nil
}

// markFailed persists a failed execution.
func markFailed(
	ctx context.Context,
	pool *pgxpool.Pool,
	id string,
	message string,
) error {
	tag, err := pool.Exec(ctx, `
		UPDATE jobs
		SET
			status = 'failed',
			error_message = $2,
			completed_at = NOW()
		WHERE id = $1::uuid
		  AND status = 'processing'
	`, id, message)

	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("job was not in processing state")
	}
	return nil
}
