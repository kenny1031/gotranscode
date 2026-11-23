package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		databaseTimeout,
	)
	if err := pool.Ping(ctx); err != nil {
		cancel()
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	cancel()

	// Stop polling when SIGINT or SIGTERM is received.
	stopCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Println("GoTranscode worker started")

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if stopCtx.Err() != nil {
			log.Println("worker stopped")
			return
		}

		if err := processOne(pool); err != nil {
			log.Printf("worker error: %v", err)
		}

		select {
		case <-stopCtx.Done():
			log.Println("worker stopped")
			return
		case <-ticker.C:
		}
	}
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
		return Job{}, false, fmt.Errorf(
			"claim job: %w", err,
		)
	}

	return job, true, nil
}

// processOne claims and processes at most one job.
func processOne(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		databaseTimeout,
	)

	job, found, err := claimNextJob(ctx, pool)
	cancel()

	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	log.Printf(
		"claimed job %s (profile=%s, attempt=%d)",
		job.ID,
		job.Profile,
		job.Attempts,
	)

	// Execute FFmpeg outside the database transaction.
	outputPath, runErr := executeJob(job)

	// Persist the execution result.
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
			return fmt.Errorf(
				"job %s failed; could not save failure: %w",
				job.ID,
				err,
			)
		}

		log.Printf("job %s failed: %v", job.ID, runErr)
		return nil
	}

	if err := markCompleted(
		updateCtx,
		pool,
		job.ID,
		outputPath,
	); err != nil {
		return fmt.Errorf(
			"job %s completed but DB update failed: %w",
			job.ID,
			err,
		)
	}

	log.Printf(
		"job %s completed: %s",
		job.ID,
		outputPath,
	)
	return nil
}

// executeJob invokes the reusable FFmpeg executor.
func executeJob(job Job) (string, error) {
	// MVP restriction: only direct files inside input/.
	inputPath := filepath.Clean(job.InputPath)

	if filepath.IsAbs(inputPath) ||
		filepath.Dir(inputPath) != "input" {
		return "", errors.New(
			"input must be a file inside input/",
		)
	}

	// Reject directories and symbolic links.
	info, err := os.Lstat(inputPath)
	if err != nil {
		return "", fmt.Errorf("input file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New(
			"input must be a regular file",
		)
	}

	// Use a unique output name for each job.
	finalPath := filepath.Join(
		"output",
		job.ID+".mp4",
	)
	tempPath := filepath.Join(
		"output",
		job.ID+".partial.mp4",
	)

	if err := os.MkdirAll("output", 0755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}

	// Do not overwrite an existing final result.
	if _, err := os.Stat(finalPath); err == nil {
		return "", fmt.Errorf(
			"final output already exists: %s", finalPath,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("check final output: %w", err)
	}

	// Remove a leftover partial output from an earlier crash.
	if err := os.Remove(tempPath); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("remove partial output: %w", err)
	}

	defer os.Remove(tempPath)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		jobTimeout,
	)
	defer cancel()

	_, err = transcode.Run(
		ctx,
		inputPath,
		tempPath,
		transcode.Profile(job.Profile),
	)
	if err != nil {
		return "", fmt.Errorf("transcode job: %w", err)
	}

	// Publish the completed output.
	if err := os.Rename(tempPath, finalPath); err != nil {
		return "", fmt.Errorf("publish output: %w", err)
	}

	return finalPath, nil
}

// markCompleted records a successful execution.
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

// markFailed records a failed execution.
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
