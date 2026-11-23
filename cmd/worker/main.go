package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pollInterval      = 2 * time.Second
	recoveryInterval  = 5 * time.Second
	heartbeatInterval = 10 * time.Second

	databaseTimeout = 5 * time.Second
	jobTimeout      = 2 * time.Minute

	leaseSeconds = 45
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	workerCount := 2

	if raw := os.Getenv("WORKER_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 8 {
			log.Fatal(
				"WORKER_CONCURRENCY must be between 1 and 8",
			)
		}
		workerCount = n
	}

	// Set a bounded connection pool size.
	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Fatalf("parse database configuration: %v", err)
	}

	config.MaxConns = int32(workerCount*3 + 2)

	startupCtx, cancel := context.WithTimeout(
		context.Background(),
		databaseTimeout,
	)

	pool, err := pgxpool.NewWithConfig(startupCtx, config)
	if err != nil {
		cancel()
		log.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(startupCtx); err != nil {
		cancel()
		log.Fatalf("connect to database: %v", err)
	}
	cancel()

	store := NewStore(pool)

	// SIGINT / SIGTERM stops new job claims.
	stopCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	var wg sync.WaitGroup

	log.Printf(
		"GoTranscode worker starting: concurrency=%d",
		workerCount,
	)

	// Start the recovery goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		recoveryLoop(stopCtx, store)
	}()

	// Start the fixed-size worker pool.
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()
			workerLoop(stopCtx, store, workerID)
		}(i)
	}

	// Wait for shutdown signal.
	<-stopCtx.Done()

	log.Println("shutdown requested: stop claiming jobs")

	// Restore default signal handling.
	stop()

	// Existing executions finish independently.
	wg.Wait()

	log.Println("all workers stopped")
}

// workerLoop repeatedly claims and executes jobs.
func workerLoop(
	stopCtx context.Context,
	store *Store,
	workerID int,
) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	log.Printf("[worker-%d] started", workerID)

	for {
		if stopCtx.Err() != nil {
			break
		}

		processed, err := processOne(
			stopCtx,
			store,
			workerID,
		)

		if err != nil {
			if stopCtx.Err() == nil {
				log.Printf(
					"[worker-%d] error: %v",
					workerID,
					err,
				)
			}
		} else if processed {
			// Check for more work immediately.
			continue
		}

		select {
		case <-stopCtx.Done():
			log.Printf("[worker-%d] stopped", workerID)
			return
		case <-ticker.C:
		}
	}

	log.Printf("[worker-%d] stopped", workerID)
}

// processOne claims and runs at most one job.
func processOne(
	stopCtx context.Context,
	store *Store,
	workerID int,
) (bool, error) {
	claimCtx, claimCancel := dbContext(stopCtx)

	job, found, err := store.claimNextJob(claimCtx)
	claimCancel()

	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}

	log.Printf(
		"[worker-%d] claimed job=%s attempt=%d/%d",
		workerID,
		job.ID,
		job.Attempts,
		job.MaxAttempts,
	)

	// Already-claimed work does not inherit shutdown
	// cancellation, but has its own execution timeout.
	jobCtx, jobCancel := context.WithTimeout(
		context.Background(),
		jobTimeout,
	)
	defer jobCancel()

	// Run heartbeat concurrently with FFmpeg.
	heartbeatCtx, stopHeartbeat := context.WithCancel(
		context.Background(),
	)

	heartbeatDone := make(chan struct{})

	go func() {
		defer close(heartbeatDone)

		heartbeatLoop(
			heartbeatCtx,
			store,
			job,
			jobCancel,
		)
	}()

	// Stop and join the heartbeat before returning.
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
	}()

	// Execute FFmpeg for this specific attempt.
	outputPath, runErr := executeJob(jobCtx, job)

	// Persist the result independently of shutdown.
	updateCtx, updateCancel := dbContext(
		context.Background(),
	)
	defer updateCancel()

	if runErr != nil {
		status, err := store.failOrRetry(
			updateCtx,
			job,
			runErr.Error(),
		)

		if err != nil {
			return true, fmtError(
				"persist execution failure",
				job.ID,
				err,
			)
		}

		log.Printf(
			"[worker-%d] job=%s error=%v next_status=%s",
			workerID,
			job.ID,
			runErr,
			status,
		)
		return true, nil
	}

	if err := store.finishJob(
		updateCtx,
		job,
		outputPath,
	); err != nil {
		return true, fmtError(
			"persist completion",
			job.ID,
			err,
		)
	}

	log.Printf(
		"[worker-%d] completed job=%s output=%s",
		workerID,
		job.ID,
		outputPath,
	)

	return true, nil
}

// recoveryLoop periodically detects expired leases.
func recoveryLoop(
	ctx context.Context,
	store *Store,
) {
	ticker := time.NewTicker(recoveryInterval)
	defer ticker.Stop()

	log.Println("[recovery] started")

	for {
		if ctx.Err() != nil {
			return
		}

		dbCtx, cancel := dbContext(ctx)
		err := store.recoverExpiredJobs(dbCtx)
		cancel()

		if err != nil && ctx.Err() == nil {
			log.Printf("[recovery] error: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("[recovery] stopped")
			return
		case <-ticker.C:
		}
	}
}

// fmtError adds context to persistence failures.
func fmtError(
	operation string,
	jobID string,
	err error,
) error {
	return &workerError{
		operation: operation,
		jobID:     jobID,
		cause:     err,
	}
}

type workerError struct {
	operation string
	jobID     string
	cause     error
}

func (e *workerError) Error() string {
	return e.operation + " for job " +
		e.jobID + ": " + e.cause.Error()
}

func (e *workerError) Unwrap() error {
	return e.cause
}
