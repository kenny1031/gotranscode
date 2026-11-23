package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kenny1031/gotranscode/internal/transcode"
)

// executeJob runs FFmpeg for a specific execution attempt.
// Each attempt has its own uniquely named output.
func executeJob(
	ctx context.Context,
	job Job,
) (string, error) {
	inputPath := filepath.Clean(job.InputPath)

	// MVP: only permit direct files inside input/.
	if filepath.IsAbs(inputPath) ||
		filepath.Dir(inputPath) != "input" {
		return "", errors.New(
			"input must be a direct file inside input/",
		)
	}

	info, err := os.Lstat(inputPath)
	if err != nil {
		return "", fmt.Errorf("input file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New(
			"input must be a regular file",
		)
	}

	// Create one output directory per job.
	outputDir := filepath.Join("output", job.ID)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf(
			"create output directory: %w", err,
		)
	}

	// Fencing token prevents different attempts
	// from using the same output filename.
	finalPath := filepath.Join(
		outputDir,
		job.LeaseToken+".mp4",
	)
	tempPath := filepath.Join(
		outputDir,
		job.LeaseToken+".partial.mp4",
	)

	// Never overwrite an already published result.
	if _, err := os.Stat(finalPath); err == nil {
		return "", fmt.Errorf(
			"output already exists: %s", finalPath,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf(
			"check output path: %w", err,
		)
	}

	defer os.Remove(tempPath)

	// Reuse our Step 5 FFmpeg executor.
	_, err = transcode.Run(
		ctx,
		inputPath,
		tempPath,
		transcode.Profile(job.Profile),
	)
	if err != nil {
		return "", fmt.Errorf("transcoding: %w", err)
	}

	// If the lease was lost during execution,
	// avoid intentionally publishing the result.
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		return "", fmt.Errorf(
			"publish output: %w", err,
		)
	}

	return finalPath, nil
}

// heartbeatLoop periodically renews the job lease.
// If renewal fails, the executor is cancelled.
func heartbeatLoop(
	ctx context.Context,
	store *Store,
	job Job,
	cancelExecution context.CancelFunc,
) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			dbCtx, cancel := dbContext(ctx)
			err := store.renewLease(dbCtx, job)
			cancel()

			if ctx.Err() != nil {
				return
			}

			if err != nil {
				log.Printf(
					"[heartbeat] job=%s lease renewal failed: %v",
					job.ID,
					err,
				)

				// Stop FFmpeg if ownership cannot
				// be maintained.
				cancelExecution()
				return
			}
		}
	}
}
