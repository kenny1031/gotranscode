package transcode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Profile identifies a supported transcoding profile.
type Profile string

const (
	Profile720p Profile = "720p"
	Profile480p Profile = "480p"
)

// Result describes a successful transcoding execution.
type Result struct {
	OutputPath string
	Duration   time.Duration
}

// Run executes a single FFmpeg transcoding task.
// It blocks until FFmpeg exits or the context is cancelled.
func Run(
	ctx context.Context,
	inputPath string,
	outputPath string,
	profile Profile,
) (Result, error) {
	var scale string

	switch profile {
	case Profile720p:
		scale = "scale=-2:720"
	case Profile480p:
		scale = "scale=-2:480"
	default:
		return Result{}, fmt.Errorf(
			"unsupported transcoding profile: %q",
			profile,
		)
	}

	// Validate input and output paths.
	if inputPath == "" || outputPath == "" {
		return Result{}, errors.New(
			"input and output paths are required",
		)
	}

	inputAbs, err := filepath.Abs(inputPath)
	if err != nil {
		return Result{}, fmt.Errorf(
			"resolve input path: %w", err,
		)
	}

	outputAbs, err := filepath.Abs(outputPath)
	if err != nil {
		return Result{}, fmt.Errorf(
			"resolve output path: %w", err,
		)
	}

	if inputAbs == outputAbs {
		return Result{}, errors.New(
			"input and output paths must differ",
		)
	}

	info, err := os.Stat(inputPath)
	if err != nil {
		return Result{}, fmt.Errorf(
			"stat input file: %w", err,
		)
	}

	if !info.Mode().IsRegular() {
		return Result{}, errors.New(
			"input must be a regular file",
		)
	}

	// Create the output directory if needed.
	if err := os.MkdirAll(
		filepath.Dir(outputPath),
		0755,
	); err != nil {
		return Result{}, fmt.Errorf(
			"create output directory: %w", err,
		)
	}

	// Refuse to overwrite existing output files.
	if _, err := os.Stat(outputPath); err == nil {
		return Result{}, fmt.Errorf(
			"output already exists: %s", outputPath,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Result{}, fmt.Errorf(
			"check output path: %w", err,
		)
	}

	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-nostdin",
		"-n",

		"-i", inputPath,

		"-map", "0:v:0",
		"-map", "0:a?",

		"-vf", scale,
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", "23",
		"-pix_fmt", "yuv420p",
		"-c:a", "aac",
		"-movflags", "+faststart",

		outputPath,
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	// Capture FFmpeg error messages.
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = io.Discard

	start := time.Now()

	// Start the child process and wait for completion.
	err = cmd.Run()
	duration := time.Since(start)

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Result{}, fmt.Errorf(
				"transcoding cancelled: %w", ctxErr,
			)
		}

		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return Result{}, fmt.Errorf(
				"ffmpeg exited with code %d: %s",
				exitErr.ExitCode(),
				stderr.String(),
			)
		}

		return Result{}, fmt.Errorf(
			"execute ffmpeg: %w", err,
		)
	}

	return Result{
		OutputPath: outputPath,
		Duration:   duration,
	}, nil
}
