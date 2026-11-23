package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kenny1031/gotranscode/internal/transcode"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal(
			"usage: go run ./cmd/transcode <input-path> <720p|480p>",
		)
	}

	inputPath := os.Args[1]
	profile := transcode.Profile(os.Args[2])

	// Build the output file path.
	filename := filepath.Base(inputPath)
	extension := filepath.Ext(filename)
	name := strings.TrimSuffix(filename, extension)

	outputPath := filepath.Join(
		"output",
		fmt.Sprintf("%s_%s.mp4", name, profile),
	)

	// Limit each transcoding task to 2 minutes.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Minute,
	)
	defer cancel()

	log.Printf(
		"starting transcoding: input=%s profile=%s",
		inputPath,
		profile,
	)

	result, err := transcode.Run(
		ctx,
		inputPath,
		outputPath,
		profile,
	)
	if err != nil {
		log.Fatalf("transcoding failed: %v", err)
	}

	log.Printf(
		"transcoding completed: output=%s duration=%s",
		result.OutputPath,
		result.Duration,
	)
}
