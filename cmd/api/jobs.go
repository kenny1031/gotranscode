package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// JobStatus represents the state of a transcoding job.
type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusProcessing JobStatus = "processing"
	StatusRetrying   JobStatus = "retrying"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

// CreateJobRequest represents the JSON body of POST /jobs.
type CreateJobRequest struct {
	InputPath string `json:"input_path"`
	Profile   string `json:"profile"`
	Priority  int    `json:"priority"`
}

// Job represents a media transcoding task.
type Job struct {
	ID        string    `json:"id"`
	InputPath string    `json:"input_path"`
	Profile   string    `json:"profile"`
	Priority  int       `json:"priority"`
	Status    JobStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// createJobHandler handles POST /jobs.
func createJobHandler(store *PostgresJobStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateJobRequest

		// Limit request body size to 1 MiB.
		decoder := json.NewDecoder(
			http.MaxBytesReader(w, r.Body, 1<<20),
		)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&req); err != nil {
			http.Error(
				w,
				"invalid JSON request",
				http.StatusBadRequest,
			)
			return
		}

		// Reject multiple JSON values in the body.
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			http.Error(
				w,
				"unexpected extra JSON",
				http.StatusBadRequest,
			)
			return
		}

		// Validate request fields.
		req.InputPath = strings.TrimSpace(req.InputPath)
		req.Profile = strings.TrimSpace(req.Profile)

		if req.InputPath == "" {
			http.Error(
				w,
				"input_path is required",
				http.StatusBadRequest,
			)
			return
		}

		if req.Profile != "720p" && req.Profile != "480p" {
			http.Error(
				w,
				"unsupported profile",
				http.StatusBadRequest,
			)
			return
		}

		if req.Priority < 0 || req.Priority > 10 {
			http.Error(
				w,
				"priority must be between 0 and 10",
				http.StatusBadRequest,
			)
			return
		}

		// Persist the new job in PostgreSQL.
		job, err := store.Create(r.Context(), req)
		if err != nil {
			log.Printf("failed to create job: %v", err)
			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Location", "/jobs/"+job.ID)
		w.WriteHeader(http.StatusCreated)

		if err := json.NewEncoder(w).Encode(job); err != nil {
			log.Printf("failed to encode job response: %v", err)
		}
	}
}

// getJobHandler handles GET /jobs/{id}.
func getJobHandler(store *PostgresJobStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		// Validate UUID before querying PostgreSQL.
		if _, err := uuid.Parse(id); err != nil {
			http.Error(
				w,
				"invalid job ID",
				http.StatusBadRequest,
			)
			return
		}

		job, err := store.Get(r.Context(), id)

		if errors.Is(err, ErrJobNotFound) {
			http.Error(
				w,
				"job not found",
				http.StatusNotFound,
			)
			return
		}

		if err != nil {
			log.Printf("failed to get job: %v", err)
			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(job); err != nil {
			log.Printf("failed to encode job response: %v", err)
		}
	}
}

// listJobsHandler handles GET /jobs.
func listJobsHandler(store *PostgresJobStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobs, err := store.List(r.Context())
		if err != nil {
			log.Printf("failed to list jobs: %v", err)
			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.NewEncoder(w).Encode(jobs); err != nil {
			log.Printf("failed to encode jobs response: %v", err)
		}
	}
}
