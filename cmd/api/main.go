package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status string `json:"status"`
}

// InfoResponse is returned by GET /info.
type InfoResponse struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Service string `json:"service"`
}

// healthHandler handles GET /health.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status: "ok",
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("failed to encode health response: %v", err)
	}
}

// infoHandler handles GET /info.
func infoHandler(w http.ResponseWriter, r *http.Request) {
	response := InfoResponse{
		Name:    "GoTranscode",
		Version: "0.1.0",
		Service: "api",
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("failed to encode info response: %v", err)
	}
}

func main() {
	// Read database configuration.
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is not set")
	}

	// Set a timeout for initial database connection.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	// Initialise PostgreSQL connection pool.
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("failed to create database pool: %v", err)
	}
	defer pool.Close()

	// Verify PostgreSQL connectivity.
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}

	log.Println("connected to PostgreSQL")

	// Initialise the persistent job store.
	store := NewPostgresJobStore(pool)

	// Create HTTP router.
	mux := http.NewServeMux()

	// Service routes.
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /info", infoHandler)

	// Job routes.
	mux.HandleFunc("POST /jobs", createJobHandler(store))
	mux.HandleFunc("GET /jobs", listJobsHandler(store))
	mux.HandleFunc("GET /jobs/{id}", getJobHandler(store))

	// Configure HTTP server.
	addr := "127.0.0.1:8080"

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("GoTranscode API listening on %s", addr)

	// Start serving HTTP requests.
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("HTTP server stopped: %v", err)
	}
}
