package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ComderCK12/Sentinel/ingestion/internal/api"
	"github.com/ComderCK12/Sentinel/ingestion/internal/idempotency"
	"github.com/ComderCK12/Sentinel/ingestion/internal/producer"
	"github.com/redis/go-redis/v9"
)

const (
	serviceName    = "ingestion"
	defaultPort    = "8080"
	idempotencyTTL = 24 * time.Hour
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := getEnv("PORT", defaultPort)
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:19092"), ",")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	prod := producer.New(brokers)
	defer prod.Close()

	// Short, no-retry timeouts: this client only ever serves the idempotency
	// fast-path check, which is meant to fail open quickly if Redis is slow
	// or unreachable. A plain context.WithTimeout on the call site isn't
	// enough on its own — go-redis binds its blocking socket I/O to these
	// client-level timeouts (several seconds by default) rather than to a
	// deadline passed into an individual command, and its default retry
	// count multiplies that further.
	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  idempotency.CheckTimeout,
		ReadTimeout:  idempotency.CheckTimeout,
		WriteTimeout: idempotency.CheckTimeout,
		MaxRetries:   -1,
	})
	defer redisClient.Close()
	idem := idempotency.New(redisClient, idempotencyTTL)

	eventHandler := api.NewEventHandler(prod, idem, logger)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/v1/events", eventHandler.HandleEvent)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("starting server", "service", serviceName, "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	waitForShutdown(srv, logger)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":      "ok",
		"serviceName": serviceName,
	})
}

func waitForShutdown(srv *http.Server, logger *slog.Logger) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutdown signal received", "service", serviceName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}

	logger.Info("shutdown complete", "service", serviceName)
}
