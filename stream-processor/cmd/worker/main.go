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

	"github.com/ComderCK12/Sentinel/stream-processor/internal/consumer"
	"github.com/ComderCK12/Sentinel/stream-processor/internal/store"
	"github.com/redis/go-redis/v9"
)

const (
	serviceName = "stream-processor"
	defaultPort = "8084"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	port := getEnv("PORT", defaultPort)
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:19092"), ",")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Plain defaults here, deliberately not idempotency.CheckTimeout's tight
	// fail-fast config: nothing on this path is blocking an HTTP response,
	// and unlike ingestion's fast-path check, Redis is the only copy of
	// this state (see store package doc) — a slow Redis should be waited
	// out with go-redis's normal retries, not bailed on quickly.
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	st := store.New(redisClient, store.TTL)

	cons := consumer.New(brokers, st, logger)
	defer cons.Close()

	go cons.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)

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

	<-ctx.Done()
	logger.Info("shutdown signal received", "service", serviceName)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}

	logger.Info("shutdown complete", "service", serviceName)
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
		"status":  "ok",
		"service": serviceName,
	})
}
