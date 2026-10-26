// Command offline-replay recomputes features.Features for every row in
// the decisions table, reusing the same replay.Run (and, underneath it,
// features.Compute) that the live stream-processor path uses — so the
// training dataset this produces can't silently drift from the online
// feature logic just by having been written twice.
//
// It exists as a second binary in this module, rather than a standalone
// tool elsewhere, specifically so it can import stream-processor's
// internal packages (features, replay) directly.
package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/ComderCK12/Sentinel/stream-processor/internal/replay"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dsn := getEnv("POSTGRES_DSN", "postgres://sentinel:sentinel@localhost:5435/sentinel")
	outPath := getEnv("OUTPUT_PATH", "features_dataset.csv")

	ctx := context.Background()

	if err := run(ctx, dsn, outPath, logger); err != nil {
		logger.Error("offline replay failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn, outPath string, logger *slog.Logger) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()

	rows, err := loadRows(ctx, pool)
	if err != nil {
		return fmt.Errorf("load rows: %w", err)
	}
	logger.Info("loaded decisions", "count", len(rows))

	out := replay.Run(rows)
	logger.Info("computed features", "count", len(out))

	if err := writeCSV(outPath, out); err != nil {
		return fmt.Errorf("write csv: %w", err)
	}
	logger.Info("wrote dataset", "path", outPath, "rows", len(out))

	return nil
}

// loadRows reads every decision ordered by user then time. The ordering
// is a performance nicety, not a correctness requirement — replay.Run
// sorts defensively — but it means Postgres does the sort instead of Go
// re-sorting an already-sorted slice.
func loadRows(ctx context.Context, pool *pgxpool.Pool) ([]replay.Row, error) {
	dbRows, err := pool.Query(ctx, `
		SELECT event_id, user_id, amount, currency, decision, reason, created_at
		FROM decisions
		ORDER BY user_id, created_at
	`)
	if err != nil {
		return nil, fmt.Errorf("query decisions: %w", err)
	}
	defer dbRows.Close()

	var rows []replay.Row
	for dbRows.Next() {
		var r replay.Row
		if err := dbRows.Scan(&r.EventID, &r.UserID, &r.Amount, &r.Currency, &r.Decision, &r.Reason, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan decision row: %w", err)
		}
		rows = append(rows, r)
	}
	if err := dbRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate decision rows: %w", err)
	}

	return rows, nil
}

var csvHeader = []string{
	"event_id",
	"user_id",
	"amount",
	"currency",
	"decision",
	"reason",
	"velocity_count",
	"amount_ewma",
	"has_amount_baseline",
	"amount_deviation_ratio",
	"time_since_last_seconds",
	"has_time_since_last",
}

func writeCSV(path string, rows []replay.OutputRow) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(csvHeader); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, r := range rows {
		record := []string{
			r.EventID,
			r.UserID,
			strconv.FormatFloat(r.Amount, 'f', -1, 64),
			r.Currency,
			r.Decision,
			r.Reason,
			strconv.Itoa(r.VelocityCount),
			strconv.FormatFloat(r.AmountEWMA, 'f', -1, 64),
			strconv.FormatBool(r.HasAmountBaseline),
			strconv.FormatFloat(r.AmountDeviationRatio, 'f', -1, 64),
			strconv.FormatFloat(r.TimeSinceLastSeconds, 'f', -1, 64),
			strconv.FormatBool(r.HasTimeSinceLast),
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("write row %s: %w", r.EventID, err)
		}
	}

	return w.Error()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
