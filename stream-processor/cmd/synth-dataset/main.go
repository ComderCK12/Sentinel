// Command synth-dataset generates a synthetic, labeled training dataset
// for Phase 3 model training, via synth.Generate — see that package's
// doc comment for why this exists instead of using real decision
// history. One-shot batch job, not a long-running server, same shape as
// cmd/offline-replay.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/ComderCK12/Sentinel/stream-processor/internal/synth"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	numUsers := flag.Int("users", 2000, "number of synthetic users to generate")
	minEvents := flag.Int("min-events", 10, "minimum events per user")
	maxEvents := flag.Int("max-events", 40, "maximum events per user")
	fraudRate := flag.Float64("fraud-rate", 0.15, "fraction of users who get one injected fraud episode")
	seed := flag.Int64("seed", 42, "random seed, for reproducible datasets")
	outPath := flag.String("out", "synthetic_dataset.csv", "output CSV path")
	flag.Parse()

	cfg := synth.Config{
		NumUsers:         *numUsers,
		MinEventsPerUser: *minEvents,
		MaxEventsPerUser: *maxEvents,
		FraudUserRate:    *fraudRate,
		Seed:             *seed,
	}

	rows := synth.Generate(cfg)
	logger.Info("generated synthetic dataset", "rows", len(rows), "users", *numUsers, "seed", *seed)

	fraudCount := 0
	for _, r := range rows {
		if r.IsFraud {
			fraudCount++
		}
	}
	logger.Info("label distribution", "fraud_rows", fraudCount, "normal_rows", len(rows)-fraudCount)

	if err := writeCSV(*outPath, rows); err != nil {
		logger.Error("failed to write dataset", "error", err)
		os.Exit(1)
	}
	logger.Info("wrote dataset", "path", *outPath, "rows", len(rows))
}

var csvHeader = []string{
	"event_id",
	"user_id",
	"timestamp",
	"amount",
	"is_fraud",
	"fraud_pattern",
	"velocity_count",
	"amount_ewma",
	"has_amount_baseline",
	"amount_deviation_ratio",
	"time_since_last_seconds",
	"has_time_since_last",
	"distance_from_last_km",
	"has_distance",
}

func writeCSV(path string, rows []synth.LabeledRow) error {
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
			r.Timestamp.Format("2006-01-02T15:04:05.000000Z07:00"),
			strconv.FormatFloat(r.Amount, 'f', -1, 64),
			strconv.FormatBool(r.IsFraud),
			r.FraudPattern,
			strconv.Itoa(r.VelocityCount),
			strconv.FormatFloat(r.AmountEWMA, 'f', -1, 64),
			strconv.FormatBool(r.HasAmountBaseline),
			strconv.FormatFloat(r.AmountDeviationRatio, 'f', -1, 64),
			strconv.FormatFloat(r.TimeSinceLastSeconds, 'f', -1, 64),
			strconv.FormatBool(r.HasTimeSinceLast),
			strconv.FormatFloat(r.DistanceFromLastKM, 'f', -1, 64),
			strconv.FormatBool(r.HasDistance),
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("write row %s: %w", r.EventID, err)
		}
	}

	return w.Error()
}
