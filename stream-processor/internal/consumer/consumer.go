package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/ComderCK12/Sentinel/shared"
	"github.com/ComderCK12/Sentinel/stream-processor/internal/features"
	"github.com/ComderCK12/Sentinel/stream-processor/internal/store"
	"github.com/segmentio/kafka-go"
)

const (
	Topic   = "transactions"
	GroupID = "stream-processor" // distinct group — independent offsets from decision-service
)

type Consumer struct {
	reader *kafka.Reader
	store  *store.Store
	logger *slog.Logger
}

func New(brokers []string, st *store.Store, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   Topic,
		GroupID: GroupID,
	})
	return &Consumer{reader: reader, store: st, logger: logger}
}

// Run blocks, consuming until ctx is cancelled. Like decision-service's
// consumer, the offset is only committed after the state write succeeds —
// but unlike decision-service (where SaveDecision's ON CONFLICT DO NOTHING
// makes redelivery harmless), a Redis failure here means we simply retry
// on redelivery rather than risk silently losing a feature update. Redis
// is the only copy of this state; there's no backstop to fall back on.
func (c *Consumer) Run(ctx context.Context) {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("consumer stopping")
				return
			}
			c.logger.Error("fetch message failed", "error", err)
			continue
		}

		var event shared.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			c.logger.Error("failed to unmarshal event, skipping", "error", err)
			// Commit anyway — a malformed message will never parse
			// successfully no matter how many times we redeliver it.
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		prevState, err := c.store.Get(ctx, event.UserID)
		if err != nil {
			c.logger.Error("failed to load state, will retry", "user_id", event.UserID, "error", err)
			// Deliberately do NOT commit — the next FetchMessage call
			// redelivers this same message.
			continue
		}

		f, nextState := features.Compute(prevState, &event)

		if err := c.store.Save(ctx, event.UserID, nextState); err != nil {
			c.logger.Error("failed to save state, will retry", "user_id", event.UserID, "error", err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit offset", "error", err)
		}

		c.logger.Info("features computed",
			"event_id", event.EventID,
			"user_id", event.UserID,
			"velocity_count", f.VelocityCount,
			"amount_deviation_ratio", f.AmountDeviationRatio,
			"time_since_last", f.TimeSinceLast,
			"distance_from_last_km", f.DistanceFromLastKM,
		)
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
