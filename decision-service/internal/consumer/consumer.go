package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/ComderCK12/Sentinel/decision-service/internal/rules"
	"github.com/ComderCK12/Sentinel/decision-service/internal/store"
	"github.com/ComderCK12/Sentinel/shared"
	"github.com/segmentio/kafka-go"
)

const (
	Topic   = "transactions"
	GroupID = "decision-service"
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
		GroupID: GroupID, // consumer group -> Kafka tracks our offset per partition
	})
	return &Consumer{reader: reader, store: st, logger: logger}
}

// Run blocks, consuming until ctx is cancelled. The offset is committed only
// AFTER a successful Postgres write — if the process crashes between reading
// and writing, that offset was never committed, so this event gets
// redelivered on restart instead of silently lost. That makes the pipeline
// at-least-once, not exactly-once: SaveDecision's ON CONFLICT DO NOTHING is
// what makes redelivery safe rather than a duplicate decision.
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

		decision, reason := rules.Evaluate(&event)

		err = c.store.SaveDecision(ctx, store.Decision{
			EventID:  event.EventID,
			UserID:   event.UserID,
			Amount:   event.Amount,
			Currency: event.Currency,
			Decision: decision,
			Reason:   reason,
		})
		if err != nil {
			c.logger.Error("failed to save decision, will retry", "event_id", event.EventID, "error", err)
			// Deliberately do NOT commit — the next FetchMessage call
			// redelivers this same message.
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit offset", "error", err)
		}

		c.logger.Info("decision made", "event_id", event.EventID, "decision", decision)
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
