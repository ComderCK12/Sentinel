package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ComderCK12/Sentinel/shared"

	"github.com/segmentio/kafka-go"
)

const Topic = "transactions"

type Producer struct {
	writer *kafka.Writer
}

func New(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        Topic,
			Balancer:     &kafka.Hash{}, // hash on key -> same user_id always lands on the same partition
			RequiredAcks: kafka.RequireAll,
			Async:        false, // synchronous — we want publish confirmed before ack'ing the HTTP request
		},
	}
}

// Publish sends the event to Kafka, keyed by user ID so all of a user's
// events land on the same partition and preserve relative order — this is
// what makes the stream-processor's per-user rolling features (Phase 2)
// correct rather than a race.
func (p *Producer) Publish(ctx context.Context, e *shared.Event) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(e.UserID),
		Value: payload,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish to kafka: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
