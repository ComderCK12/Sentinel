package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "sentinel:ingestion:event:"

type Checker struct {
	client *redis.Client
	ttl    time.Duration
}

func New(client *redis.Client, ttl time.Duration) *Checker {
	return &Checker{client: client, ttl: ttl}
}

// MarkIfNew atomically records event_id as seen and reports whether this
// is the first time it's been seen. SETNX is atomic, so two concurrent
// requests carrying the same event_id can never both get isNew == true —
// this is what makes the check race-safe, not just "check then set".
func (c *Checker) MarkIfNew(ctx context.Context, eventID string) (isNew bool, err error) {
	ok, err := c.client.SetNX(ctx, keyPrefix+eventID, 1, c.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency check: %w", err)
	}
	return ok, nil
}
