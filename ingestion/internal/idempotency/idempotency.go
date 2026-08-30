package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "sentinel:ingestion:event:"

// CheckTimeout bounds both the Redis client's own dial/read/write timeouts
// (set on the client passed to New — see ingestion/cmd/server/main.go) and
// the per-call context callers should apply to MarkIfNew/Release. Sharing
// one constant across both keeps them from drifting apart: a context
// deadline alone doesn't bound go-redis's underlying blocking socket I/O,
// so the client-level timeouts have to actually be this value too, not
// just some other "short-sounding" number.
const CheckTimeout = 300 * time.Millisecond

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
//
// A true result is a claim, not a completed fact — the caller still has to
// actually do the work (publish to Kafka) before the event is really
// "processed". If that work fails, call Release so a retry with the same
// event_id isn't silently told "duplicate" for a duplicate that never
// actually happened.
func (c *Checker) MarkIfNew(ctx context.Context, eventID string) (isNew bool, err error) {
	ok, err := c.client.SetNX(ctx, keyPrefix+eventID, 1, c.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency check: %w", err)
	}
	return ok, nil
}

// Release undoes a claim made by MarkIfNew, for when the work that claim
// was guarding (the Kafka publish) failed. Best-effort: if this itself
// fails, the claim just lives until its TTL expires, which only costs a
// spurious "duplicate" response on a client retry within that TTL — an
// availability hit, not a correctness one, unlike the original bug where
// this had no way of being called at all.
func (c *Checker) Release(ctx context.Context, eventID string) error {
	if err := c.client.Del(ctx, keyPrefix+eventID).Err(); err != nil {
		return fmt.Errorf("idempotency release: %w", err)
	}
	return nil
}
