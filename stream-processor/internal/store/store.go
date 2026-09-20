package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ComderCK12/Sentinel/stream-processor/internal/features"
	"github.com/redis/go-redis/v9"
)

const keyPrefix = "sentinel:features:state:"
const TTL = 24 * time.Hour

type Store struct {
	client *redis.Client
	ttl    time.Duration
}

func New(client *redis.Client, ttl time.Duration) *Store {
	return &Store{client: client, ttl: ttl}
}

func (s *Store) Get(ctx context.Context, userID string) (features.State, error) {
	raw, err := s.client.Get(ctx, keyPrefix+userID).Bytes()
	if errors.Is(err, redis.Nil) {
		return features.State{}, nil
	}

	if err != nil {
		return features.State{}, fmt.Errorf("get state: %w", err)
	}

	var st features.State
	if err := json.Unmarshal(raw, &st); err != nil {
		return features.State{}, fmt.Errorf("unmarshal state: %w", err)
	}

	return st, nil
}

func (s *Store) Save(ctx context.Context, userID string, st features.State) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	if err := s.client.Set(ctx, keyPrefix+userID, raw, s.ttl).Err(); err != nil {
		return fmt.Errorf("save state: %w", err)
	}

	return nil
}
