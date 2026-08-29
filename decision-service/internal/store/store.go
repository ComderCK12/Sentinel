package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

type Decision struct {
	EventID  string
	UserID   string
	Amount   float64
	Currency string
	Decision string
	Reason   string
}

// SaveDecision persists a decision. ON CONFLICT DO NOTHING is what makes
// this write idempotent — if the consumer redelivers the same event after
// a crash (see consumer.Run), writing the same decision twice is a no-op,
// not a duplicate row.
func (s *Store) SaveDecision(ctx context.Context, d Decision) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO decisions (event_id, user_id, amount, currency, decision, reason)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING
	`, d.EventID, d.UserID, d.Amount, d.Currency, d.Decision, d.Reason)
	if err != nil {
		return fmt.Errorf("insert decision: %w", err)
	}
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
}
