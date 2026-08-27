package shared

import (
	"errors"
	"time"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// Event is the canonical representation of an incoming transaction event.
// Both ingestion (producer) and downstream consumers (stream-processor,
// decision-service) depend on this shape — keep it here, not duplicated,
// so producer and consumers can never drift out of schema sync.
type Event struct {
	EventID   string    `json:"event_id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	Timestamp time.Time `json:"timestamp"`
	Location  *Location `json:"location,omitempty"`
}

var (
	ErrMissingEventID  = errors.New("event_id is required")
	ErrMissingUserID   = errors.New("user_id is required")
	ErrInvalidAmount   = errors.New("amount must be positive")
	ErrMissingCurrency = errors.New("currency is required")
)

func (e *Event) Validate() error {
	if e.EventID == "" {
		return ErrMissingEventID
	}

	if e.UserID == "" {
		return ErrMissingUserID
	}

	if e.Amount <= 0 {
		return ErrInvalidAmount
	}

	if e.Currency == "" {
		return ErrMissingCurrency
	}

	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}

	return nil
}