package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/ComderCK12/Sentinel/ingestion/internal/idempotency"
	"github.com/ComderCK12/Sentinel/ingestion/internal/producer"
	"github.com/ComderCK12/Sentinel/shared"
)

// idempotencyCheckTimeout bounds the Redis fast-path check independently of
// the caller's own context. Without this, a hung (not just down) Redis
// blocks the request for as long as the client is willing to wait, which
// defeats "fail open" — a dependency that's merely slow should degrade
// ingestion's latency by a bounded amount, not by however long it takes the
// caller to give up.
const idempotencyCheckTimeout = 300 * time.Millisecond

type EventHandler struct {
	producer    *producer.Producer
	idempotency *idempotency.Checker
	logger      *slog.Logger
}

func NewEventHandler(p *producer.Producer, idem *idempotency.Checker, logger *slog.Logger) *EventHandler {
	return &EventHandler{producer: p, idempotency: idem, logger: logger}
}

func (h *EventHandler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	var event shared.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid Json body")
		return
	}

	if err := event.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Redis is a fast-path dedup check, not the source of truth — if it's
	// down, we fail open and publish anyway. decision-service's Postgres
	// unique constraint on event_id is what actually guarantees no
	// duplicate decision ever gets recorded; this just avoids the wasted
	// Kafka publish + consumer round trip for the common case.
	idemCtx, cancel := context.WithTimeout(r.Context(), idempotencyCheckTimeout)
	isNew, err := h.idempotency.MarkIfNew(idemCtx, event.EventID)
	cancel()
	if err != nil {
		h.logger.Error("idempotency check failed, publishing anyway", "event_id", event.EventID, "error", err)
	} else if !isNew {
		h.logger.Info("duplicate event, skipping publish", "event_id", event.EventID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"event_id": event.EventID,
			"status":   "duplicate",
		})
		return
	}

	if err := h.producer.Publish(r.Context(), &event); err != nil {
		h.logger.Error("failed to publish event", "event_id", event.EventID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to accept event")
		return
	}

	h.logger.Info("event accepted", "event_id", event.EventID, "user_id", event.UserID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"event_id": event.EventID,
		"status":   "accepted",
	}); err != nil {
		return
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(map[string]string{"error": message})
	if err != nil {
		return
	}
}
