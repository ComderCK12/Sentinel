package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/ComderCK12/Sentinel/ingestion/internal/producer"
	"github.com/ComderCK12/Sentinel/shared"
)

type EventHandler struct {
	producer *producer.Producer
	logger   *slog.Logger
}

func NewEventHandler(p *producer.Producer, logger *slog.Logger) *EventHandler {
	return &EventHandler{producer: p, logger: logger}
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

	if err := h.producer.Publish(r.Context(), &event); err != nil {
		h.logger.Error("failed to publish event", "event_id", event.EventID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to accept event")
		return
	}

	h.logger.Info("event accepted", "event_id", event.EventID, "user_id", event.UserID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	err := json.NewEncoder(w).Encode(map[string]string{
		"event_id": event.EventID,
		"status":   "accepted",
	})
	if err != nil {
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
