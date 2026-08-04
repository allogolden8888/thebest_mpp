package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"mpp/partner-api/internal/auth"
	"mpp/partner-api/internal/store"
)

type messageStatusResponse struct {
	MessageID       string           `json:"message_id"`
	ApplicationID   string           `json:"application_id"`
	TraceID         string           `json:"trace_id"`
	PipelineID      string           `json:"pipeline_id"`
	PipelineVersion string           `json:"pipeline_version"`
	CurrentStatus   string           `json:"current_status"`
	Terminal        bool             `json:"terminal"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
	History         []lifecycleEvent `json:"history,omitempty"`
}

type lifecycleEvent struct {
	LifecycleVersion int64     `json:"lifecycle_version"`
	Status           string    `json:"status"`
	EventID          string    `json:"event_id"`
	OccurredAt       time.Time `json:"occurred_at"`
	Source           string    `json:"source"`
}

// handle_status_query — service_internal_methods.md §7.2: message_id или
// trace_id -> MessageStatusResponse (PostgreSQL read), с необязательной
// историей переходов (?history=1).
func handleStatusQuery(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		messageID := r.URL.Query().Get("message_id")
		traceID := r.URL.Query().Get("trace_id")
		if messageID == "" && traceID == "" {
			http.Error(w, "требуется message_id или trace_id", http.StatusBadRequest)
			return
		}

		var (
			status store.MessageStatus
			err    error
		)
		if messageID != "" {
			status, err = pg.StatusByMessageID(r.Context(), claims.PartnerID, messageID)
		} else {
			status, err = pg.StatusByTraceID(r.Context(), claims.PartnerID, traceID)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "сообщение не найдено", http.StatusNotFound)
			return
		}
		if err != nil {
			internalError(w, http.StatusInternalServerError, "status_query: ошибка чтения из PostgreSQL", err)
			return
		}

		resp := messageStatusResponse{
			MessageID:       status.MessageID,
			ApplicationID:   status.ApplicationID,
			TraceID:         status.TraceID,
			PipelineID:      status.PipelineID,
			PipelineVersion: status.PipelineVersion,
			CurrentStatus:   status.CurrentStatus,
			Terminal:        status.Terminal,
			CreatedAt:       status.CreatedAt,
			UpdatedAt:       status.UpdatedAt,
		}

		if r.URL.Query().Get("history") == "1" {
			events, err := pg.LifecycleHistory(r.Context(), claims.PartnerID, status.MessageID)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				internalError(w, http.StatusInternalServerError, "status_query_history: ошибка чтения из PostgreSQL", err)
				return
			}
			for _, e := range events {
				resp.History = append(resp.History, lifecycleEvent{
					LifecycleVersion: e.LifecycleVersion,
					Status:           e.Status,
					EventID:          e.EventID,
					OccurredAt:       e.OccurredAt,
					Source:           e.Source,
				})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}