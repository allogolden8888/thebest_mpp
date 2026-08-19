// handleMessageBrowse — GET /v1/messages (право support:trace, тот же
// класс права, что у /v1/support/messages/search — оба читают
// messaging.message_read_model кросс-партнёрски). В отличие от
// /v1/support/messages/search, id не требуется — список последних
// сообщений с пагинацией/фильтрами, тот же паттерн, что /v1/dlq и
// /v1/reconciliation.
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"mpp/backoffice-api/internal/store"
)

func handleMessageBrowse(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := store.MessageBrowseFilter{
			PartnerID:     q.Get("partner_id"),
			CurrentStatus: q.Get("current_status"),
		}
		if terminal := q.Get("terminal"); terminal != "" {
			b, err := strconv.ParseBool(terminal)
			if err != nil {
				http.Error(w, "неверный terminal: ожидалось true/false", http.StatusBadRequest)
				return
			}
			filter.Terminal = &b
		}
		if limit := q.Get("limit"); limit != "" {
			n, err := strconv.Atoi(limit)
			if err != nil {
				http.Error(w, "неверный limit", http.StatusBadRequest)
				return
			}
			filter.Limit = n
		}
		if offset := q.Get("offset"); offset != "" {
			n, ok := parseNonNegativeInt(offset)
			if !ok {
				http.Error(w, "неверный offset: ожидалось неотрицательное целое", http.StatusBadRequest)
				return
			}
			filter.Offset = n
		}

		results, err := pg.MessageBrowse(r.Context(), filter)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "message_browse: ошибка чтения из PostgreSQL", err)
			return
		}

		messages := make([]supportMessageResponse, 0, len(results))
		for _, m := range results {
			messages = append(messages, toSupportMessageResponse(m))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Messages []supportMessageResponse `json:"messages"`
		}{Messages: messages})
	}
}

type lifecycleEventResponse struct {
	LifecycleVersion int64  `json:"lifecycle_version"`
	Status           string `json:"status"`
	EventID          string `json:"event_id"`
	OccurredAt       string `json:"occurred_at"`
	Source           string `json:"source"`
}

// handleMessageDetail — GET /v1/messages/{message_id}: read-model строка
// + полная лента messaging.message_lifecycle_history, ORDER BY
// lifecycle_version ASC (см. store.GetMessageDetail doc-комментарий за
// тем, чего в этом таймлайне сознательно нет).
func handleMessageDetail(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		messageID := chi.URLParam(r, "message_id")
		msg, history, err := pg.GetMessageDetail(r.Context(), messageID)
		if errors.Is(err, store.ErrMessageNotFound) {
			http.Error(w, "сообщение не найдено", http.StatusNotFound)
			return
		}
		if err != nil {
			internalError(w, http.StatusInternalServerError, "message_detail: ошибка чтения из PostgreSQL", err)
			return
		}

		historyResp := make([]lifecycleEventResponse, 0, len(history))
		for _, e := range history {
			historyResp = append(historyResp, lifecycleEventResponse{
				LifecycleVersion: e.LifecycleVersion,
				Status:           e.Status,
				EventID:          e.EventID,
				OccurredAt:       e.OccurredAt.Format("2006-01-02T15:04:05.000Z07:00"),
				Source:           e.Source,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Message supportMessageResponse   `json:"message"`
			History []lifecycleEventResponse `json:"history"`
		}{Message: toSupportMessageResponse(msg), History: historyResp})
	}
}
