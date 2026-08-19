// handleMessageBrowse — GET /v1/messages (право support:trace, тот же
// класс права, что у /v1/support/messages/search — оба читают
// messaging.message_read_model кросс-партнёрски). В отличие от
// /v1/support/messages/search, id не требуется — список последних
// сообщений с пагинацией/фильтрами, тот же паттерн, что /v1/dlq и
// /v1/reconciliation.
package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

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
