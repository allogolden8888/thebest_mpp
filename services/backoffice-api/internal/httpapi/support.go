// handleSupportMessagesSearch — GET /v1/support/messages/search
// (право support:trace, luminous-hugging-charm.md Ф9). Расширение уже
// существующего кросс-партнёрского обзора backoffice-api (тот же класс
// эндпоинта, что DlqBrowse/ReconciliationBrowse — messaging.
// message_read_model уже читает partner-api, но там намертво зашит
// partner_id из JWT; здесь тот же источник данных, без этого фильтра).
package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"mpp/backoffice-api/internal/store"
)

type supportMessageResponse struct {
	MessageID       string `json:"message_id"`
	PartnerID       string `json:"partner_id"`
	ApplicationID   string `json:"application_id"`
	TraceID         string `json:"trace_id"`
	PipelineID      string `json:"pipeline_id"`
	PipelineVersion string `json:"pipeline_version"`
	CurrentStatus   string `json:"current_status"`
	Terminal        bool   `json:"terminal"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
}

func toSupportMessageResponse(m store.SupportMessage) supportMessageResponse {
	return supportMessageResponse{
		MessageID:       m.MessageID,
		PartnerID:       m.PartnerID,
		ApplicationID:   m.ApplicationID,
		TraceID:         m.TraceID,
		PipelineID:      m.PipelineID,
		PipelineVersion: m.PipelineVersion,
		CurrentStatus:   m.CurrentStatus,
		Terminal:        m.Terminal,
		CreatedAt:       m.CreatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
		UpdatedAt:       m.UpdatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
	}
}

// handleSupportMessagesSearch — требует хотя бы message_id ИЛИ trace_id.
// Ни один из них не задан -> 400, не "верни всю таблицу кросс-партнёрски"
// (см. store.SupportMessageSearchFilter package doc). Поиск по msisdn не
// поддержан — параметр молча игнорируется, если передан (не 400 — msisdn
// как query-параметр синтаксически валиден, платформа просто не может его
// разрешить сегодня; см. README "Кросс-партнёрский поиск" за полным
// разбором, почему).
func handleSupportMessagesSearch(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		messageID := q.Get("message_id")
		traceID := q.Get("trace_id")
		if messageID == "" && traceID == "" {
			http.Error(w, "требуется хотя бы message_id или trace_id", http.StatusBadRequest)
			return
		}

		filter := store.SupportMessageSearchFilter{MessageID: messageID, TraceID: traceID}
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

		results, err := pg.SupportMessageSearch(r.Context(), filter)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "support_messages_search: ошибка чтения из PostgreSQL", err)
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
