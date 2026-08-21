// GET /v1/messages/{message_id}/timeline — пер-стадийная хронология
// сообщения (BACKOFFICE_DESIGN_SPEC.md §3B): на какой стадии сколько
// времени провело. Источник — analytics.stage_events (ClickHouse).
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"mpp/backoffice-api/internal/store"
)

type stageTimelineResponse struct {
	StageName  string `json:"stage_name"`
	Outcome    string `json:"outcome"`
	ReasonCode string `json:"reason_code"`
	OccurredAt string `json:"occurred_at"`
	// DurationMs — сколько заняла САМА эта стадия: от завершения
	// предыдущей до завершения этой. Для первой стадии null, а не 0:
	// момент начала пайплайна (приём сообщения) лежит в другом источнике
	// (messaging.message_read_model.created_at), и подставлять сюда 0
	// значило бы утверждать, что стадия была мгновенной.
	DurationMs *int64 `json:"duration_ms"`
}

func handleMessageTimeline(ch *store.ClickHouse) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		messageID := chi.URLParam(r, "message_id")
		if ch == nil {
			http.Error(w, "аналитика недоступна: ClickHouse не сконфигурирован", http.StatusServiceUnavailable)
			return
		}

		entries, err := ch.MessageStageTimeline(r.Context(), messageID)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "message_timeline: ошибка чтения из ClickHouse", err)
			return
		}

		stages := make([]stageTimelineResponse, 0, len(entries))
		var totalMs int64
		for i, e := range entries {
			item := stageTimelineResponse{
				StageName:  e.StageName,
				Outcome:    e.Outcome,
				ReasonCode: e.ReasonCode,
				OccurredAt: e.OccurredAt.Format(timeFormat),
			}
			if i > 0 {
				d := e.OccurredAt.Sub(entries[i-1].OccurredAt).Milliseconds()
				item.DurationMs = &d
			}
			stages = append(stages, item)
		}
		if len(entries) > 1 {
			totalMs = entries[len(entries)-1].OccurredAt.Sub(entries[0].OccurredAt).Milliseconds()
		}

		writeJSON(w, struct {
			MessageID       string                  `json:"message_id"`
			TotalDurationMs int64                   `json:"total_duration_ms"`
			Stages          []stageTimelineResponse `json:"stages"`
		}{MessageID: messageID, TotalDurationMs: totalMs, Stages: stages})
	}
}
