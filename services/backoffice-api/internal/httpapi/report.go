package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"mpp/backoffice-api/internal/store"
)

type reportRow struct {
	Hour       time.Time `json:"hour"`
	PartnerID  string    `json:"partner_id"`
	StageName  string    `json:"stage_name"`
	Outcome    string    `json:"outcome"`
	EventCount uint64    `json:"event_count"`
}

// handle_report_query — service_internal_methods.md §7.3: HTTP-запрос ->
// ReportResponse (ClickHouse read). В отличие от Partner API, partner_id —
// опциональный фильтр (оператор видит все партнёры по умолчанию).
func handleReportQuery(ch *store.ClickHouse) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := store.ReportFilter{
			PartnerID: q.Get("partner_id"),
			StageName: q.Get("stage_name"),
		}
		if from := q.Get("from"); from != "" {
			t, err := time.Parse(time.RFC3339, from)
			if err != nil {
				http.Error(w, "неверный формат from (ожидался RFC3339)", http.StatusBadRequest)
				return
			}
			filter.From = t
		}
		if to := q.Get("to"); to != "" {
			t, err := time.Parse(time.RFC3339, to)
			if err != nil {
				http.Error(w, "неверный формат to (ожидался RFC3339)", http.StatusBadRequest)
				return
			}
			filter.To = t
		}

		rows, err := ch.Report(r.Context(), filter)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "report_query: ошибка чтения из ClickHouse", err)
			return
		}

		results := make([]reportRow, 0, len(rows))
		for _, row := range rows {
			results = append(results, reportRow{
				Hour:       row.Hour,
				PartnerID:  row.PartnerID,
				StageName:  row.StageName,
				Outcome:    row.Outcome,
				EventCount: row.EventCount,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Rows []reportRow `json:"rows"`
		}{Rows: results})
	}
}
