package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"mpp/partner-api/internal/auth"
	"mpp/partner-api/internal/store"
)

type reportResponse struct {
	Rows []reportRow `json:"rows"`
}

type reportRow struct {
	Hour       time.Time `json:"hour"`
	StageName  string    `json:"stage_name"`
	Outcome    string    `json:"outcome"`
	EventCount uint64    `json:"event_count"`
}

// handle_report_query — service_internal_methods.md §7.2: период/агрегат ->
// ReportResponse (ClickHouse read).
func handleReportQuery(ch *store.ClickHouse) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		q := r.URL.Query()
		filter := store.ReportFilter{
			PartnerID: claims.PartnerID,
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := reportResponse{Rows: make([]reportRow, 0, len(rows))}
		for _, row := range rows {
			resp.Rows = append(resp.Rows, reportRow{
				Hour:       row.Hour,
				StageName:  row.StageName,
				Outcome:    row.Outcome,
				EventCount: row.EventCount,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}