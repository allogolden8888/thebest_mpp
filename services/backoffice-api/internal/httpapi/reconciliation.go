package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"mpp/backoffice-api/internal/store"
)

type reconciliationCaseResponse struct {
	CaseID           string `json:"case_id"`
	MessageID        string `json:"message_id"`
	StageExecutionID string `json:"stage_execution_id"`
	OperatorID       string `json:"operator_id"`
	Status           string `json:"status"`
	OpenedAt         string `json:"opened_at"`
	ResolvedAt       string `json:"resolved_at,omitempty"`
	DeadlineAt       string `json:"deadline_at"`
}

// handle_reconciliation_browse — service_internal_methods.md §7.3: фильтры
// -> ReconciliationCases (PostgreSQL read).
func handleReconciliationBrowse(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := store.ReconciliationFilter{
			Status:     q.Get("status"),
			OperatorID: q.Get("operator_id"),
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
			n, err := strconv.Atoi(offset)
			if err != nil {
				http.Error(w, "неверный offset", http.StatusBadRequest)
				return
			}
			filter.Offset = n
		}

		rows, err := pg.ReconciliationBrowse(r.Context(), filter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		const layout = "2006-01-02T15:04:05.000Z07:00"
		results := make([]reconciliationCaseResponse, 0, len(rows))
		for _, row := range rows {
			resp := reconciliationCaseResponse{
				CaseID:           row.CaseID,
				MessageID:        row.MessageID,
				StageExecutionID: row.StageExecutionID,
				OperatorID:       row.OperatorID,
				Status:           row.Status,
				OpenedAt:         row.OpenedAt.Format(layout),
				DeadlineAt:       row.DeadlineAt.Format(layout),
			}
			if row.ResolvedAt != nil {
				resp.ResolvedAt = row.ResolvedAt.Format(layout)
			}
			results = append(results, resp)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Cases []reconciliationCaseResponse `json:"cases"`
		}{Cases: results})
	}
}
