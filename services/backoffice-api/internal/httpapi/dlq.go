package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"mpp/backoffice-api/internal/store"
)

type dlqRecordResponse struct {
	StageExecutionID string `json:"stage_execution_id"`
	MessageID        string `json:"message_id"`
	StageName        string `json:"stage_name"`
	Attempt          int32  `json:"attempt"`
	ReasonCode       string `json:"reason_code"`
	ErrorDetail      string `json:"error_detail,omitempty"`
	CreatedAt        string `json:"created_at"`
	ReplayStatus     string `json:"replay_status"`
}

// handle_dlq_browse — service_internal_methods.md §7.3: фильтры ->
// DlqRecords (PostgreSQL read).
func handleDlqBrowse(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := store.DlqFilter{
			StageName:    q.Get("stage_name"),
			ReplayStatus: q.Get("replay_status"),
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

		rows, err := pg.DlqBrowse(r.Context(), filter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		results := make([]dlqRecordResponse, 0, len(rows))
		for _, row := range rows {
			results = append(results, dlqRecordResponse{
				StageExecutionID: row.StageExecutionID,
				MessageID:        row.MessageID,
				StageName:        row.StageName,
				Attempt:          row.Attempt,
				ReasonCode:       row.ReasonCode,
				ErrorDetail:      row.ErrorDetail,
				CreatedAt:        row.CreatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
				ReplayStatus:     row.ReplayStatus,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Records []dlqRecordResponse `json:"records"`
		}{Records: results})
	}
}
