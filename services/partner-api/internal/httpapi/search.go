package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"mpp/partner-api/internal/auth"
	"mpp/partner-api/internal/store"
)

type searchResults struct {
	Results []messageStatusResponse `json:"results"`
}

// handle_search_query — service_internal_methods.md §7.2: фильтры ->
// SearchResults (PostgreSQL read). partner_id всегда из JWT, остальные
// фильтры — из query-параметров.
func handleSearchQuery(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		q := r.URL.Query()
		filter := store.SearchFilter{
			PartnerID:     claims.PartnerID,
			ApplicationID: q.Get("application_id"),
			Status:        q.Get("status"),
		}
		if from := q.Get("from"); from != "" {
			t, err := time.Parse(time.RFC3339, from)
			if err != nil {
				http.Error(w, "неверный формат from (ожидался RFC3339)", http.StatusBadRequest)
				return
			}
			filter.CreatedFrom = t
		}
		if to := q.Get("to"); to != "" {
			t, err := time.Parse(time.RFC3339, to)
			if err != nil {
				http.Error(w, "неверный формат to (ожидался RFC3339)", http.StatusBadRequest)
				return
			}
			filter.CreatedTo = t
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

		rows, err := pg.Search(r.Context(), filter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := searchResults{Results: make([]messageStatusResponse, 0, len(rows))}
		for _, row := range rows {
			resp.Results = append(resp.Results, messageStatusResponse{
				MessageID:       row.MessageID,
				ApplicationID:   row.ApplicationID,
				TraceID:         row.TraceID,
				PipelineID:      row.PipelineID,
				PipelineVersion: row.PipelineVersion,
				CurrentStatus:   row.CurrentStatus,
				Terminal:        row.Terminal,
				CreatedAt:       row.CreatedAt,
				UpdatedAt:       row.UpdatedAt,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}