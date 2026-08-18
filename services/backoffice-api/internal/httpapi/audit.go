package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"mpp/backoffice-api/internal/store"
)

type auditEntryResponse struct {
	Source    string `json:"source"`
	Actor     string `json:"actor"`
	Action    string `json:"action"`
	Target    string `json:"target"`
	CreatedAt string `json:"created_at"`
}

// handleAuditBrowse — GET /v1/audit?source=&limit=&offset=: объединённый
// Audit Log из messaging.replay_audit/control.execution_control_audit/
// billing.reconciliation_audit/iam.identity_audit (store.Postgres.AuditBrowse
// — маппинг колонок задокументирован там). next_offset присутствует в ответе
// только когда за текущей страницей есть ещё строки (omitempty на *int, а не
// 0 — 0 сам по себе валидный offset).
func handleAuditBrowse(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		filter := store.AuditFilter{Source: q.Get("source")}

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

		rows, hasMore, err := pg.AuditBrowse(r.Context(), filter)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "audit_browse: ошибка чтения из PostgreSQL", err)
			return
		}

		entries := make([]auditEntryResponse, 0, len(rows))
		for _, row := range rows {
			entries = append(entries, auditEntryResponse{
				Source:    row.Source,
				Actor:     row.Actor,
				Action:    row.Action,
				Target:    row.Target,
				CreatedAt: row.CreatedAt.Format("2006-01-02T15:04:05.000Z07:00"),
			})
		}

		var nextOffset *int
		if hasMore {
			n := filter.Offset + len(entries)
			nextOffset = &n
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Entries    []auditEntryResponse `json:"entries"`
			NextOffset *int                 `json:"next_offset,omitempty"`
		}{Entries: entries, NextOffset: nextOffset})
	}
}
