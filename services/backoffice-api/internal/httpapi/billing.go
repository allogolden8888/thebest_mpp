// Экраны "Биллинг" (лента списаний + сводка) и блок операторских событий
// в карточке сообщения — BACKOFFICE_DESIGN_SPEC.md разделы 5 и 3C.
// Читают billing.billing_ledger и dlr.dlr_correlation напрямую, тот же
// класс прямого чтения чужих схем, что DlqBrowse/ReconciliationBrowse.
package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"mpp/backoffice-api/internal/store"
)

type ledgerEntryResponse struct {
	ID             int64   `json:"id"`
	ChargeID       string  `json:"charge_id"`
	AccountID      string  `json:"account_id"`
	PartnerID      string  `json:"partner_id"`
	Amount         string  `json:"amount"`
	Currency       string  `json:"currency"`
	EntryType      string  `json:"entry_type"`
	SourceChargeID *string `json:"source_charge_id"`
	CreatedAt      string  `json:"created_at"`
}

func toLedgerEntryResponse(e store.LedgerEntry) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID:             e.ID,
		ChargeID:       e.ChargeID,
		AccountID:      e.AccountID,
		PartnerID:      e.PartnerID,
		Amount:         e.Amount,
		Currency:       e.Currency,
		EntryType:      e.EntryType,
		SourceChargeID: e.SourceChargeID,
		CreatedAt:      e.CreatedAt.Format(timeFormat),
	}
}

const timeFormat = "2006-01-02T15:04:05.000Z07:00"

// parseTimeParam — пустой параметр это "фильтр не задан", а не ошибка;
// заданный, но не разобранный — именно ошибка (тихо игнорировать
// опечатку в дате значит показать оператору не тот период, чем он
// думает).
func parseTimeParam(raw string) (*time.Time, bool) {
	if raw == "" {
		return nil, true
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, false
	}
	return &t, true
}

func handleLedgerBrowse(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		f := store.LedgerFilter{
			PartnerID: q.Get("partner_id"),
			EntryType: q.Get("entry_type"),
		}
		from, ok := parseTimeParam(q.Get("from"))
		if !ok {
			http.Error(w, "неверный from: ожидался RFC3339", http.StatusBadRequest)
			return
		}
		to, ok := parseTimeParam(q.Get("to"))
		if !ok {
			http.Error(w, "неверный to: ожидался RFC3339", http.StatusBadRequest)
			return
		}
		f.From, f.To = from, to

		if limit := q.Get("limit"); limit != "" {
			n, err := strconv.Atoi(limit)
			if err != nil {
				http.Error(w, "неверный limit", http.StatusBadRequest)
				return
			}
			f.Limit = n
		}
		if offset := q.Get("offset"); offset != "" {
			n, ok := parseNonNegativeInt(offset)
			if !ok {
				http.Error(w, "неверный offset: ожидалось неотрицательное целое", http.StatusBadRequest)
				return
			}
			f.Offset = n
		}

		rows, err := pg.LedgerBrowse(r.Context(), f)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "ledger_browse: ошибка чтения из PostgreSQL", err)
			return
		}
		entries := make([]ledgerEntryResponse, 0, len(rows))
		for _, e := range rows {
			entries = append(entries, toLedgerEntryResponse(e))
		}
		writeJSON(w, struct {
			Entries []ledgerEntryResponse `json:"entries"`
		}{Entries: entries})
	}
}

func handleLedgerSummary(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		groupBy := q.Get("group_by")
		if groupBy == "" {
			groupBy = "partner"
		}
		if groupBy != "partner" && groupBy != "day" {
			http.Error(w, "неверный group_by: ожидалось partner|day", http.StatusBadRequest)
			return
		}
		from, ok := parseTimeParam(q.Get("from"))
		if !ok {
			http.Error(w, "неверный from: ожидался RFC3339", http.StatusBadRequest)
			return
		}
		to, ok := parseTimeParam(q.Get("to"))
		if !ok {
			http.Error(w, "неверный to: ожидался RFC3339", http.StatusBadRequest)
			return
		}

		rows, currency, err := pg.LedgerSummary(r.Context(), groupBy, from, to)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "ledger_summary: ошибка чтения из PostgreSQL", err)
			return
		}

		type summaryRow struct {
			Key           string `json:"key"`
			Charges       string `json:"charges"`
			Compensations string `json:"compensations"`
			Net           string `json:"net"`
			EntryCount    int64  `json:"entry_count"`
		}
		out := make([]summaryRow, 0, len(rows))
		for _, r := range rows {
			out = append(out, summaryRow{
				Key: r.Key, Charges: r.Charges, Compensations: r.Compensations,
				Net: r.Net, EntryCount: r.EntryCount,
			})
		}
		writeJSON(w, struct {
			Currency string       `json:"currency"`
			GroupBy  string       `json:"group_by"`
			Rows     []summaryRow `json:"rows"`
		}{Currency: currency, GroupBy: groupBy, Rows: out})
	}
}

// handleMessageOperatorEvents — GET /v1/messages/{message_id}/operator-events.
// Что реально ушло оператору по этому сообщению (submit-сторона) плюс
// дедлайн ожидания DLR. Сам факт/время прихода DLR виден в ленте статусов
// карточки (переход в DELIVERED) — отдельного персиста DLR в платформе
// нет, см. store.OperatorEventsByMessage.
func handleMessageOperatorEvents(pg *store.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		messageID := chi.URLParam(r, "message_id")
		rows, err := pg.OperatorEventsByMessage(r.Context(), messageID)
		if err != nil {
			internalError(w, http.StatusInternalServerError, "operator_events: ошибка чтения из PostgreSQL", err)
			return
		}

		type segment struct {
			SegmentID        int32  `json:"segment_id"`
			OperatorID       string `json:"operator_id"`
			SmscMessageID    string `json:"smsc_message_id"`
			StageExecutionID string `json:"stage_execution_id"`
			SubmittedAt      string `json:"submitted_at"`
			DlrExpiresAt     string `json:"dlr_expires_at"`
		}
		segments := make([]segment, 0, len(rows))
		for _, s := range rows {
			segments = append(segments, segment{
				SegmentID:        s.SegmentID,
				OperatorID:       s.OperatorID,
				SmscMessageID:    s.SmscMessageID,
				StageExecutionID: s.StageExecutionID,
				SubmittedAt:      s.SubmittedAt.Format(timeFormat),
				DlrExpiresAt:     s.ExpiresAt.Format(timeFormat),
			})
		}
		writeJSON(w, struct {
			MessageID string    `json:"message_id"`
			Segments  []segment `json:"segments"`
		}{MessageID: messageID, Segments: segments})
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
