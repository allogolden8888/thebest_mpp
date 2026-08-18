package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"mpp/billing-self-service-api/internal/auth"
	"mpp/billing-self-service-api/internal/store"
)

// parseTimeParam — "from"/"to" query params, RFC3339. Пусто — не задано
// (zero time.Time), store.LedgerFilter уже трактует нулевое время как
// "без этой границы".
func parseTimeParam(r *http.Request, name string) (time.Time, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s должен быть в формате RFC3339: %w", name, err)
	}
	return t, nil
}

func handleLedger(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		from, err := parseTimeParam(r, "from")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		to, err := parseTimeParam(r, "to")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		limit := 0
		if raw := r.URL.Query().Get("limit"); raw != "" {
			limit, _ = strconv.Atoi(raw)
		}
		offset := 0
		if raw := r.URL.Query().Get("offset"); raw != "" {
			offset, _ = strconv.Atoi(raw)
		}

		entries, err := d.Store.Ledger(r.Context(), store.LedgerFilter{
			PartnerID: claims.PartnerID, CreatedFrom: from, CreatedTo: to, Limit: limit, Offset: offset,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать историю списаний: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries)
	}
}

func handleSpendSummary(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		from, err := parseTimeParam(r, "from")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		to, err := parseTimeParam(r, "to")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if from.IsZero() || to.IsZero() {
			http.Error(w, "from и to обязательны для /summary (RFC3339)", http.StatusBadRequest)
			return
		}

		summary, err := d.Store.SpendSummary(r.Context(), claims.PartnerID, from, to)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось посчитать сводку списаний: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(summary)
	}
}

// tariffResponse — Custom=false означает "для партнёра не опубликован
// собственный BILLING_TARIFF, применяется платформенный дефолт" — конкретные
// цифры дефолта здесь недоступны (см. configreads.go doc-комментарий),
// возвращаем это явно, не подставляем угаданные значения.
type tariffResponse struct {
	Custom bool           `json:"custom"`
	Tariff *billingTariff `json:"tariff,omitempty"`
}

func handleTariff(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		tariff, found, err := getPartnerTariff(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать тариф: %v", err), http.StatusBadGateway)
			return
		}

		resp := tariffResponse{Custom: found}
		if found {
			resp.Tariff = &tariff
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// recurringChargePreview — один active sender + его ожидаемая ежемесячная
// плата, если она вообще определена (см. FeeKnown).
type recurringChargePreview struct {
	SenderID string `json:"sender_id"`
	Type     string `json:"type"`
	FeeKnown bool   `json:"fee_known"`
	Fee      *int64 `json:"fee,omitempty"`
	Currency string `json:"currency,omitempty"`
}

func handleRecurring(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		senders, err := getActiveSenders(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать отправителей партнёра: %v", err), http.StatusBadGateway)
			return
		}

		tariff, found, err := getPartnerTariff(r.Context(), d.ConfigClient, claims.PartnerID)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось прочитать тариф: %v", err), http.StatusBadGateway)
			return
		}

		var fee *int64
		var currency string
		if found && tariff.RecurringCharges != nil && tariff.RecurringCharges.AlphanameMonthlyFee != nil {
			fee = tariff.RecurringCharges.AlphanameMonthlyFee
			currency = tariff.Currency
		}

		previews := make([]recurringChargePreview, 0, len(senders))
		for _, s := range senders {
			previews = append(previews, recurringChargePreview{
				SenderID: s.SenderID, Type: s.Type, FeeKnown: fee != nil, Fee: fee, Currency: currency,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(previews)
	}
}
