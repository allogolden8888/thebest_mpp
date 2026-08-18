// Package httpapi — GET /v1/self-service/templates: аутентифицированный
// прокси в template-management-service (Ф4 плана закрытия API-пробелов).
//
// Найдено при подготовке Ф3-фронтенда (partner-portal-ui, раздел "Мои
// шаблоны"): template-management-service вообще не несёт auth
// (services/template-management-service/src/main.rs не монтирует никакой
// JWT-middleware) — GET /v1/templates принимает partner_id обычным query-
// параметром без какой-либо проверки. Это осознанно нормально для
// backoffice-ui (admin-gated отдельным правом на уровне backoffice-api),
// но НЕДОПУСТИМО открывать напрямую партнёрскому порталу: любой партнёр
// мог бы прочитать чужую библиотеку шаблонов, просто подставив чужой
// partner_id в query string. Этот хендлер — единственная точка, откуда
// partner-portal-ui имеет право читать шаблоны: partner_id ВСЕГДА
// перезаписывается из claims.PartnerID, никогда не берётся из запроса
// вызывающего.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"

	"mpp/partner-self-service-api/internal/auth"
)

const templatesProxyTimeout = 5 * time.Second

// templateRow — зеркало TemplateRow из
// services/template-management-service/src/list.rs (см. struct там же за
// авторитетным источником формы).
type templateRow struct {
	TemplateID string  `json:"template_id"`
	PartnerID  string  `json:"partner_id"`
	OperatorID *string `json:"operator_id"`
	SenderID   *string `json:"sender_id"`
	Channel    string  `json:"channel"`
	Category   string  `json:"category"`
	Pattern    string  `json:"pattern"`
	Version    int32   `json:"version"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
}

type templatesListResponse struct {
	Templates []templateRow `json:"templates"`
	Limit     int64         `json:"limit"`
	Offset    int64         `json:"offset"`
}

func mountTemplates(r chi.Router, d Deps) {
	r.Get("/templates", handleListTemplates(d))
}

// handleListTemplates — пробрасывает sender_id/category/status/limit/offset
// из входящего запроса как есть (безопасны — управляют только тем, ЧТО
// внутри собственных шаблонов партнёра видно, не ЧЬИ шаблоны видны),
// partner_id — всегда claims.PartnerID, полностью игнорирует одноимённый
// query-параметр вызывающего, если он был передан.
func handleListTemplates(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := auth.ClaimsFromContext(r.Context())
		if !ok {
			http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
			return
		}

		q := url.Values{}
		q.Set("partner_id", claims.PartnerID)
		for _, key := range []string{"sender_id", "category", "status", "limit", "offset"} {
			if v := r.URL.Query().Get(key); v != "" {
				q.Set(key, v)
			}
		}

		targetURL := d.TemplatesServiceURL + "/v1/templates?" + q.Encode()

		ctx, cancel := context.WithTimeout(r.Context(), templatesProxyTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("не удалось построить запрос к template-management-service: %v", err), http.StatusInternalServerError)
			return
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, fmt.Sprintf("template-management-service недоступен: %v", err), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			http.Error(w, fmt.Sprintf("template-management-service вернул %d", resp.StatusCode), http.StatusBadGateway)
			return
		}

		var upstream templatesListResponse
		if err := json.NewDecoder(resp.Body).Decode(&upstream); err != nil {
			http.Error(w, fmt.Sprintf("не удалось разобрать ответ template-management-service: %v", err), http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(upstream)
	}
}
