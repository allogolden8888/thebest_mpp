// Package httpapi — GET/POST /v1/compliance/consent
// (luminous-hugging-charm.md, BACKOFFICE_ROADMAP.md §4 "Blacklist"). Плоский
// HTTP-прокси в compliance-api (тот же паттерн, что ops.go →
// ops-visibility-service — compliance-api тоже не gRPC, см. его
// internal/httpapi/router.go).
//
// В отличие от ops.go: compliance-api сам проверяет JWT (d.Validator.
// Middleware смонтирован на весь /v1/compliance у него, см. его router.go) и
// сам гейтит запись правом compliance:write — значит здесь нужно
// форвардить Authorization вызывающего, не просто проксировать тело.
// GET здесь намеренно БЕЗ auth.RequirePermission (тот же выбор, что уже
// сделан в compliance-api: chтение открыто любому валидному токену realm'а),
// POST — с compliance:write, чтобы отклонять без права ДО похода в сеть, а
// не только полагаться на 403 от compliance-api.
package httpapi

import (
	"context"
	"io"
	"net/http"
	"time"
)

const complianceProxyTimeout = 5 * time.Second

func proxyToComplianceAPI(client *http.Client, method, url string, r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(r.Context(), complianceProxyTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, r.Body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	return client.Do(req)
}

func writeProxiedResponse(w http.ResponseWriter, resp *http.Response, context string) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		internalError(w, http.StatusBadGateway, context+": чтение ответа Compliance API не удалось", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

// handleConsentLookup — GET /v1/compliance/consent?msisdn=.
func handleConsentLookup(client *http.Client, complianceAPIURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := proxyToComplianceAPI(client, http.MethodGet, complianceAPIURL+"/v1/compliance/consent?"+r.URL.RawQuery, r)
		if err != nil {
			internalError(w, http.StatusBadGateway, "consent_lookup: запрос к Compliance API не удался", err)
			return
		}
		writeProxiedResponse(w, resp, "consent_lookup")
	}
}

// handleManualConsent — POST /v1/compliance/consent (ручная блокировка/
// разблокировка msisdn по category/sender, вне обычного STOP-потока).
func handleManualConsent(client *http.Client, complianceAPIURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := proxyToComplianceAPI(client, http.MethodPost, complianceAPIURL+"/v1/compliance/consent", r)
		if err != nil {
			internalError(w, http.StatusBadGateway, "manual_consent: запрос к Compliance API не удался", err)
			return
		}
		writeProxiedResponse(w, resp, "manual_consent")
	}
}
