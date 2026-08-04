// Package httpapi — handle_status_query/handle_search_query/handle_report_query
// (service_internal_methods.md §7.2) поверх chi. Каждый маршрут защищён
// auth.Validator.Middleware — partner_id всегда берётся из JWT-claims, не из
// запроса.
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	"mpp/partner-api/internal/auth"
	"mpp/partner-api/internal/ratelimit"
	"mpp/partner-api/internal/store"
	"mpp/partner-api/internal/telemetry"
)

// maxRequestBodyBytes/rateLimitPerSecond/rateLimitBurst — CODE_REVIEW.md
// findings: partner-facing GET-эндпоинты принимали неограниченный объём
// фильтрованных запросов к Postgres/ClickHouse от одного партнёра/токена.
// Значения — консервативная отправная точка (не специфицированы ни в
// одном LLD), не distributed (см. package doc ratelimit).
const (
	rateLimitPerSecond = 20.0
	rateLimitBurst     = 40.0
)

func rateLimitMiddleware(limiter *ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := auth.ClaimsFromContext(r.Context())
			if !ok {
				http.Error(w, "нет claims в контексте", http.StatusInternalServerError)
				return
			}
			if !limiter.Allow(claims.PartnerID) {
				http.Error(w, "превышен лимит запросов", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func NewRouter(validator *auth.Validator, pg *store.Postgres, ch *store.ClickHouse, tp trace.TracerProvider) *chi.Mux {
	r := chi.NewRouter()

	r.Use(telemetry.Middleware(tp, func(req *http.Request) string {
		rctx := chi.RouteContext(req.Context())
		if rctx != nil && rctx.RoutePattern() != "" {
			return rctx.RoutePattern()
		}
		return req.URL.Path
	}))

	limiter := ratelimit.New(rateLimitPerSecond, rateLimitBurst)

	r.Route("/v1", func(r chi.Router) {
		r.Use(validator.Middleware)
		r.Use(rateLimitMiddleware(limiter))
		r.Get("/messages/status", handleStatusQuery(pg))
		r.Get("/messages/search", handleSearchQuery(pg))
		r.Get("/reports", handleReportQuery(ch))
	})

	return r
}