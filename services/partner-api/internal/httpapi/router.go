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
	"mpp/partner-api/internal/store"
	"mpp/partner-api/internal/telemetry"
)

func NewRouter(validator *auth.Validator, pg *store.Postgres, ch *store.ClickHouse, tp trace.TracerProvider) *chi.Mux {
	r := chi.NewRouter()

	r.Use(telemetry.Middleware(tp, func(req *http.Request) string {
		rctx := chi.RouteContext(req.Context())
		if rctx != nil && rctx.RoutePattern() != "" {
			return rctx.RoutePattern()
		}
		return req.URL.Path
	}))

	r.Route("/v1", func(r chi.Router) {
		r.Use(validator.Middleware)
		r.Get("/messages/status", handleStatusQuery(pg))
		r.Get("/messages/search", handleSearchQuery(pg))
		r.Get("/reports", handleReportQuery(ch))
	})

	return r
}