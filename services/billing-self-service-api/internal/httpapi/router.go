// Package httpapi — GET-only self-service billing эндпоинты (Фаза 5 плана
// закрытия API-пробелов, после Фазы 5a — multi-tenancy в billing-service).
// Тонкий read-mostly сервис: billing.billing_ledger читается напрямую
// (billing-self-service-api владеет собственным Postgres-подключением, по
// образцу backoffice-api), BILLING_TARIFF/PARTNER — через ConfigServiceClient
// (payload_json, закрыто в Фазе 3 — "platform-contracts:
// ConfigVersionResponse.payload_json").
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/billing-self-service-api/internal/auth"
	"mpp/billing-self-service-api/internal/store"
	"mpp/billing-self-service-api/internal/telemetry"
)

type Deps struct {
	Validator      *auth.Validator
	ConfigClient   grpcv1.ConfigServiceClient
	Store          *store.Postgres
	TracerProvider trace.TracerProvider
}

func NewRouter(d Deps) *chi.Mux {
	r := chi.NewRouter()

	r.Use(telemetry.Middleware(d.TracerProvider, func(req *http.Request) string {
		rctx := chi.RouteContext(req.Context())
		if rctx != nil && rctx.RoutePattern() != "" {
			return rctx.RoutePattern()
		}
		return req.URL.Path
	}))

	r.Route("/v1/self-service/billing", func(r chi.Router) {
		r.Use(d.Validator.Middleware)

		r.Get("/ledger", handleLedger(d))
		r.Get("/summary", handleSpendSummary(d))
		r.Get("/tariff", handleTariff(d))
		r.Get("/recurring", handleRecurring(d))
	})

	return r
}
