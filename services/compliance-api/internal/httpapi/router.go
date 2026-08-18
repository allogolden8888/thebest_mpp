// Package httpapi — GET/POST /v1/compliance/consent (Фаза 6 плана,
// service_internal_methods.md-стиль обвязки поверх chi, тот же паттерн,
// что backoffice-api/partner-api). Чтение открыто любому валидному токену
// realm'а (как и read-only эндпоинты backoffice-api) — ручная запись
// требует права compliance:write через IamService (см.
// migrations/V025__iam.sql — право уже засеяно, роль compliance-officer
// уже существует).
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/compliance-api/internal/auth"
	"mpp/compliance-api/internal/redisio"
	"mpp/compliance-api/internal/telemetry"
)

// maxRequestBodyBytes — тот же CODE_REVIEW.md-класс защиты, что уже
// применён в backoffice-api: мутирующий эндпоинт не должен принимать
// неограниченное тело от аутентифицированного вызывающего.
const maxRequestBodyBytes = 1 << 20

func maxBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

type Deps struct {
	Validator      *auth.Validator
	Redis          *redisio.Client
	ConfigClient   grpcv1.ConfigServiceClient
	IamClient      grpcv1.IamServiceClient
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
	r.Use(maxBodyMiddleware)

	r.Route("/v1/compliance", func(r chi.Router) {
		r.Use(d.Validator.Middleware)

		r.Get("/consent", handleConsentLookup(d.Redis))

		r.With(auth.RequirePermission("compliance:write", d.IamClient)).
			Post("/consent", handleManualConsent(d.ConfigClient))
	})

	return r
}
