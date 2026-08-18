// Package httpapi — точка сборки роутера partner-self-service-api (Фаза 3
// плана, /Users/Alisher/.claude/plans/luminous-hugging-charm.md). Сам файл
// не содержит хендлеров — только merge-точку: applications.go/senders.go,
// credentials.go, webhook.go монтируют свои саброуты через NewRouter,
// не трогая этот файл (тот же паттерн разделения, что list.rs/preview.rs/
// bulk.rs в template-management-service — Ф4).
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/partner-self-service-api/internal/auth"
	"mpp/partner-self-service-api/internal/telemetry"
)

// maxRequestBodyBytes — тот же CODE_REVIEW.md-класс защиты, что уже
// применён в backoffice-api/compliance-api: мутирующий эндпоинт не должен
// принимать неограниченное тело от аутентифицированного вызывающего.
const maxRequestBodyBytes = 1 << 20

func maxBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
}

type Deps struct {
	Validator        *auth.Validator
	ConfigClient     grpcv1.ConfigServiceClient
	CredentialClient grpcv1.CredentialIssuerServiceClient
	// TemplatesServiceURL — базовый URL template-management-service (Ф4,
	// без trailing slash), используется только handleListTemplates
	// (templates.go) — см. doc-комментарий там за тем, почему это
	// аутентифицированный прокси, а не прямой клиентский вызов.
	TemplatesServiceURL string
	TracerProvider      trace.TracerProvider
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

	r.Route("/v1/self-service", func(r chi.Router) {
		r.Use(d.Validator.Middleware)

		mountApplications(r, d)
		mountSenders(r, d)
		mountCredentials(r, d)
		mountWebhook(r, d)
		mountTemplates(r, d)
	})

	return r
}
