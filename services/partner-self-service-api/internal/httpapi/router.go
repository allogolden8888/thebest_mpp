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
	// ChatClient — BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat" (chat.go),
	// proxy в chat-service. Тот же класс зависимости, что CredentialClient
	// выше — маленький выделенный control-plane сервис, не прямой доступ к
	// support.chat_messages (см. services/chat-service/README.md).
	ChatClient grpcv1.ChatServiceClient
	// TemplatesServiceURL — базовый URL template-management-service (Ф4,
	// без trailing slash), используется только handleListTemplates
	// (templates.go) — см. doc-комментарий там за тем, почему это
	// аутентифицированный прокси, а не прямой клиентский вызов.
	TemplatesServiceURL string
	// IamClient — BACKOFFICE_ROADMAP.md Production Readiness Review P0#5
	// (auth.go's handleLogin, internal/auth/resolve.go's ResolveLiveAccess).
	// Новая зависимость этого сервиса — до этого захода
	// partner-self-service-api вообще не говорил с iam-service (см. package
	// doc jwt.go за тем, почему раньше — осознанное упрощение, теперь
	// закрытое).
	IamClient grpcv1.IamServiceClient
	// TokenIssuer — auth.go's handleLogin, internal/auth/issuer.go. Первый
	// случай, когда partner-self-service-api сам ПОДПИСЫВАЕТ JWT, не только
	// валидирует чужие — см. package doc там за разбором отдельного от
	// остальных self-service API keypair'а.
	TokenIssuer    *auth.TokenIssuer
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

	// POST /v1/self-service/auth/login — BACKOFFICE_ROADMAP.md Production
	// Readiness Review P0#5 (auth.go). Единственный маршрут этого сервиса
	// БЕЗ d.Validator.Middleware — вызывающий по определению ещё не имеет
	// JWT на этом шаге, это и есть то, что этот маршрут выдаёт. Отдельный
	// r.Route ДО группы с Use(d.Validator.Middleware) ниже — в chi нельзя
	// смонтировать маршрут без middleware внутри роутера, который уже вызвал
	// Use() (тот же порядок, что backoffice-api/internal/httpapi/router.go).
	r.Route("/v1/self-service/auth", func(r chi.Router) {
		r.Post("/login", handleLogin(d.IamClient, d.TokenIssuer))
	})

	r.Route("/v1/self-service", func(r chi.Router) {
		r.Use(d.Validator.Middleware)
		// ResolveLiveAccess — BACKOFFICE_ROADMAP.md Production Readiness
		// Review P0#5 (internal/auth/resolve.go). Смонтирован ПОСЛЕ
		// Validator.Middleware (нужны claims в контексте) и ПЕРЕД всеми
		// mountXxx ниже — перезаписывает claims.PartnerID/claims.IsAdmin()
		// живым результатом IamService.ResolvePartnerPortalAccess ДО того,
		// как любой хендлер их прочитает, так что ни один из них не
		// нуждается в изменении.
		r.Use(auth.ResolveLiveAccess(d.IamClient))

		mountApplications(r, d)
		mountSenders(r, d)
		mountCredentials(r, d)
		mountWebhook(r, d)
		mountTemplates(r, d)
		mountChat(r, d)
	})

	return r
}
