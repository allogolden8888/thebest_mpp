// Package httpapi — все 7 методов service_internal_methods.md §7.3 поверх
// chi. Каждый маршрут защищён auth.Validator.Middleware — requested_by для
// аудита всегда берётся из JWT `sub`, не из тела запроса.
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/backoffice-api/internal/auth"
	"mpp/backoffice-api/internal/kafkaio"
	"mpp/backoffice-api/internal/store"
	"mpp/backoffice-api/internal/telemetry"
)

type Deps struct {
	Validator          *auth.Validator
	Postgres           *store.Postgres
	ClickHouse         *store.ClickHouse
	Publisher          *kafkaio.Publisher
	ConfigClient       grpcv1.ConfigServiceClient
	ExecutionControl   grpcv1.ExecutionControlServiceClient
	Replay             grpcv1.ReplayServiceClient
	TracerProvider     trace.TracerProvider
}

// maxRequestBodyBytes — CODE_REVIEW.md Low finding: ни один мутирующий
// эндпоинт не ограничивал размер тела запроса (json.Decode без
// http.MaxBytesReader), позволяя аутентифицированному вызывающему прислать
// произвольно большое тело. 1 МиБ — с большим запасом покрывает самое
// крупное реальное тело (payload_json конфигурации).
const maxRequestBodyBytes = 1 << 20

func maxBodyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		next.ServeHTTP(w, r)
	})
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

	r.Route("/v1", func(r chi.Router) {
		r.Use(d.Validator.Middleware)

		// Read-only browse/report — доступны любому валидному токену
		// realm'а, ничего не мутируют.
		r.Get("/config/versions/active", handleConfigGetActiveVersion(d.ConfigClient))
		r.Get("/config/versions", handleConfigListVersions(d.ConfigClient))
		r.Get("/dlq", handleDlqBrowse(d.Postgres))
		r.Get("/reconciliation", handleReconciliationBrowse(d.Postgres))
		r.Get("/reports", handleReportQuery(d.ClickHouse))

		// Деструктивные/мутирующие операции — требуют роль AdminRole
		// (CODE_REVIEW.md CRITICAL — раньше здесь не было вообще никакой
		// проверки прав, см. auth.RequireRole и package doc в auth/jwt.go).
		r.Group(func(r chi.Router) {
			r.Use(auth.RequireRole(auth.AdminRole))

			r.Post("/config/versions", handleConfigCreateVersion(d.ConfigClient))
			r.Post("/config/versions/archive", handleConfigArchiveVersion(d.ConfigClient))

			r.Post("/execution-control/override", handleExecutionControlApplyOverride(d.ExecutionControl))
			r.Post("/execution-control/override/clear", handleExecutionControlClearOverride(d.ExecutionControl))

			r.Post("/scheduler/force-command", handleForceSchedulerCommand(d.Publisher))

			r.Post("/replay", handleReplayRequest(d.Replay))
		})
	})

	return r
}