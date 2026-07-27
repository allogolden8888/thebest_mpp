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

func NewRouter(d Deps) *chi.Mux {
	r := chi.NewRouter()

	r.Use(telemetry.Middleware(d.TracerProvider, func(req *http.Request) string {
		rctx := chi.RouteContext(req.Context())
		if rctx != nil && rctx.RoutePattern() != "" {
			return rctx.RoutePattern()
		}
		return req.URL.Path
	}))

	r.Route("/v1", func(r chi.Router) {
		r.Use(d.Validator.Middleware)

		r.Post("/config/versions", handleConfigCreateVersion(d.ConfigClient))
		r.Get("/config/versions/active", handleConfigGetActiveVersion(d.ConfigClient))
		r.Get("/config/versions", handleConfigListVersions(d.ConfigClient))
		r.Post("/config/versions/archive", handleConfigArchiveVersion(d.ConfigClient))

		r.Post("/execution-control/override", handleExecutionControlApplyOverride(d.ExecutionControl))
		r.Post("/execution-control/override/clear", handleExecutionControlClearOverride(d.ExecutionControl))

		r.Post("/scheduler/force-command", handleForceSchedulerCommand(d.Publisher))

		r.Get("/dlq", handleDlqBrowse(d.Postgres))
		r.Post("/replay", handleReplayRequest(d.Replay))

		r.Get("/reconciliation", handleReconciliationBrowse(d.Postgres))

		r.Get("/reports", handleReportQuery(d.ClickHouse))
	})

	return r
}