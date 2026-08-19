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
	Validator              *auth.Validator
	Postgres               *store.Postgres
	ClickHouse             *store.ClickHouse
	Publisher              *kafkaio.Publisher
	ConfigClient           grpcv1.ConfigServiceClient
	ExecutionControl       grpcv1.ExecutionControlServiceClient
	Replay                 grpcv1.ReplayServiceClient
	IamClient              grpcv1.IamServiceClient
	CredentialIssuerClient grpcv1.CredentialIssuerServiceClient
	IncidentClient         grpcv1.IncidentServiceClient
	// HTTPClient/OpsVisibilityURL — luminous-hugging-charm.md Ф8.
	// ops-visibility-service не gRPC (см. ops.go package doc) — плоский
	// HTTP-клиент, не сгенерированный gRPC stub, как у остальных полей выше.
	HTTPClient       *http.Client
	OpsVisibilityURL string
	TracerProvider   trace.TracerProvider
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

		// luminous-hugging-charm.md Ф10 — config preview/diff. POST
		// /config/versions/validate — единственный POST-маршрут в этом
		// файле без gate прав: он ничего не пишет (ConfigService.
		// ValidateVersion не трогает ни config_versions, ни config_outbox,
		// см. package doc в internal_control.proto) — тот же класс
		// действия, что read-only browse выше, просто HTTP-метод POST,
		// потому что вход (payload_json) не помещается в query string.
		r.Post("/config/versions/validate", handleConfigValidateVersion(d.ConfigClient))
		r.Get("/config/versions/diff", handleConfigDiffVersions(d.ConfigClient))

		// GET /v1/me — без gate прав: любой аутентифицированный вызывающий
		// может увидеть СВОЙ ЖЕ эффективный доступ (luminous-hugging-charm.md
		// Фаза 0 — "видимость, которой сейчас нет вообще",
		// iam-service/README.md).
		r.Get("/me", handleMe(d.IamClient))

		// Деструктивные/мутирующие операции и административные IAM-
		// эндпоинты — гранулярные права через IamService.CheckPermission
		// (auth.RequirePermission, permission.go), не одна бинарная роль
		// AdminRole (CODE_REVIEW.md CRITICAL, закрыто Фазой 0 целиком — см.
		// package doc auth/jwt.go и iam-service/README.md). В отличие от
		// прежнего auth.RequireRole(auth.AdminRole), смонтированного одной
		// r.Group на все деструктивные маршруты разом, здесь у каждого
		// маршрута СВОЁ право — r.With(...) точечно на маршрут, а не общий
		// Group-gate.
		r.With(auth.RequirePermission("config:write", d.IamClient)).Post("/config/versions", handleConfigCreateVersion(d.ConfigClient))
		r.With(auth.RequirePermission("config:write", d.IamClient)).Post("/config/versions/archive", handleConfigArchiveVersion(d.ConfigClient))

		r.With(auth.RequirePermission("execution-control:write", d.IamClient)).Post("/execution-control/override", handleExecutionControlApplyOverride(d.ExecutionControl))
		r.With(auth.RequirePermission("execution-control:write", d.IamClient)).Post("/execution-control/override/clear", handleExecutionControlClearOverride(d.ExecutionControl))

		r.With(auth.RequirePermission("scheduler:force", d.IamClient)).Post("/scheduler/force-command", handleForceSchedulerCommand(d.Publisher))

		r.With(auth.RequirePermission("replay:request", d.IamClient)).Post("/replay", handleReplayRequest(d.Replay))

		// GET /v1/audit — агрегированный Audit Log из четырёх источников
		// прямыми SQL-запросами (audit.go), не через gRPC — тот же паттерн
		// прямого чтения чужих схем, что handleDlqBrowse/
		// handleReconciliationBrowse (store/postgres.go package doc).
		r.With(auth.RequirePermission("audit:read", d.IamClient)).Get("/audit", handleAuditBrowse(d.Postgres))

		// /v1/iam/* — административные эндпоинты для экрана backoffice-ui
		// "Access Control" (iam-service/README.md), проксируют
		// ListRoles/ListStaffAssignments/AssignStaffRole/RevokeStaffRole.
		r.Route("/iam", func(r chi.Router) {
			r.Use(auth.RequirePermission("iam:manage", d.IamClient))
			r.Get("/roles", handleIamListRoles(d.IamClient))
			r.Get("/staff-assignments", handleIamListStaffAssignments(d.IamClient))
			r.Post("/staff-assignments", handleIamAssignStaffRole(d.IamClient))
			r.Delete("/staff-assignments/{external_id}/{role}", handleIamRevokeStaffRole(d.IamClient))
		})

		// /v1/partners/{partner_id}/... — CredentialIssuerService proxy
		// (luminous-hugging-charm.md Ф1, credentials.go). Один gate
		// credentials:issue на обе операции — просмотр истории выпуска
		// секретов той же административной способности, что и сама
		// ротация, не отдельное read-only право (см. credentials.go
		// package doc и README.md для разбора этого решения).
		r.With(auth.RequirePermission("credentials:issue", d.IamClient)).
			Post("/partners/{partner_id}/applications/{application_id}/credentials/rotate", handleCredentialsRotate(d.CredentialIssuerClient))
		r.With(auth.RequirePermission("credentials:issue", d.IamClient)).
			Get("/partners/{partner_id}/credentials", handleCredentialsList(d.CredentialIssuerClient))

		// GET /v1/support/messages/search — luminous-hugging-charm.md Ф9,
		// кросс-партнёрский поиск для саппорта (support.go). Прямое чтение
		// messaging.message_read_model БЕЗ фильтра по partner_id — тот же
		// источник, что partner-api's Search, но там partner_id намертво
		// зашит из JWT (многотенантность), здесь сознательно нет (это и есть
		// "кросс-партнёрский").
		r.With(auth.RequirePermission("support:trace", d.IamClient)).
			Get("/support/messages/search", handleSupportMessagesSearch(d.Postgres))

		// GET /v1/messages — browse/пагинация, тот же messaging.
		// message_read_model, что search выше, но без обязательного id
		// (messages.go) — для списка последних сообщений на главном экране
		// backoffice-ui, а не точечного support-поиска.
		r.With(auth.RequirePermission("support:trace", d.IamClient)).
			Get("/messages", handleMessageBrowse(d.Postgres))

		// /v1/incidents/* — luminous-hugging-charm.md Ф7, IncidentService
		// proxy (incidents.go), backoffice-ui "Incidents". Один gate
		// incident:manage на все операции — открытие/заметки/закрытие
		// инцидента не разделены на отдельные права, тот же класс решения,
		// что credentials:issue выше.
		r.Route("/incidents", func(r chi.Router) {
			r.Use(auth.RequirePermission("incident:manage", d.IamClient))
			r.Post("/", handleOpenIncident(d.IncidentClient))
			r.Get("/", handleListIncidents(d.IncidentClient))
			r.Get("/{incident_id}", handleGetIncident(d.IncidentClient))
			r.Post("/{incident_id}/notes", handleAddIncidentNote(d.IncidentClient))
			r.Post("/{incident_id}/resolve", handleResolveIncident(d.IncidentClient))
		})

		// GET /v1/ops/snapshot — luminous-hugging-charm.md Ф8, плоский HTTP
		// прокси (ops.go, НЕ gRPC) в ops-visibility-service, backoffice-ui
		// "Ops Health".
		r.With(auth.RequirePermission("ops:read", d.IamClient)).
			Get("/ops/snapshot", handleOpsSnapshot(d.HTTPClient, d.OpsVisibilityURL))
	})

	return r
}
