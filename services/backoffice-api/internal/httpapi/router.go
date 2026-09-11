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
	// ChatClient — BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat" (chat.go),
	// proxy в новый chat-service. Тот же класс зависимости, что
	// IncidentClient выше — маленький выделенный control-plane сервис,
	// не прямой доступ к support.chat_messages (см. chat.go package doc
	// и services/chat-service/README.md за полным разбором решения).
	ChatClient grpcv1.ChatServiceClient
	// HTTPClient/OpsVisibilityURL — luminous-hugging-charm.md Ф8.
	// ops-visibility-service не gRPC (см. ops.go package doc) — плоский
	// HTTP-клиент, не сгенерированный gRPC stub, как у остальных полей выше.
	HTTPClient       *http.Client
	OpsVisibilityURL string
	// ComplianceAPIURL — BACKOFFICE_ROADMAP.md §4 "Blacklist" (compliance.go).
	// compliance-api тоже плоский HTTP, не gRPC — reuse d.HTTPClient, тот же
	// класс зависимости, что OpsVisibilityURL выше.
	ComplianceAPIURL string
	// RedisRuntime — BACKOFFICE_ROADMAP.md §2 "Операторы" (operators.go),
	// read-only снимок operator_route:* (store/redis.go). Единственное
	// прямое Redis-чтение в backoffice-api — тот же класс решения, что
	// прямое чтение чужих Postgres-схем (Postgres/ClickHouse выше, см.
	// store/postgres.go package doc), не проксирование через gRPC.
	RedisRuntime *store.Redis
	// TokenIssuer — luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md
	// Экран 33 "Admin users" (auth.go). Первый случай, когда backoffice-api
	// сам ПОДПИСЫВАЕТ JWT, не только валидирует чужие — см.
	// internal/auth/issuer.go package doc.
	TokenIssuer    *auth.TokenIssuer
	TracerProvider trace.TracerProvider
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

	// POST /v1/auth/login — luminous-hugging-charm.md, BACKOFFICE_DESIGN_
	// SPEC.md Экран 33 "Admin users" (auth.go). Единственный маршрут этого
	// сервиса БЕЗ d.Validator.Middleware — по определению, вызывающий ещё
	// не имеет JWT на этом шаге, это и есть то, что этот маршрут выдаёт.
	r.Route("/v1/auth", func(r chi.Router) {
		r.Post("/login", handleLogin(d.IamClient, d.TokenIssuer))
	})

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
			// BACKOFFICE_DESIGN_SPEC.md Экран 35 "Partner Users" —
			// iam.partner_portal_role_assignments (V025) уже реально
			// используется (partner-self-service-api), но не имело
			// админского эндпоинта до сих пор. Тот же gate iam:manage —
			// один экран/одна административная способность, не отдельное
			// право.
			r.Get("/partner-portal-assignments", handleIamListPartnerPortalAssignments(d.IamClient))
			r.Post("/partner-portal-assignments", handleIamAssignPartnerPortalRole(d.IamClient))
			r.Delete("/partner-portal-assignments/{external_id}/{role}", handleIamRevokePartnerPortalRole(d.IamClient))
			// BACKOFFICE_DESIGN_SPEC.md Экран 33 "Admin users" — управление
			// локальными staff-аккаунтами (auth.go's handleLogin — сам
			// логин, эти три — административный CRUD над учётками).
			r.Get("/staff-accounts", handleIamListStaffAccounts(d.IamClient))
			r.Post("/staff-accounts", handleIamCreateStaffAccount(d.IamClient))
			r.Post("/staff-accounts/{external_id}/deactivate", handleIamDeactivateStaffAccount(d.IamClient))
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
		r.With(auth.RequirePermission("support:trace", d.IamClient)).
			Get("/messages/{message_id}", handleMessageDetail(d.Postgres))
		// Операторская (SMPP) сторона по сообщению — что и когда ушло
		// оператору, dlr.dlr_correlation (billing.go).
		r.With(auth.RequirePermission("support:trace", d.IamClient)).
			Get("/messages/{message_id}/operator-events", handleMessageOperatorEvents(d.Postgres))
		// Пер-стадийная хронология ("где сколько провело") — ClickHouse,
		// analytics.stage_events (timeline.go).
		r.With(auth.RequirePermission("support:trace", d.IamClient)).
			Get("/messages/{message_id}/timeline", handleMessageTimeline(d.ClickHouse))
		// Пер-PDU лог (Экраны 38-40) — каждый реальный SMPP PDU, а не
		// агрегат по стадии/сегменту, как два маршрута выше (pdulog.go).
		r.With(auth.RequirePermission("support:trace", d.IamClient)).
			Get("/messages/{message_id}/pdu-log", handleMessagePduLog(d.ClickHouse, d.Postgres))

		// /v1/billing/* — лента списаний и сводка (billing.go). Читают
		// billing.billing_ledger напрямую. Право audit:read, а не
		// отдельное billing:read: финансовая видимость здесь того же
		// класса, что и остальной аудит (собственного права под биллинг
		// в migrations/V025__iam.sql не заведено, выдумывать его в
		// обход IAM-схемы здесь нельзя).
		r.With(auth.RequirePermission("audit:read", d.IamClient)).
			Get("/billing/ledger", handleLedgerBrowse(d.Postgres))
		r.With(auth.RequirePermission("audit:read", d.IamClient)).
			Get("/billing/summary", handleLedgerSummary(d.Postgres))

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

		// /v1/chat/* — BACKOFFICE_DESIGN_SPEC.md Экран 27 "Chat", ChatService
		// proxy (chat.go), backoffice-ui "Chat". Один gate chat:write на все
		// операции (список тредов/чтение/отправка) — тот же класс решения,
		// что incident:manage выше (один экран, одна административная
		// способность, не read/write расщепление).
		r.Route("/chat", func(r chi.Router) {
			r.Use(auth.RequirePermission("chat:write", d.IamClient))
			r.Get("/threads", handleListChatThreads(d.ChatClient))
			r.Get("/{partner_id}/messages", handleListChatMessages(d.ChatClient))
			r.Post("/{partner_id}/messages", handleSendChatMessage(d.ChatClient))
		})

		// GET /v1/ops/snapshot — luminous-hugging-charm.md Ф8, плоский HTTP
		// прокси (ops.go, НЕ gRPC) в ops-visibility-service, backoffice-ui
		// "Ops Health".
		r.With(auth.RequirePermission("ops:read", d.IamClient)).
			Get("/ops/snapshot", handleOpsSnapshot(d.HTTPClient, d.OpsVisibilityURL))

		// /v1/compliance/consent — BACKOFFICE_ROADMAP.md §4 "Blacklist",
		// плоский HTTP-прокси в compliance-api (compliance.go). GET без
		// gate — то же решение, что compliance-api само уже приняло для
		// своего /v1/compliance/consent (read открыт любому валидному
		// токену realm'а); POST — compliance:write, зеркалит гейт
		// compliance-api, чтобы отклонять без права здесь, а не только
		// получать 403 после похода в сеть.
		r.Get("/compliance/consent", handleConsentLookup(d.HTTPClient, d.ComplianceAPIURL))
		r.With(auth.RequirePermission("compliance:write", d.IamClient)).
			Post("/compliance/consent", handleManualConsent(d.HTTPClient, d.ComplianceAPIURL))

		// GET /v1/operators/routes — BACKOFFICE_ROADMAP.md §2, живой снимок
		// operator_route:* из Runtime Redis (operators.go). ops:read — тот
		// же класс видимости живого инфраструктурного состояния, что уже
		// гейтит /v1/ops/snapshot, не отдельное operators:read право.
		r.With(auth.RequirePermission("ops:read", d.IamClient)).
			Get("/operators/routes", handleOperatorRoutesList(d.RedisRuntime))
	})

	return r
}
