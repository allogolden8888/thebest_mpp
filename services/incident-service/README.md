# Incident Service

**Основание:** `luminous-hugging-charm.md` (12-фазный план закрытия API-пробелов), Фаза 7 — инцидент-менеджмент. `control.execution_control_audit` (`migrations/V016__execution_control_audit.sql`) уже пишет каждый override (scope, scope_id, state, reason, requested_by, expires_at) — реальные данные, но никакой концепции группировки связанных override в именованный, отслеживаемый инцидент с таймлайном и постмортемом не было. `incident.*` (`migrations/V028__incident.sql`) — эта концепция, реализованная, а не отложенная дальше. Мирроит структуру `services/iam-service` (Ф0) и `services/credential-issuer-service` (Ф1) — тот же small Go control-plane service паттерн этой сессии.

**Статус:** реально компилируется и тестируется — `go build ./... && go vet ./... && go test ./... -race`, **23/23 теста проходят** (14 `grpcserver` — фейковый `Store`, валидация/маппинг ошибок в gRPC-коды; 9 `store` — реальный round-trip против локального PostgreSQL 17, включая тест, который вставляет строку напрямую в `control.execution_control_audit` и подтверждает, что `TimelineForIncident` реально читает чужую схему, не просто компилируется).

```bash
brew services start postgresql@17   # если ещё не запущен
cd migrations && for f in V0*.sql; do psql postgresql://localhost:5432/mpp -v ON_ERROR_STOP=1 -f "$f"; done
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/incident-service
go build ./... && go vet ./... && go test ./... -race
```

## Модель

`migrations/V028__incident.sql`:

* `incident.incidents` — `id, title, severity (LOW/MEDIUM/HIGH/CRITICAL), status (OPEN/RESOLVED, default OPEN), opened_by/at, resolved_by/at, postmortem_notes`. Один инцидент — одна строка, без отдельной audit-таблицы поверх: `opened_at`/`resolved_at` уже сами по себе аудит-след переходов статуса (тот же принцип, что `iam.staff_role_assignments` не удаляет строку на отзыв — состояние и история читаются из одной таблицы).
* `incident.incident_notes` — `id, incident_id, author, note, created_at`. Заметка сама по себе IS событие таймлайна/коллаборации — не нужна отдельная audit-таблица сверху (в отличие от `iam.staff_role_assignments`/`credentials.issued_secrets`, где мутация и audit-запись — разные строки в разных таблицах одной транзакции; здесь `AddNote` — одна вставка).
* `control.execution_control_audit` получает nullable `incident_id BIGINT` + частичный индекс `WHERE incident_id IS NOT NULL` (таймлайн-запрос не сканирует строки без привязки к инциденту — подавляющее большинство). **Намеренно без FOREIGN KEY через границу схем**: `incident.incidents` принадлежит этому сервису, `control.execution_control_audit` — `execution-control-service`, независимо разворачиваемые сервисы с разными владельцами схем. Тот же контраст, что уже виден в кодовой базе между `iam.staff_role_assignments.role_id` (FK на `iam.roles.id` — тот же владелец схемы, FK безопасен) и `billing.reconciliation_audit`/`messaging.replay_audit` (ни то ни другое не несёт FK за пределы собственной таблицы) — жёсткий FK здесь сцепил бы DDL двух сервисов так, как нигде больше в этой кодовой базе не сделано намеренно.

## gRPC-контракт

`platform-contracts/grpc/incident.proto` — `IncidentService`, тот же стиль синхронного request/response RPC, что `IamService`/`ExecutionControlService`:

* **`OpenIncident(title, severity, opened_by) -> Incident`** — первый шаг разбора, status инициализируется `OPEN`.
* **`ListIncidents(status?) -> Incident[]`** — список для экрана `backoffice-ui` "Incidents", отсортирован `opened_at DESC`. Пустой `status` — все инциденты; `OPEN`/`RESOLVED` — фильтр.
* **`GetIncident(incident_id) -> IncidentDetail`** — карточка ВКЛЮЧАЯ таймлайн (`TimelineEntry[]` — проекция строк `control.execution_control_audit`, где `incident_id` совпадает, cross-schema чтение, см. ниже) и заметки (`IncidentNote[]`).
* **`AddNote(incident_id, author, note) -> IncidentNote`** — коллаборация/таймлайн.
* **`ResolveIncident(incident_id, resolved_by, postmortem_notes) -> Incident`** — status → `RESOLVED`, `resolved_by`/`resolved_at` проставляются сервером. `postmortem_notes` **обязателен, непустой** (после `strings.TrimSpace`) — закрытый без разбора причин инцидент обесценивает саму идею трекинга, это настоящая валидация (`codes.InvalidArgument`), не просьба. Повторное закрытие уже `RESOLVED` инцидента — `codes.FailedPrecondition` (`store.ErrAlreadyResolved`), не тихий no-op и не `Internal`.

**Cross-schema чтение таймлайна, не новая gRPC-зависимость:** `store.TimelineForIncident` читает `control.execution_control_audit` напрямую из Postgres — тот же паттерн прямого чтения чужой схемы, что `backoffice-api` уже использует для DLQ/reconciliation/audit browse (`services/backoffice-api/internal/store/postgres.go` package doc, `AuditBrowse`) и `credential-issuer-service` для `config.config_versions` (`LookupCredentialRef`). Этот сервис НЕ вызывает `execution-control-service` по gRPC ни для чтения, ни для записи `incident_id` — линковку `ApplyOverrideRequest.incident_id -> control.execution_control_audit.incident_id` делает сам `execution-control-service` (см. его README).

Регенерация protobuf (тот же паттерн, что `iam-service/README.md`):

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/incident-service
rm -rf internal/proto/gen && mkdir -p internal/proto/gen
protoc --proto_path=../../platform-contracts \
  --go_out=internal/proto/gen --go-grpc_out=internal/proto/gen \
  grpc/incident.proto
```

Не нужны `common/enums.proto` или что-либо ещё — `incident.proto` self-contained (только `google/protobuf/timestamp.proto`), тот же вывод, что `credential-issuer-service` сделал для `credentials.proto` (не завёл зависимость на `internal_control.proto`, см. `git show 7301117`).

## Реальные проверки, не только компиляция

* **`store_test.go`** — реальный round-trip против локального Postgres: `OpenIncident` → `GetIncident` видит `status=OPEN`; `ListIncidents` фильтрует по статусу корректно (`RESOLVED` не протекает в фильтр `OPEN` и наоборот); `AddNote`/`ListNotes` — хронологический порядок (`created_at ASC`); `ResolveIncident` реально проставляет `resolved_by/resolved_at/postmortem_notes` и переводит статус; повторное закрытие — `ErrAlreadyResolved`, не тихий успех; неизвестный `incident_id` — `ErrIncidentNotFound` для `GetIncident`/`AddNote`/`ResolveIncident`. **`TestTimelineForIncidentFindsCrossSchemaAuditRows`** — вставляет две строки напрямую в `control.execution_control_audit` с `incident_id`, заполненным как это делал бы `execution-control-service.PersistOverrideAudit`, плюс одну контрольную строку БЕЗ `incident_id`, и подтверждает, что `TimelineForIncident` находит ровно две привязанные строки в правильном хронологическом порядке — доказывает, что cross-schema чтение реально работает, не просто компилируется.
* **`grpcserver_test.go`** — валидация обязательных полей (`title`/`severity`/`opened_by` для `OpenIncident`, недопустимый `severity` вне `LOW/MEDIUM/HIGH/CRITICAL`, `author`/`note` из одних пробелов для `AddNote`, `postmortem_notes` из одних пробелов для `ResolveIncident`), маппинг `store.ErrIncidentNotFound`/`store.ErrAlreadyResolved` в `codes.NotFound`/`codes.FailedPrecondition`, композиция `GetIncident` (карточка + timeline + notes из трёх независимых вызовов fake `Store`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся, ни разу не запущено в реальном k8s-кластере** — та же оговорка, что у остальных сервисов сессии на момент их первого среза. `k8s/generate_manifests.py`/`services/backoffice-api`/`services/backoffice-ui` намеренно не тронуты в этом срезе — оркестрирующая сессия сводит онбординг этого сервиса вместе с Ф8 (`ops-visibility-service`) отдельным шагом, чтобы не было двух параллельных агентов, редактирующих одни и те же общие файлы одновременно.
* **`backoffice-ui` раздел "Incidents"** (открыть/закрыть, связанные overrides, таймлайн, постмортем; право `incident:manage`, уже заведённое в `iam.roles`/`iam.permissions` `migrations/V025__iam.sql`) — не реализован в этом срезе, тот же намеренный вынос за скоуп, что и выше.
* **Нет full-text/regex поиска по инцидентам** — `ListIncidents` фильтрует только по `status`, поиск по `title`/severity/дате не реализован; если понадобится, расширение метода механическое.
* **`incident_id` на override не проставляется автоматически** — `execution-control-service.ApplyOverride` принимает опциональный `incident_id` в запросе (см. `internal_control.proto`/README того сервиса), но никакого автоматического "текущий активный инцидент для этого scope" не существует — вызывающий (`backoffice-ui`, когда появится) должен явно передать `incident_id`, если override делается в контексте конкретного разбора.
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков (`incidents_opened_total` и т.п.).
