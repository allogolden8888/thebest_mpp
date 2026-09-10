# IAM Service

**Основание:** новый 12-фазный план закрытия API-пробелов платформы (`/Users/Alisher/.claude/plans/luminous-hugging-charm.md`, утверждён пользователем), Фаза 0 — Identity/RBAC/Audit, фундамент. Прямая замена заглушки `migrations/V017__backoffice_stub.sql` (`backoffice.users`) — эта таблица никем не читалась в коде (только упоминалась в комментариях/README `backoffice-api`), полная RBAC-модель была явно отложена на "отдельный LLD Backoffice API", которого не существовало. `iam.*` (`migrations/V025__iam.sql`) — этот LLD, реализованный, а не отложенный дальше.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./... -race`, **14/14 тестов проходят** (9 `grpcserver` — фейковый `Store`, валидация/маппинг ошибок в gRPC-коды; 5 `store` — реальный round-trip против локального PostgreSQL 17, включая readback `iam.identity_audit`).

```bash
brew services start postgresql@17   # если ещё не запущен
cd migrations && for f in V0*.sql; do psql postgresql://localhost:5432/mpp -v ON_ERROR_STOP=1 -f "$f"; done
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/iam-service
go build ./... && go test ./... -race
```

## Модель

`migrations/V025__iam.sql`:

* `iam.permissions` / `iam.roles` / `iam.role_permissions` — классический RBAC many-to-many. Затравочные права — ровно то множество, что называет план Ф0 (`audit:read`, `credentials:issue`, `incident:manage`, `compliance:write`, `support:trace`, `ops:read`), плюс декомпозиция существующих деструктивных операций `backoffice-api`, которые раньше были за одной бинарной ролью `backoffice-admin` (`config:write`, `execution-control:write`, `scheduler:force`, `replay:request`, `dlq:manage`) — без этой декомпозиции `RequireRole("backoffice-admin")` было бы нечего "расширять до конкретных прав", сами права должны для начала существовать как отдельные сущности.
* `iam.staff_role_assignments` — (external_id из Keycloak `sub`, role_id, granted_by/at, revoked_by/at). Строка не удаляется на отзыв (`revoked_at` вместо `DELETE`) — иначе "кто и когда имел доступ" было бы невосстановимо; текущее активное состояние читается той же таблицей (`revoked_at IS NULL`), не только через `iam.identity_audit`. Уникальный частичный индекс не даёт дважды активно назначить одну и ту же (external_id, role).
* `iam.partner_portal_users` / `iam.partner_portal_role_assignments` — заготовка под партнёрских людей-пользователей будущего портала (Фаза 3). Пустые до Фазы 3 — это не проблема, тот же принцип, что `policy.subscriber_consent` был заведён задолго до `compliance-api`.
* `iam.identity_audit` — append-only, тот же формат, что остальные `*_audit` таблицы этой сессии (`BIGSERIAL id, actor TEXT, created_at`).
* **`backoffice-admin` — суперроль с полным набором прав**, seed-заполнена всеми затравочными правами. Обратная совместимость: существующие Keycloak-токены с realm-ролью `backoffice-admin` продолжают работать без миграции ролей на стороне Keycloak (координация с владельцем IdP — вне репозитория, то же допущение, что в начале плана).

## gRPC-контракт

`platform-contracts/grpc/iam.proto` — `IamService`, тот же стиль синхронного request/response RPC, что `ExecutionControlService` (`internal_control.proto`), намеренно не переизобретён:

* **`CheckPermission(external_id, permission) -> (allowed, roles[])`** — вызывается middleware'ом `backoffice-api` (и позже `partner-self-service-api`, Фаза 3) на КАЖДЫЙ мутирующий запрос. Проверяет не только активное назначение роли, но и `iam.staff_accounts.active`, поэтому деактивация сотрудника немедленно закрывает доступ даже для ещё не истёкшего JWT. **Fail-closed по контракту**: недоступность IAM Service должна трактоваться вызывающим как запрет, не как молчаливое разрешение — сервис, способный поставить на паузу весь трафик платформы, не может по умолчанию проваливаться в "открыто" (тот же CRITICAL класс находки, который вся эта фаза закрывает).
* `ListRoles` / `ListStaffAssignments` / `AssignStaffRole` / `RevokeStaffRole` — административные RPC для экрана `backoffice-ui` "Access Control" ("у кого какой доступ" — видимость, которой сейчас нет вообще).

Регенерация protobuf (тот же паттерн, что `execution-control-service/README.md`):

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/iam-service
rm -rf internal/proto/gen && mkdir -p internal/proto/gen
protoc --proto_path=../../platform-contracts \
  --go_out=internal/proto/gen --go-grpc_out=internal/proto/gen \
  grpc/iam.proto
```

## Реальные проверки, не только компиляция

* **`store_test.go`** — реальный round-trip против локального Postgres: назначение роли → `CheckPermission` видит новое право → отзыв → `CheckPermission` снова `false`; изоляция прав между узкими ролями (`ops-viewer` не несёт `audit:read`); повторное активное назначение той же (external_id, role) даёт `ErrAlreadyAssigned` (уникальный индекс, не тихий дубль); назначение несуществующей роли — `ErrRoleNotFound`; `AssignStaffRole`/`RevokeStaffRole` реально пишут `iam.identity_audit` в ОДНОЙ транзакции с мутацией — либо оба применяются, либо ни один (в отличие от `execution-control-service`, где registry — in-memory, а audit — Postgres, и гонка между ними структурно возможна и явно задокументирована там; здесь оба в одном Postgres, гонка невозможна).
* **`server_test.go`** — валидация обязательных полей (пустой `external_id`/`permission`/`role`/`granted_by` → `InvalidArgument`), маппинг `store.ErrRoleNotFound`/`ErrAlreadyAssigned` в `codes.NotFound`/`codes.AlreadyExists`, а не голый `codes.Unknown`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся, ни разу не запущено в реальном k8s-кластере** — та же оговорка, что у остальных сервисов сессии на момент их первого среза.
* **`CheckPermission` не кеширует результат** — каждый вызов идёт в Postgres через gRPC синхронно, тот же принцип минимализма, что у `ExecutionControlService.ApplyOverride` ("без кеширования на этом шаге" явно зафиксировано в `iam.proto`). Под нагрузкой это отдельная, отдельно оцениваемая оптимизация (in-proc TTL-кеш на стороне `backoffice-api`, не здесь).
* **Keycloak realm/роли не заводятся этим репозиторием** — явное допущение плана: администрирование ролей в самом Keycloak (создание `audit:read` как отдельной назначаемой realm-роли и т.п.) координируется с владельцем IdP вне кода. `iam.roles`/`iam.permissions` — модель ролей ВНУТРИ платформы (кто что может сделать через API), не модель Keycloak realm-ролей как таковых; связь между ними — `external_id` (Keycloak `sub`), не имя realm-роли.
* **k8s-онбординг (`k8s/generate_manifests.py`, `infra/secrets/`) — отдельный коммит**, не в этом.
