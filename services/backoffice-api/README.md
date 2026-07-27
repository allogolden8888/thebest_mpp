# Backoffice API

**Основание:** `development_plan.md` — Субагент 1. Реализует `service_internal_methods.md` §7.3 целиком: `handle_config_crud`, `handle_execution_control_override`, `handle_force_scheduler_command`, `handle_dlq_browse`, `handle_replay_request`, `handle_reconciliation_browse`, `handle_report_query`.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, не псевдокод.

```bash
brew services start postgresql@17
xattr -d com.apple.quarantine /opt/homebrew/bin/clickhouse
clickhouse server -- --path=/tmp/clickhouse-data --listen_host=127.0.0.1 &
cd services/backoffice-api
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §7.3

| Метод | Где | Как проверено |
|---|---|---|
| `handle_config_crud` | `internal/httpapi/config.go` — 4 маршрута (`POST /v1/config/versions`, `GET /v1/config/versions/active`, `GET /v1/config/versions`, `POST /v1/config/versions/archive`), gRPC-проксирование в `ConfigService` (`CreateVersion`/`GetActiveVersion`/`ListVersions`/`ArchiveVersion`) | `internal/httpapi/router_test.go` (`TestHandleConfigCreateVersionUsesRequestedByFromJWT` — реальный HTTP → chi → gRPC round-trip через **bufconn** (in-process transport) с фейковой реализацией `ConfigServiceServer`, проверяет, что `requested_by` берётся из JWT, не из тела запроса) |
| `handle_execution_control_override` | `internal/httpapi/executioncontrol.go` — `POST /v1/execution-control/override` (`ApplyOverride`), `POST /v1/execution-control/override/clear` (`ClearOverride`) | `router_test.go` (`TestHandleExecutionControlApplyOverrideEndToEnd`, bufconn + фейковый `ExecutionControlServiceServer`) |
| `handle_force_scheduler_command` | `internal/kafkaio/publisher.go` `BuildCriticalCommand`/`PublishCriticalCommand` (`scheduler.critical.commands`, `task_type` ограничен `FORCE_TIMEOUT`/`FORCE_RETRY`), `internal/httpapi/scheduler.go` — `POST /v1/scheduler/force-command` | `kafkaio/publisher_test.go` (чистая функция `BuildCriticalCommand`) + `router_test.go` (`TestHandleForceSchedulerCommandRejectsUnknownTaskType` — 400 на неизвестный `task_type`, защита от открытого редиректа в Kafka) |
| `handle_dlq_browse` | `internal/store/postgres.go` `DlqBrowse` (`messaging.dlq_record`), `internal/httpapi/dlq.go` — `GET /v1/dlq?stage_name=&replay_status=&limit=&offset=` | `internal/store/postgres_test.go` (реальный PostgreSQL) |
| `handle_replay_request` | `internal/httpapi/replay.go` — `POST /v1/replay`, gRPC-проксирование в `ReplayService.RequestReplay` | `router_test.go` (`TestHandleReplayRequestRejection`, bufconn + фейковый `ReplayServiceServer`, проверяет проброс `rejection_reason`) |
| `handle_reconciliation_browse` | `internal/store/postgres.go` `ReconciliationBrowse` (`reconciliation.reconciliation_cases`), `internal/httpapi/reconciliation.go` — `GET /v1/reconciliation?status=&operator_id=&limit=&offset=` | `postgres_test.go` (реальный PostgreSQL) |
| `handle_report_query` | `internal/store/clickhouse.go` `Report`, `internal/httpapi/report.go` — `GET /v1/reports?partner_id=&from=&to=&stage_name=` (переиспользует схему Analytics Writer, как и Partner API) | `internal/store/clickhouse_test.go` — **реальный INSERT/SELECT на локальном ClickHouse** |

`cmd/backoffice-api/main.go` — сборка: health-сервер `:9090`, HTTP API `:8080` (chi), gRPC-клиенты к Configuration Service/Execution Control Service/Replay Service (обычная k8s-балансировка, не instance-addressed — `platform-contracts/grpc/internal_control.proto` комментарий "Backoffice API → X").

## Аутентификация/аудит

`internal/auth/jwt.go` — тот же RS256-валидатор, что `partner-api` (см. его README для деталей проверки), но здесь claim — стандартный `sub` (Keycloak OIDC subject), не кастомный `partner_id`: Backoffice API административный, не многотенантный. `sub` используется как `requested_by` в каждой аудируемой мутации (`CreateVersion`/`ArchiveVersion`/`ApplyOverride`/`ClearOverride`/`SchedulerCriticalCommand.requested_by`/`RequestReplay.requested_by`) — сервер, принимающий запрос, не может доверять `requested_by` в теле, поэтому все proxy-методы всегда перезаписывают его значением из проверенного токена.

## gRPC-проксирование — реально протестировано через bufconn, не только описание

`internal/httpapi/router_test.go` поднимает **реальные gRPC-серверы** через `google.golang.org/grpc/test/bufconn` (in-process транспорт, официальный пакет grpc-go для тестов — тот же принцип, что `InProcessServerBuilder` в Java-сервисах этой сессии) с фейковыми реализациями `ConfigServiceServer`/`ExecutionControlServiceServer`/`ReplayServiceServer`, и настоящими сгенерированными gRPC-клиентами (`grpcv1.NewConfigServiceClient` и т.д.) поверх них. Это первое использование bufconn в Go-сервисах этой сессии (остальные Go gRPC-тесты этой сессии — на стороне сервера, не клиента).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **`k8s/generate_manifests.py` резервирует `internal_grpc_port=9000` для `backoffice-api`**, но ни один документ (`service_internal_methods.md` §7.3, `service_io_contracts.md`) не специфицирует, какой gRPC-сервис должен слушать на этом порту — Backoffice API в этой реализации только вызывает чужие gRPC-сервисы (клиент), собственного gRPC-сервера не поднимает. Не реализовано — находка, не молчаливое решение; возможно порт зарезервирован для будущего внутреннего API (например, health/debug gRPC) или это шаблонное значение генератора манифестов, применённое по инерции ко всем сервисам "Мелкого Go control-plane" пула.
* **JWKS/ротация ключей** — статический RSA public key из `JWT_PUBLIC_KEY_PEM`, как и в Partner API.
* **RBAC/права доступа** — `migrations/V017__backoffice_stub.sql` явно оставляет полную ролевую модель отдельному LLD; здесь проверяется только подлинность токена, не авторизация конкретных действий (любой валидный токен может вызвать любой из 7 методов).
* **Live Kafka не проверялся** — `internal/kafkaio.Publisher` использует реальный `franz-go`, но публикация в `scheduler.critical.commands` не проверена против живого брокера (тот же общесессионный пробел).
* **OpenTelemetry-экспорт** — span'ы создаются (`internal/telemetry`, тот же паттерн, что Partner API), никуда не отправляются (`noopExporter`).
* **`ListVersions`/`DlqBrowse`/`ReconciliationBrowse`/`Report` без `total_count`** — только страница результатов, без общего количества строк по фильтру.
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков.
