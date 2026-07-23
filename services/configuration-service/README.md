# Configuration Service

**Основание:** `development_plan.md` — Субагент 1, Control plane. `services_specifictaion.md` §4.2: CRUD конфигурации, валидация, immutable version, запись configuration + outbox в одной транзакции.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. JSON Schema-валидация протестирована на реальных примерах из `config_schemas/examples/` (скопированы в тест, не выдуманы). PostgreSQL-путь протестирован против реального локального PostgreSQL 17.

```bash
cd services/configuration-service
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §3.2

| Метод | Где | Как проверено |
|---|---|---|
| `validate_config_change` | `internal/validate/validate.go` (структурная JSON Schema, Draft 2020-12) + `internal/validate/semantic.go` (семантические проверки — перенос 1:1 `config_schemas/validate_all.py::SEMANTIC_CHECKS`) | `validate_test.go`, `semantic_test.go` — реальные примеры из `config_schemas/examples/` (pipeline/routing_table/number_range/partner, valid и invalid), включая инварианты, которые чистый JSON Schema не выражает (entry_node_id → DESTINATION_RESOLUTION, POLICY.REJECTED → BILLING, range_end ≥ range_start, active_route_id ∈ routes[]) |
| `create_immutable_version` + `write_config_and_outbox` | `internal/store/store.go::CreateImmutableVersionAndOutbox` — одна PostgreSQL-транзакция (`pgx.BeginFunc`) | `store_test.go` — **реальные вставки в локальный PostgreSQL** (`migrations/V002__config_versions.sql`, `V003__config_outbox.sql`): версия инкрементируется корректно, `policy_template`/`subscriber_consent` пропускают `config_versions` (config_version_id=NULL в outbox), `ArchiveVersion` реально меняет `status` |
| `handle_crud_request` | `internal/grpcserver/server.go` — `ConfigService` (`CreateVersion`/`GetActiveVersion`/`ListVersions`/`ArchiveVersion`, `platform-contracts/grpc/internal_control.proto`) | `server_test.go` — с фейковым `Store` (Postgres-путь отдельно покрыт `store_test.go`): отклоняет отсутствующий `requested_by`/неизвестный `entity_type`, пробрасывает ошибку валидации, счастливый путь возвращает версию |

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в `services/execution-control-service`: `protoc --go_out --go-grpc_out` против `platform-contracts/common/{enums,types,stage_contract}.proto`, `events/config_and_control.proto`, `grpc/internal_control.proto`, отдельный Go-модуль `mpp/platformcontracts`, подключённый через `replace`.

`schemas/*.schema.json` — **копия** `config_schemas/*.schema.json`, встроена через `go:embed` (Go не может embed-ить файлы за пределами своего модуля). Источник истины остаётся `config_schemas/` — эта копия должна обновляться вручную при изменении оригинала; автоматической синхронизации в этой сессии не заведено (см. "Что НЕ реализовано").

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **`schemas/*.schema.json` — ручная копия**, не symlink и не auto-sync с `config_schemas/`. Если оригинал изменится (Главный агент правит `config_schemas/`, это не в его границах правки по `development_plan.md`, но это общий read-only справочник, не файл конкретного агента), эта копия молча устареет — нет CI-проверки расхождения.
* **`SELECT ... FOR UPDATE` для вычисления next version** — сериализует конкурентные CRUD-запросы на одну и ту же `(entity_type, entity_id)` через блокировку строки, но не протестировано под конкурентной нагрузкой (нет параллельного теста с двумя одновременными транзакциями).
* **`page_token`** — простая курсорная пагинация по `id` (не opaque/подписанный токен) — подходит для внутреннего API, не проверялась на устойчивость к гонкам при удалении строк между страницами (архивирование не удаляет строки, так что это не должно происходить на практике).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.