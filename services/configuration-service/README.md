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
| `handle_crud_request` | `internal/grpcserver/server.go` — `ConfigService` (`CreateVersion`/`GetActiveVersion`/`ListVersions`/`ArchiveVersion`, `platform-contracts/grpc/internal_control.proto`). Каждая ошибка сопоставляется с осмысленным gRPC status code через `storeErrToStatus` (`codes.InvalidArgument` для валидации/неизвестного `entity_type`/отсутствующего `requested_by`, `codes.NotFound` для `pgx.ErrNoRows`, `codes.Internal` иначе) — **исправлено по code review**: раньше каждая ошибка была голым `fmt.Errorf`, доходившим до Backoffice API как неразличимый `codes.Unknown` | `server_test.go` — с фейковым `Store` (Postgres-путь отдельно покрыт `store_test.go`): отклоняет отсутствующий `requested_by`/неизвестный `entity_type` (теперь также проверено для `GetActiveVersion`/`ListVersions`/`ArchiveVersion`, не только `CreateVersion`), пробрасывает ошибку валидации, счастливый путь возвращает версию; отдельные тесты проверяют конкретные `codes.InvalidArgument`/`codes.NotFound` через `status.Code(err)` |

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в `services/execution-control-service`: `protoc --go_out --go-grpc_out` против `platform-contracts/common/{enums,types,stage_contract}.proto`, `events/config_and_control.proto`, `grpc/internal_control.proto`, отдельный Go-модуль `mpp/platformcontracts`, подключённый через `replace`.

`internal/validate/schemas/*.schema.json` — **копия** `config_schemas/*.schema.json`, встроена через `go:embed` (Go не может embed-ить файлы за пределами директории пакета, объявляющего директиву, — поэтому копия лежит рядом с `validate.go`, не на верхнем уровне сервиса). Источник истины остаётся `config_schemas/` — эта копия должна обновляться вручную при изменении оригинала; автоматической синхронизации в этой сессии не заведено (см. "Что НЕ реализовано").

**Исправлено по code review:** сервис изначально не компилировался — `//go:embed schemas/*.json` в `internal/validate/validate.go` указывал на директорию `internal/validate/schemas/`, а реальные файлы лежали в `services/configuration-service/schemas/` (на уровень выше пакета, `go:embed` не может ссылаться за пределы своей директории через `../`). Схемы перенесены под `internal/validate/schemas/`. Отдельно `compiler.AddResource(url, doc)` передавал уже распарсенный `any` вместо `io.Reader`, ожидаемого текущей версией `santhosh-tekuri/jsonschema/v5` — заменено на `compiler.AddResource(url, bytes.NewReader(raw))`. Оба бага означали, что `internal/validate`/`internal/store`/`internal/grpcserver`/`cmd` никогда не собирались в этом состоянии, несмотря на предыдущую версию этого README, утверждавшую обратное — сейчас `go build ./... && go test ./...` реально проходит (перепроверено).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **`schemas/*.schema.json` — ручная копия**, не symlink и не auto-sync с `config_schemas/`. Если оригинал изменится (Главный агент правит `config_schemas/`, это не в его границах правки по `development_plan.md`, но это общий read-only справочник, не файл конкретного агента), эта копия молча устареет — нет CI-проверки расхождения.
* **Вычисление next version** — было (неверно) `SELECT COALESCE(MAX(version),0)+1 ... FOR UPDATE`, что PostgreSQL вообще не разрешает (`FOR UPDATE` нельзя сочетать с агрегатной функцией — реальная ошибка SQL, не просто гонка) и что дополнительно не сериализовало бы конкурентные `CreateVersion` для совершенно новой сущности (`FOR UPDATE` блокирует только уже существующие строки — CODE_REVIEW.md finding). Исправлено на `pg_advisory_xact_lock(hashtext(entity_type||':'||entity_id))` перед вычислением следующей версии — сериализует конкурентные создания той же сущности, включая самую первую версию. Проверено `TestConcurrentCreateOnBrandNewEntitySerializesVersions` — 10 конкурентных `CreateImmutableVersionAndOutbox` для одной новой сущности реально получают 10 разных последовательных версий без коллизии.
* **`page_token`** — простая курсорная пагинация по `id` (не opaque/подписанный токен) — подходит для внутреннего API, не проверялась на устойчивость к гонкам при удалении строк между страницами (архивирование не удаляет строки, так что это не должно происходить на практике).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.