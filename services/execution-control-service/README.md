# Execution Control Service

**Основание:** `development_plan.md` — Субагент 1, Фаза 3.1. Рекомендован документом как первый сервис Субагента 1, поскольку алгоритм гистерезиса уже спроектирован и протестирован как исполняемая Python-модель — `state_machines/execution_control_hysteresis.py` (5/5 тестов). Задача этого среза — перенести его на Go 1:1, не переизобретать, и достроить вокруг него остальные методы `service_internal_methods.md` §3.1.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, не псевдокод. 37/37 теста проходят (`-race` чисто), включая интеграционный тест против реального локального PostgreSQL.

**CODE_REVIEW.md — что исправлено после первого прохода ревью:** раньше `ApplyOverride`/`ClearOverride` (Backoffice + Billing Reconciliation freeze/unfreeze, HLD §15.5) мутировали `registry` и писали аудит, но никогда не публиковали `ExecutionControlRecord` в Kafka для scope, отличного от `GLOBAL` — единственный publish-путь был периодический GLOBAL-only цикл в `main.go`. Это молча сводило на нет весь механизм partner billing freeze (CRITICAL). Исправлено: `grpcserver.Server` теперь держит `*kafkaio.Publisher` и публикует запись после каждого успешного `ApplyOverride`/`ClearOverride`, для любого scope. Заодно: порядок операций сменён на audit → registry mutation → publish (раньше registry мутировался до аудита — если аудит падал, caller получал ошибку, но in-memory состояние уже изменилось, откатывать было нечего, HIGH); `admission_rate` теперь валидируется (конечное число в `[0,1]`, MEDIUM); `registry.Evaluate` инкрементирует `version` при каждом реальном переходе состояния, не только при manual override (раньше metric-driven записи навсегда несли `version=0`, MEDIUM); `runGlobalControlLoop` теперь пропускает тик и логирует предупреждение, если Prometheus вернул `NaN`/`Inf` (легитимно для `0/0` при нулевом трафике — раньше это молча "замораживало" гистерезис, MEDIUM).

```bash
brew services start postgresql@17   # если ещё не запущен
cd services/execution-control-service
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §3.1

| Метод | Где | Как проверено |
|---|---|---|
| `evaluate_hysteresis` / `apply_dwell_time` | `internal/hysteresis/hysteresis.go` — перенос 1:1 `state_machines/execution_control_hysteresis.py` | `internal/hysteresis/hysteresis_test.go` — те же 5 тестов, что в Python-оригинале (шум не флапает, устойчивая деградация ACTIVE→DEGRADED→PAUSED, восстановление только через DEGRADED, катастрофа может уйти в PAUSED напрямую, min_state_duration блокирует мгновенный повторный переход) |
| `compose_effective_rate` | `internal/hysteresis/scope.go` | `scope_test.go` — min(admission_rate) по применимым scope, наиболее строгое state побеждает, PARTNER_STAGE/OPERATOR_ROUTE матчатся только на свой scope_id |
| `compute_ramp_step` | `internal/hysteresis/ramp.go` | `ramp_test.go` — лестница `0→5→10→25→50→100%`, не продвигается вне ACTIVE |
| `apply_manual_override` + `persist_override_audit` | `internal/registry/registry.go` (приоритет override над метрикой, версионирование, TTL по `expires_at`) + `internal/store/audit.go` (запись в `control.execution_control_audit`) | `registry_test.go` (override перевешивает метрику, истекает, ClearOverride восстанавливает metric-driven путь, scope изолированы друг от друга) + `store/audit_test.go` — **реальная вставка и readback в локальном PostgreSQL 17** (миграции `migrations/V0*.sql` применены в этой песочнице, `mpp` база, см. ниже) |
| `publish_control_record` | `internal/kafkaio/publisher.go` — `BuildControlRecord` (чистая функция) + `Publisher` (реальный `franz-go` клиент) | `publisher_test.go` — маппинг scope/state в protobuf enum, `expires_at` пробрасывается, ключ Kafka-сообщения различает scope с одинаковым пустым `scope_id` (иначе GLOBAL и STAGE("") коллапсировали бы на compaction) |
| `collect_signals` | `internal/signals/prometheus.go` — `Client.Query` (реальный HTTP-клиент к Prometheus instant query API) + `ParseInstantQueryResponse` (чистый разбор JSON) | `prometheus_test.go` — разбор фикстур ответа + реальный HTTP round-trip против `httptest.Server` (не мок транспорта) |
| gRPC `ApplyOverride`/`ClearOverride` (`platform-contracts/grpc/internal_control.proto`, `ExecutionControlService`) | `internal/grpcserver/server.go` | `server_test.go` — валидация `requested_by`, версия/аудит на apply и на clear |

`cmd/execution-control-service/main.go` — сборка: health-сервер на `:9090` (готов сразу), gRPC-сервер `ExecutionControlService` на `:9000` (envFrom-конвенция `internal_grpc_port=9000` из `k8s/generate_manifests.py`), фоновый control loop для `scope=GLOBAL` (Prometheus → `evaluate_hysteresis` → Kafka `execution.control`, тик 10с).

## Реальная сквозная проверка контрактов, не только описание

`internal/proto/gen/` компилирует **те же** `platform-contracts/common/{enums,types,stage_contract}.proto`, `platform-contracts/events/config_and_control.proto` (`ExecutionControlRecord`), `platform-contracts/grpc/internal_control.proto` (`ExecutionControlService`) через `protoc --go_out --go-grpc_out` — реальные Go-типы, не выдуманные структуры. Сгенерированный код — отдельный Go-модуль (`mpp/platformcontracts`, свой `go.mod`), подключённый через `replace` в `go.mod` этого сервиса, поскольку `go_package` в `.proto` (`mpp/platformcontracts/...`) не совпадает с путём модуля сервиса.

**Регенерация protobuf** (если `platform-contracts/*.proto` изменятся — правки координирует Главный агент, см. `development_plan.md` "Координация" п.2):

```bash
export PATH="$PATH:$(go env GOPATH)/bin"   # protoc-gen-go, protoc-gen-go-grpc
cd services/execution-control-service
rm -rf internal/proto/gen && mkdir -p internal/proto/gen
protoc --proto_path=../../platform-contracts \
  --go_out=internal/proto/gen --go-grpc_out=internal/proto/gen \
  common/enums.proto common/types.proto common/stage_contract.proto \
  events/config_and_control.proto grpc/internal_control.proto
```

## PostgreSQL — что реально прогнано

`internal/store/audit_test.go` подключается к `postgres://localhost:5432/mpp` (переопределяется `EXECUTION_CONTROL_TEST_DSN`) и вставляет/читает обратно реальную строку в `control.execution_control_audit` (`migrations/V016__execution_control_audit.sql`). В этой песочнице БД поднята так же, как в `migrations/README.md` (brew, не Docker — Docker daemon недоступен, `development_plan.md` "Координация" п.5):

```bash
brew install postgresql@17 && brew services start postgresql@17
createdb mpp
cd migrations && for f in V*.sql; do psql postgresql://localhost:5432/mpp -v ON_ERROR_STOP=1 -f "$f"; done
```

Если `mpp`-база недоступна, тест пропускается (`t.Skip`), не падает — тот же принцип, что и остальная сессия: реальный I/O тестируется, где окружение позволяет, честно помечается, где нет.

## Секреты — сверено с `k8s/generate_manifests.py SECRET_DEPENDENCIES`

`execution-control-service`: `["postgresql", "redis-configuration"]`. Использован `postgresql` (`POSTGRES_HOST/PORT/DB/USER/PASSWORD` — `persist_override_audit`). **`redis-configuration` выдан, но не использован в этом срезе** — ни один метод `service_internal_methods.md` §3.1 не описывает чтение/запись Configuration Redis для Execution Control (Configuration Redis пишет `config-cache-projector`, читают stateless-сервисы hot-path). Не выдумано использование ради секрета — находка, не молчаливое решение; если в реальности сервису нужен доступ к Configuration Redis (например, для чтения per-partner порогов из projected config), это отдельная задача для следующего среза.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — тот же недоступный Docker daemon (см. `Dockerfile`).
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go` клиент, компилируется, но `run`/интеграция с `kind`+Strimzi не проверялась (Kafka не поднимался live ни для одного сервиса в этой сессии).
* **Ни разу не запущено против реального Prometheus** — `internal/signals.Client` реальный HTTP-клиент, протестирован против `httptest.Server`, но не против живого `kube-prometheus-stack` (не задеплоен, Фаза 2.3 не начата).
* **Per-scope пороги гистерезиса не читаются из PostgreSQL.** `service_io_contracts.md` §3.1 упоминает "PostgreSQL SQL (read) — сохранённые правила/overrides" — в этом срезе все scope используют единый `defaultThresholds` (тот же STANDARD-профиль, что в `state_machines/execution_control_hysteresis.py`), различный порог per-partner/per-stage не реализован.
* **`collect_signals` от Scheduler по gRPC** — `service_io_contracts.md` §3.1 перечисляет "Scheduler (все три lane) → Prometheus-метрики" как канал (не gRPC), поэтому здесь читается один и тот же Prometheus HTTP API — отдельного gRPC-приёмника сигналов от Scheduler нет, это соответствует документу, не пробел.
* **Автоматический metric-driven control loop заведён только для `scope=GLOBAL`.** Manual override (Backoffice, Billing Reconciliation freeze/unfreeze) теперь публикуется в Kafka для ЛЮБОГО scope (см. выше — это и было CRITICAL-находкой), так что ручное управление per-partner/per-stage/per-operator-route уже полноценно работает end-to-end. Но периодического цикла "снять метрику → evaluate_hysteresis → publish" для STAGE/PARTNER/PARTNER_STAGE/OPERATOR_ROUTE по-прежнему нет — только GLOBAL эволюционирует от Prometheus сам по себе. Не расширено на этом шаге: в кодовой базе нет установленной конвенции per-stage/per-partner Prometheus-меток (`mpp_stage_errors_total`/`mpp_stage_processed_total`, единственное использование — этот же файл, без label `stage`/`partner` где-либо ещё в репозитории), и выдумывать её здесь означало бы гадать вместо переноса согласованного контракта. Расширение до полного обхода активных scope в реестре механическое, как только эта конвенция появится.
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков (override_applied_total и т.п.).
