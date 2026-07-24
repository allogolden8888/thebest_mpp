# Replay Service

**Основание:** `development_plan.md` — Субагент 1. Реализует `service_internal_methods.md` §7.4 целиком: единственный вызывающий — Backoffice API `handle_replay_request` (gRPC `ReplayService.RequestReplay`, `platform-contracts/grpc/internal_control.proto`).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, не псевдокод.

```bash
brew services start postgresql@17   # если ещё не запущен
cd services/replay-service
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §7.4

| Метод | Где | Как проверено |
|---|---|---|
| `load_dlq_record` | `internal/store/store.go` `LoadDlqRecord` — читает `messaging.dlq_record`, дополнительно разбирает `original_command` как `StageExecuteCommand`, чтобы достать `message_ttl` | `store_test.go` — **реальная вставка/чтение в локальном PostgreSQL 17** |
| `check_ttl` | `internal/core/checks.go` `CheckTTL` (чистая функция) | `checks_test.go` — валиден/истёк/не задан TTL |
| `check_idempotency` | `internal/core/checks.go` `CheckIdempotency` — небезопасно, если `replay_status = 'replayed'` | `checks_test.go` + `store_test.go` (`TestMarkReplayedThenIdempotencyCheckIsUnsafe` — реальный round-trip через PostgreSQL) |
| `check_billing_side_effect` | `internal/core/checks.go` `CheckBillingSideEffect` — применяется только к стадии BILLING, проверяет наличие `charge_id` в `billing.billing_ledger` (`store.ChargeExistsInLedger`) | `checks_test.go` + `store_test.go` (`TestChargeExistsInLedgerRealCheck` — реальная вставка/чтение) |
| `check_delivery_ambiguity` | `internal/core/checks.go` `CheckDeliveryAmbiguity` — применяется только к стадии DELIVERY, проверяет `dlr.dlr_correlation` (`store.DlrCorrelationExists`) | `checks_test.go` |
| `republish` | `internal/kafkaio/republish.go` `Publisher.Republish` — реальный `franz-go` продюсер, тот же `stage.*`-маппинг, что в scheduler-critical-sweep | `republish_test.go` (чистые части: `StageTopic`, `DecodeOriginalCommand`) |
| `write_audit` | `internal/store/store.go` `WriteAudit` — вставка в `messaging.replay_audit` (`checks_passed` JSONB, `outcome`, `target_topic`) | `store_test.go` (`TestWriteAuditInsertsRealRow` — реальная вставка/чтение) |
| gRPC `ReplayService.RequestReplay` | `internal/grpcserver/server.go` — оркестрация всех методов выше в порядке §7.4: `load_dlq_record` → `check_ttl` → `check_idempotency` → `check_billing_side_effect` → `check_delivery_ambiguity` → `republish` → `write_audit` → `mark_replayed`/`mark_expired` | не покрыт отдельным unit-тестом (см. "Что НЕ реализовано" — оркестратор целиком зависит от реального PostgreSQL+Kafka, интерфейсы `DlqLoader`/`LedgerChecker`/`CorrelationChecker`/`AuditWriter` в файле подготовлены для фейковой реализации, но фейки не написаны в этом срезе) |

`cmd/replay-service/main.go` — сборка: health-сервер на `:9090` (готов сразу после старта Kafka producer и PostgreSQL пула), gRPC-сервер `ReplayService` на `:9000`.

## Реальная сквозная проверка контрактов, не только описание

`internal/proto/gen/` компилирует те же `platform-contracts/common/{enums,types,stage_contract}.proto`, `platform-contracts/events/{dlq_record,config_and_control}.proto`, `platform-contracts/grpc/internal_control.proto` (`ReplayService`) через `protoc --go_out --go-grpc_out` — реальные Go-типы. Сгенерированный код — отдельный Go-модуль (`mpp/platformcontracts`), подключённый через `replace` в `go.mod`.

**Регенерация protobuf** (если `platform-contracts/*.proto` изменятся):

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/replay-service
rm -rf internal/proto/gen && mkdir -p internal/proto/gen
protoc --proto_path=../../platform-contracts \
  --go_out=internal/proto/gen --go-grpc_out=internal/proto/gen \
  common/enums.proto common/types.proto common/stage_contract.proto \
  events/dlq_record.proto events/config_and_control.proto grpc/internal_control.proto
```

## PostgreSQL — что реально прогнано

`internal/store/store_test.go` подключается к `postgres://localhost:5432/mpp` (переопределяется `REPLAY_SERVICE_TEST_DSN`) и вставляет/читает обратно реальные строки в `messaging.dlq_record`, `billing.billing_ledger`, `messaging.replay_audit`. Если `mpp`-база недоступна, тесты пропускаются (`t.Skip`), не падают.

## Секреты — сверено с `k8s/generate_manifests.py SECRET_DEPENDENCIES`

`replay-service: ["postgresql"]`. Использован (`POSTGRES_HOST/PORT/DB/USER/PASSWORD`). Kafka-брокеры (`KAFKA_BOOTSTRAP_SERVERS`) читаются как обычная конфигурация, не секрет — тот же приём, что в остальных Go-сервисах этой сессии.

## Открытый вопрос

* **`check_idempotency` в §7.4 описан как "подтверждение, что republish безопасен по контракту §6"**, без указания конкретного механизма. Реализовано как проверка `replay_status != 'replayed'` — не даёт реплеить дважды один и тот же `stage_execution_id`. Если "контракт §6" подразумевает что-то более широкое (например, сверку с текущим состоянием пайплайна в `execution.pipeline_state`), это не покрыто в этом срезе — находка, не молчаливое решение.
* **`republish` не увеличивает `attempt`** — `original_command` публикуется как есть, без модификации счётчика попыток. `service_internal_methods.md` §7.4 не специфицирует поведение `attempt` для ручного replay-пути (в отличие от автоматического retry в Scheduler), поэтому оставлено как есть — вниз по потоку stage-сервис увидит тот же `attempt`, что был на момент попадания в DLQ.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon в этой песочнице.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go` клиент, компилируется, но live-брокер не поднимался ни для одного сервиса в этой сессии.
* **gRPC-оркестратор (`internal/grpcserver/server.go`) не покрыт unit-тестами** — интерфейсы (`DlqLoader`, `LedgerChecker`, `CorrelationChecker`, `AuditWriter`) специально минимальны и достаточны для фейковых реализаций в тестах, но фейки не написаны в этом срезе; проверены только его составные части (`internal/core`, `internal/store`, `internal/kafkaio` по отдельности).
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков (`replays_total`, `replays_rejected_total` и т.п.).