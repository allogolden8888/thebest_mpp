# Replay Service

**Основание:** `development_plan.md` — Субагент 1. Реализует `service_internal_methods.md` §7.4 целиком: единственный вызывающий — Backoffice API `handle_replay_request` (gRPC `ReplayService.RequestReplay`, `platform-contracts/grpc/internal_control.proto`).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, `-race` чисто.

**CODE_REVIEW.md — что исправлено после первого прохода ревью:**
* **CRITICAL — TOCTOU-гонка позволяла двойной replay.** `load_dlq_record` (SELECT) и `MarkReplayed` (UPDATE, только после успешного republish) были двумя отдельными шагами без атомарного claim между ними — два конкурентных `RequestReplay` для одного `stage_execution_id` оба видели `replay_status='pending'`, оба проходили `check_idempotency`, оба республиковали (DELIVERY — физический дубль SMS абоненту, BILLING — двойное списание). Исправлено: новый `internal/store.Store.ClaimForReplay` атомарно переводит `pending -> in_progress` через `UPDATE ... WHERE replay_status='pending' ... RETURNING` — только один конкурентный вызов побеждает. Потребовалась миграция `migrations/V018__dlq_record_replay_in_progress.sql` (расширяет CHECK на новое значение `in_progress`, аддитивно, не ломает существующих читателей таблицы). Регрессионный тест против **реального Postgres** — `internal/store/store_test.go` `TestClaimForReplayConcurrentClaimsOnlyOneWins` (20 конкурентных горутин, ровно 1 claim) — и на уровне orchestration — `internal/grpcserver/server_test.go` `TestRequestReplayConcurrentCallsOnlyOnePublishes`.
* **HIGH — `MarkReplayed`-failure раньше молча открывал повторный replay.** Ошибка отбрасывалась (`_ = s.dlq.MarkReplayed(...)`), запись оставалась `'pending'` (в старой модели без claim) — открытая дверь для повторной обработки без всякой гонки. Теперь запись уже `in_progress` (после claim), и при сбое `MarkReplayed` остаётся `in_progress` — fail-safe (требует ручного вмешательства через прямой SQL), не fail-open. Ответ вызывающему всё равно `Accepted=true` — сообщение реально ушло в Kafka, врать об этом было бы хуже.
* **HIGH — `republish` игнорировал `execution.control`.** Прошедшая все safety-проверки команда публиковалась напрямую на `stage.*` без единой проверки текущей паузы — например, намеренная заморозка `PARTNER_STAGE=BILLING,PAUSED` во время инцидента не останавливала republish backlog'а BILLING-стадийных DLQ-записей тем же оператором, разбирающим тот же инцидент. Новый `internal/controlsnapshot` (та же чистая структура данных, что в scheduler-critical-sweep) + `internal/kafkaio/controlconsumer.go` — консьюмер `execution.control` **без** `kgo.ConsumerGroup()` (без группы franz-go назначает клиенту ВСЕ партиции — честный full-mirror на реплику, а не разделённый по партициям, как документирует `service_io_contracts.md` "Kafka (compacted, local snapshot)"), с обработкой tombstone через разбор ключа (`ParseRecordKey`). `RequestReplay` проверяет `IsPaused(stage_name)` перед republish.
* **CRITICAL (частично) — отсутствие авторизации.** `requested_by` раньше не валидировался вообще. Теперь обязателен (пустой — отказ до любых DB/Kafka операций). Транспорт: сервис зарегистрирован в `k8s/generate_manifests.py` и деплоится в namespace `mpp` под Istio STRICT `PeerAuthentication` — mTLS обеспечен мешем. Остаток finding'а — отсутствие Istio `AuthorizationPolicy`, ограничивающей, чьи identity могут звать этот gRPC (нет ни одной в `infra/istio/` вообще, не специфично для этого сервиса) — это infra-уровневый пробел, не в периметре ответственности этого среза (`infra/` не трогается); задокументировано честно в `internal/grpcserver/server.go`, не изобретён конкурирующий с mesh app-level credential-механизм.
* **Test-quality finding закрыт**: `internal/grpcserver/server.go` (единственный orchestration-путь) раньше не имел вообще ни одного теста. `publisher` было конкретным `*kafkaio.Publisher`, что делало тестирование без живого Kafka-брокера невозможным — добавлен минимальный интерфейс `Republisher`. `server_test.go` — 11 тестов через реалистичный in-memory fake (claim/release/mark-семантика под mutex, включая конкурентный сценарий).

```bash
brew services start postgresql@17   # если ещё не запущен
cd services/replay-service
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §7.4

| Метод | Где | Как проверено |
|---|---|---|
| `load_dlq_record` | `internal/store/store.go` — `LoadDlqRecord` (read-only просмотр) и `ClaimForReplay` (атомарный claim, реальный replay-путь использует именно его — см. CODE_REVIEW.md ниже), оба разбирают `original_command` как `StageExecuteCommand`, чтобы достать `message_ttl` | `store_test.go` — **реальная вставка/чтение в локальном PostgreSQL 17**, включая конкурентный regression-тест на атомарность claim |
| `check_ttl` | `internal/core/checks.go` `CheckTTL` (чистая функция) | `checks_test.go` — валиден/истёк/не задан TTL |
| `check_idempotency` | `internal/core/checks.go` `CheckIdempotency` — небезопасно, если `replay_status = 'replayed'` | `checks_test.go` + `store_test.go` (`TestMarkReplayedThenIdempotencyCheckIsUnsafe` — реальный round-trip через PostgreSQL) |
| `check_billing_side_effect` | `internal/core/checks.go` `CheckBillingSideEffect` — применяется только к стадии BILLING, проверяет наличие `charge_id` в `billing.billing_ledger` (`store.ChargeExistsInLedger`) | `checks_test.go` + `store_test.go` (`TestChargeExistsInLedgerRealCheck` — реальная вставка/чтение) |
| `check_delivery_ambiguity` | `internal/core/checks.go` `CheckDeliveryAmbiguity` — применяется только к стадии DELIVERY, проверяет `dlr.dlr_correlation` (`store.DlrCorrelationExists`) | `checks_test.go` |
| `republish` | `internal/kafkaio/republish.go` `Publisher.Republish` — реальный `franz-go` продюсер, тот же `stage.*`-маппинг, что в scheduler-critical-sweep | `republish_test.go` (чистые части: `StageTopic`, `DecodeOriginalCommand`) |
| `write_audit` | `internal/store/store.go` `WriteAudit` — вставка в `messaging.replay_audit` (`checks_passed` JSONB, `outcome`, `target_topic`) | `store_test.go` (`TestWriteAuditInsertsRealRow` — реальная вставка/чтение) |
| gRPC `ReplayService.RequestReplay` | `internal/grpcserver/server.go` — оркестрация в порядке §7.4: `claim_for_replay` (атомарно, заменяет отдельные `load_dlq_record`+`check_idempotency`) → `check_ttl` → `check_billing_side_effect` → `check_delivery_ambiguity` → `check_execution_control` → `republish` → `write_audit` → `mark_replayed`/`mark_expired`/`release_claim` | `server_test.go` — 11 тестов через реалистичный in-memory fake (claim/release/mark-семантика), включая конкурентный сценарий; реальная атомарность claim — отдельно в `store_test.go` против Postgres |

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
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher`/`ControlConsumer` используют настоящий `franz-go` клиент, компилируются, но live-брокер не поднимался ни для одного сервиса в этой сессии.
* **Istio `AuthorizationPolicy` для replay-service** — см. CODE_REVIEW.md выше: mTLS обеспечен мешем, но ничто не ограничивает, чьи identity могут звать `RequestReplay` — инфра-уровневый пробел, не в периметре этого среза (`infra/` не трогается).
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков (`replays_total`, `replays_rejected_total` и т.п.).