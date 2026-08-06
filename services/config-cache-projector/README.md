# Config Cache Projector

**Основание:** `development_plan.md` — Субагент 1, Control plane. `service_internal_methods.md` §3.4: `on_config_change` (Kafka `config.changes`) → `write_projection` (Configuration Redis, `data_infrastructure_spec.md` §2.2).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Redis-путь протестирован против реального протокола Redis (`miniredis`).

**CODE_REVIEW.md — что исправлено:**
* **CRITICAL — `subscriber_consent` events aren't filtered out and get written into Configuration Redis.** `internal/kafkaio/consumer.go::DecodeConfigChangeEvent` только проверял `entity_id != ""`, в отличие от симметричного `consent-cache-projector`, который явно фильтрует свой путь. `data_infrastructure_spec.md §1.9c` требует проекции consent-данных в Runtime Redis, не в Configuration Redis (bootstrap-only, не рассчитана на per-subscriber объём/PII retention). Теперь `DecodeConfigChangeEvent` явно возвращает `(nil, nil)` для `SUBSCRIBER_CONSENT` — симметрично тому, как `consent-cache-projector` пропускает всё остальное.
* **HIGH — malformed/unset `entity_type` is silently accepted and projected under a bogus `unspecified` key.** Раньше `projector.entityTypeString`'s `default: "unspecified"` был единственной защитой, и ничего не мешало записи с `entity_type=UNSPECIFIED` дойти до `WriteProjection` и осесть под `config:version:unspecified:...`. Теперь `DecodeConfigChangeEvent` явно отклоняет (error) всё, что не входит в allow-list известных entity_type (см. `validConfigEntityTypes`), и `WriteProjection` сам дополнительно отказывается писать под `unspecified` (defense in depth, не единственная линия защиты).
* **HIGH — `WriteProjection` did two non-atomic Redis writes with no compensation on partial failure.** Раньше два независимых `SET` — если первый (version) проходил, а второй (current) падал по сети, читатели видели новую версию в истории, но старый current-указатель, split-brain. Теперь оба `SET` идут через `TxPipelined` (Redis `MULTI`/`EXEC`) — атомарно, либо оба применились, либо ни один.
* **Cross-cutting, CRITICAL — Kafka autocommit decoupled from processing success.** `NewConsumer` теперь передаёт `kgo.DisableAutoCommit()`. Consume-цикл (`Consumer.Run` → `processRecords`) коммитит offset только после успешного `write_projection`; retriable-ошибка (напр. Redis timeout) останавливает обработку этой партиции на этом fetch и НЕ коммитит — запись передоставляется на следующем `PollFetches`. Poison-сообщение (не парсится / неизвестный `entity_type`) коммитится (пропускается), чтобы не блокировать партицию навечно — bounded, не open-ended.
* **`/readyz` never reflects real downstream health.** `internal/health` теперь поддерживает `SetDependencyChecks` — `/readyz` реально пингует Redis (`redisClient.Ping`) и Kafka (`consumer.Ping`).
* **No `sync.WaitGroup` before `Close()` on shutdown.** `main.go` теперь ждёт `wg.Wait()` (фоновый `Consumer.Run`) перед закрытием Kafka/Redis соединений.
* **Тестируемость** — раньше `Consumer.Run` не был покрыт тестами вообще (только чистый `DecodeConfigChangeEvent`). Оркестрационная логика вынесена в чистую функцию `processRecords`, протестированную без живого Kafka-брокера: коммит только успешно обработанных записей, отсутствие коммита за retriable-ошибкой (и остановка партиции), пропуск+коммит poison-сообщений, независимость партиций друг от друга, фильтрация `subscriber_consent`.

```bash
cd services/config-cache-projector
go build ./...
go test ./...
```

## Что реализовано

| Метод | Где | Как проверено |
|---|---|---|
| `on_config_change` | `internal/kafkaio/consumer.go::DecodeConfigChangeEvent` (чистый разбор + фильтр `subscriber_consent`/allow-list `entity_type`) + `Consumer` (реальный `franz-go`, `DisableAutoCommit`) + `processRecords` (commit-on-success оркестрация) | `consumer_test.go` — round-trip через реальный `proto.Marshal`/`Unmarshal`, отклоняет запись без `entity_id`/мусорные байты/неизвестный `entity_type`, фильтрует `subscriber_consent`, `processRecords` покрывает commit/retry/poison/per-partition независимость |
| `write_projection` | `internal/projector/projector.go::WriteProjection` — `config:current:{entity_type}:{entity_id}` (STRING, номер версии) + `config:version:{entity_type}:{entity_id}:{version}` (STRING, payload), атомарно через `TxPipelined` | `projector_test.go` — реальные `SET`/`GET` на `miniredis`: `config:version:*` пишется всегда, `config:current` обновляется только для `status="active"`, архивная версия не становится текущей, но остаётся читаемой по номеру, `unspecified` entity_type отклоняется |

`entityTypeString` — обратный маппинг `ConfigEntityType` (protobuf) → тот же `lower_snake_case`, что `config.config_versions.entity_type` (PostgreSQL) — не сырое имя protobuf-константы, чтобы Redis-ключи и PostgreSQL-строки оставались в одном алфавите для дебага (`projector_test.go::TestEntityTypeStringMatchesPostgresConvention`).

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в остальных Go-сервисах Субагента 1: `protoc --go_out --go-grpc_out` против `platform-contracts/common/*.proto`, `events/config_and_control.proto`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Consumer` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **Чистка старых версий** ("только active + N последних версий, чистка — отдельный worker", `data_infrastructure_spec.md` §2.2) — этот сервис только пишет, retention/чистку `config:version:*` не реализует; это отдельный воркер, не описанный в `service_internal_methods.md` для этого сервиса и не входящий в текущий срез.
* **Значение `payload_json` пишется как есть (bytes)**, без ре-сериализации в отдельный удобный для чтения формат — bootstrap-потребители должны сами парсить JSON, как и раньше.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (`projected_total`, `consumer_lag` и т.п.).
