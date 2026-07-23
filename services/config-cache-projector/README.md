# Config Cache Projector

**Основание:** `development_plan.md` — Субагент 1, Control plane. `service_internal_methods.md` §3.4: `on_config_change` (Kafka `config.changes`) → `write_projection` (Configuration Redis, `data_infrastructure_spec.md` §2.2).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Redis-путь протестирован против реального протокола Redis (`miniredis`).

```bash
cd services/config-cache-projector
go build ./...
go test ./...
```

## Что реализовано

| Метод | Где | Как проверено |
|---|---|---|
| `on_config_change` | `internal/kafkaio/consumer.go::DecodeConfigChangeEvent` (чистый разбор) + `Consumer` (реальный `franz-go`) | `consumer_test.go` — round-trip через реальный `proto.Marshal`/`Unmarshal`, отклоняет запись без `entity_id` и мусорные байты |
| `write_projection` | `internal/projector/projector.go::WriteProjection` — `config:current:{entity_type}:{entity_id}` (STRING, номер версии) + `config:version:{entity_type}:{entity_id}:{version}` (STRING, payload) | `projector_test.go` — реальные `SET`/`GET` на `miniredis`: `config:version:*` пишется всегда, `config:current` обновляется только для `status="active"`, архивная версия не становится текущей, но остаётся читаемой по номеру |

`entityTypeString` — обратный маппинг `ConfigEntityType` (protobuf) → тот же `lower_snake_case`, что `config.config_versions.entity_type` (PostgreSQL) — не сырое имя protobuf-константы, чтобы Redis-ключи и PostgreSQL-строки оставались в одном алфавите для дебага (`projector_test.go::TestEntityTypeStringMatchesPostgresConvention`).

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в остальных Go-сервисах Субагента 1: `protoc --go_out --go-grpc_out` против `platform-contracts/common/*.proto`, `events/config_and_control.proto`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Consumer` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **Чистка старых версий** ("только active + N последних версий, чистка — отдельный worker", `data_infrastructure_spec.md` §2.2) — этот сервис только пишет, retention/чистку `config:version:*` не реализует; это отдельный воркер, не описанный в `service_internal_methods.md` для этого сервиса и не входящий в текущий срез.
* **Значение `payload_json` пишется как есть (bytes)**, без ре-сериализации в отдельный удобный для чтения формат — bootstrap-потребители должны сами парсить JSON, как и раньше.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (`projected_total`, `consumer_lag` и т.п.).
