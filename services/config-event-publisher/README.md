# Config Event Publisher

**Основание:** `development_plan.md` — Субагент 1, Control plane. `service_internal_methods.md` §3.3 / `service_io_contracts.md` §3.3: `poll_outbox` (PostgreSQL) → `publish_config_change` (Kafka `config.changes`, compacted) → `mark_published` (PostgreSQL).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Outbox-путь протестирован против реального локального PostgreSQL 17.

```bash
cd services/config-event-publisher
go build ./...
go test ./...
```

## Что реализовано

| Метод | Где | Как проверено |
|---|---|---|
| `poll_outbox` | `internal/outbox/outbox.go::PollOutbox` — `SELECT ... WHERE published=false ORDER BY created_at ASC` (partial index `config_outbox_unpublished_idx`), `LEFT JOIN config_versions` для version/status | `outbox_test.go` — реальные вставки/чтения на локальном PostgreSQL: только unpublished-записи возвращаются, порядок по `created_at ASC` |
| `publish_config_change` | `internal/kafkaio/publisher.go::BuildConfigChangeEvent` (чистая сборка) + `Publisher.Publish` (реальный `franz-go`) | `publisher_test.go` — маппинг всех 9 `entity_type` в `ConfigEntityType`, ошибка на неизвестном `entity_type`, поля (`version`/`status`/`payload_json`/`created_at`) пробрасываются верно |
| `mark_published` | `internal/outbox/outbox.go::MarkPublished` — `UPDATE ... SET published=true, published_at=now()` | `outbox_test.go` — реально помечает строку, запись пропадает из следующего `poll_outbox`, ошибка на несуществующем id |

`cmd/config-event-publisher/main.go` — цикл раз в `POLL_INTERVAL_MS` (default 500мс): `poll_outbox` → для каждой записи `publish_config_change` → при успехе `mark_published`. При ошибке публикации запись остаётся unpublished и будет подхвачена следующим тиком (at-least-once, не exactly-once — компактированный топик по ключу `entity_id` терпим к повторной публикации того же payload).

## Открытый вопрос — ключ Kafka-сообщения при коллизии entity_id между entity_type

`ConfigChangeEvent` публикуется с ключом `entity_id` (напр. "acme"). Compaction в Kafka работает по ключу сообщения — если `partner "acme"` и, гипотетически, другой `entity_type` когда-нибудь получит тот же `entity_id` "acme", их записи schlägen в один и тот же compaction-слот на топике `config.changes`, и consumer, читающий все entity_type из одного топика, может потерять одну из них при compaction. `config_versions_unique UNIQUE (entity_type, entity_id, version)` в PostgreSQL защищает от коллизии версий, но не от коллизии Kafka-ключа между разными `entity_type` с одинаковым `entity_id`. Не встречалось на реальных данных этой сессии (все `entity_id` в `config_schemas/examples/` уникальны глобально), но не проверено формально — задокументировано, не скрыто.

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в `execution-control-service`/`configuration-service`: `protoc --go_out --go-grpc_out` против `platform-contracts/common/*.proto`, `events/config_and_control.proto`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **Нет distributed lock / leader election** между репликами — при нескольких инстансах `config-event-publisher` возможна конкурентная публикация одной и той же outbox-записи до того, как первый инстанс успеет `mark_published` (гонка между `poll_outbox` двух реплик в один и тот же момент). Компактированный топик и идемпотентность consumer'ов (`config-cache-projector` читает `entity_type+entity_id+version`, последний побеждает) делают повторную публикацию безвредной, но не бесплатной (лишний трафик). `SELECT ... FOR UPDATE SKIP LOCKED` устранил бы гонку — не реализовано в этом срезе.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (`published_total`, `poll_lag` и т.п.).
