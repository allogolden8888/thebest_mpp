# Config Event Publisher

**Основание:** `development_plan.md` — Субагент 1, Control plane. `service_internal_methods.md` §3.3 / `service_io_contracts.md` §3.3: `poll_outbox` (PostgreSQL) → `publish_config_change` (Kafka `config.changes`, compacted) → `mark_published` (PostgreSQL).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Outbox-путь протестирован против реального локального PostgreSQL 17.

**CODE_REVIEW.md — что исправлено:**
* **CRITICAL, compliance-sensitive — `COALESCE(cv.status, 'active')` делал consent-revocation структурно невозможной.** `PollOutbox`'s `LEFT JOIN config.config_versions` даёт `cv.status` только для entity_type, пишущих строку в `config_versions` — `policy_template`/`subscriber_consent` этого никогда не делают (`config_version_id` всегда `NULL`, см. `migrations/V003` комментарий), поэтому `status` для ОБОИХ навсегда резолвился в `"active"`. Разрешено по-разному для двух entity_type (`internal/kafkaio/publisher.go::ResolveStatus`): **`policy_template` — настоящий фикс**, `config_schemas/policy_template.schema.json` требует поле `status` прямо в `payload_json` — теперь читается оттуда, archival реально доходит до `config.changes`. **`subscriber_consent` — подтверждено неразрешимо в рамках этого сервиса**: `config_schemas/subscriber_consent.schema.json` не содержит поля `status` вообще ("append/delete по PRIMARY KEY, не version-based"), и чтением `services/configuration-service/internal/store/store.go` подтверждено, что `ArchiveVersion` не пишет в `config_outbox` вообще ни для одного entity_type — сегодня в репозитории физически нет кода, который создал бы outbox-строку, сигнализирующую `archived` для consent. `"active"` остаётся единственным безопасным предположением без изобретения нового контракта конфигурации (вне scope этого сервиса — владеет `configuration-service`), но теперь это **явно и громко логируется** (`WARNING` в `runPollLoop`) как предположение, а не тихий баг. См. "Что НЕ реализовано" ниже.
* **HIGH — poison-message head-of-line blocking, no bound, no DLQ.** Строка, которая не может опубликоваться (malformed payload, неизвестный `entity_type`), раньше переигрывалась вечно и, накопившись до `POLL_BATCH_SIZE`, вытесняла реальные pending-строки из `ORDER BY created_at ASC LIMIT N`. `migrations/V020__config_outbox_claim_and_retry.sql` добавила `attempts`/`last_error` — `PollOutboxWithLimits` перестаёт выбирать строку после `POLL_MAX_ATTEMPTS` (default 10) попыток; строка остаётся `published=false` с `last_error`, видна оператору прямым SQL-запросом (мягкий DLQ без отдельной таблицы/топика).
* **HIGH — no `SELECT ... FOR UPDATE SKIP LOCKED` for multi-replica safety.** Та же миграция добавила `claimed_at` — `PollOutboxWithLimits` теперь атомарно клеймит строки транзакцией с `FOR UPDATE SKIP LOCKED` (claim истекает через `DefaultClaimTTL`=30с, так что упавшая между claim и mark_published/mark_publish_failed реплика не теряет строку навсегда).
* **MEDIUM — no backoff on poll-loop errors.** `runPollLoop` теперь делает экспоненциальный backoff (до 30с) на ошибку `PollOutbox` (напр. Postgres недоступен) вместо hammering каждые `POLL_INTERVAL_MS`.
* **MEDIUM — `/readyz` never reflects real downstream health.** `internal/health` теперь поддерживает `SetDependencyChecks` — `/readyz` реально пингует Postgres (`pool.Ping`) и Kafka (`publisher.Ping`).
* **No `sync.WaitGroup` before `Close()` on shutdown.** `main.go` теперь ждёт `wg.Wait()` (фоновый `runPollLoop`) перед закрытием Kafka producer / Postgres pool.
* **Тестируемость** — раньше ни один тест не проверял оркестрацию (`runPollLoop`) как таковую. `internal/outbox/outbox_test.go` теперь покрывает claim/attempts/exhaustion (`TestPollOutboxDoesNotReturnAlreadyClaimedRow`, `TestPollOutboxReclaimsRowAfterClaimTTLExpires`, `TestMarkPublishFailedStopsBeingPolledAfterMaxAttempts`, `TestMarkPublishFailedClearsClaimForImmediateRetry`), `internal/kafkaio/publisher_test.go` покрывает `ResolveStatus` для всех трёх случаев (`config_versions`-based, `policy_template`-payload-based, `subscriber_consent`-unresolved).

```bash
cd services/config-event-publisher
go build ./...
go test ./...
```

## Что реализовано

| Метод | Где | Как проверено |
|---|---|---|
| `poll_outbox` | `internal/outbox/outbox.go::PollOutboxWithLimits` — атомарный claim (`SELECT ... FOR UPDATE SKIP LOCKED`) строк `published=false AND attempts<maxAttempts`, `LEFT JOIN config_versions` для version/status | `outbox_test.go` — реальные вставки/чтения на локальном PostgreSQL: только unpublished-записи возвращаются, порядок по `created_at ASC`, claim/attempts/exhaustion (см. CODE_REVIEW.md выше) |
| `publish_config_change` | `internal/kafkaio/publisher.go::BuildConfigChangeEvent` (чистая сборка, включая `ResolveStatus`) + `Publisher.Publish` (реальный `franz-go`) | `publisher_test.go` — маппинг всех 9 `entity_type` в `ConfigEntityType`, ошибка на неизвестном `entity_type`, поля (`version`/`status`/`payload_json`/`created_at`) пробрасываются верно, `ResolveStatus` для всех трёх путей резолвинга status |
| `mark_published` / `mark_publish_failed` | `internal/outbox/outbox.go::MarkPublished`/`MarkPublishFailed` — `UPDATE ... SET published=true, published_at=now()` / `UPDATE ... SET attempts=attempts+1, last_error=..., claimed_at=NULL` | `outbox_test.go` — реально помечает строку, запись пропадает из следующего `poll_outbox`, ошибка на несуществующем id, retriable-ошибка снимает claim для немедленного повтора |

`cmd/config-event-publisher/main.go` — цикл раз в `POLL_INTERVAL_MS` (default 500мс): `poll_outbox` → для каждой записи `publish_config_change` → при успехе `mark_published`, при ошибке `mark_publish_failed` (attempts++, claim снят для повтора). При ошибке `poll_outbox` самого (напр. Postgres недоступен) — экспоненциальный backoff, не hammering. at-least-once, не exactly-once — компактированный топик по ключу `entity_id` терпим к повторной публикации того же payload.

## Открытый вопрос — ключ Kafka-сообщения при коллизии entity_id между entity_type

`ConfigChangeEvent` публикуется с ключом `entity_id` (напр. "acme"). Compaction в Kafka работает по ключу сообщения — если `partner "acme"` и, гипотетически, другой `entity_type` когда-нибудь получит тот же `entity_id` "acme", их записи schlägen в один и тот же compaction-слот на топике `config.changes`, и consumer, читающий все entity_type из одного топика, может потерять одну из них при compaction. `config_versions_unique UNIQUE (entity_type, entity_id, version)` в PostgreSQL защищает от коллизии версий, но не от коллизии Kafka-ключа между разными `entity_type` с одинаковым `entity_id`. Не встречалось на реальных данных этой сессии (все `entity_id` в `config_schemas/examples/` уникальны глобально), но не проверено формально — задокументировано, не скрыто.

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в `execution-control-service`/`configuration-service`: `protoc --go_out --go-grpc_out` против `platform-contracts/common/*.proto`, `events/config_and_control.proto`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **subscriber_consent revocation по-прежнему не может опубликоваться как `archived`** — не баг этого сервиса (см. CODE_REVIEW.md выше), а подтверждённый структурный пробел выше по пайплайну: `config_schemas/subscriber_consent.schema.json` не несёт поля `status`, и `configuration-service` не пишет outbox-строку для consent-revocation вообще (`ArchiveVersion` не трогает `config_outbox` ни для одного entity_type). Чтобы это заработало, `configuration-service` должен получить явный способ сигнализировать revocation — либо расширить `subscriber_consent.schema.json` полем вроде `deleted`/`status`, либо завести отдельный DELETE-путь, который сам пишет outbox-строку с известным статусом. Оба варианта — новый контракт, которым владеет `configuration-service`/схемы, не этот сервис; здесь только зафиксирован факт и подготовлен `ResolveStatus`, готовый корректно обработать `cv.status`/`payload_json.status`, как только откуда-то появится настоящий сигнал.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (`published_total`, `poll_lag`, `outbox_exhausted_total` и т.п.).
