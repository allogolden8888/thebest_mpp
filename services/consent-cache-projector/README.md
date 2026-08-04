# Consent Cache Projector

**Основание:** `development_plan.md` — Субагент 1, Control plane. `data_infrastructure_spec.md` §1.9c: "Consent Cache Projector (Go, симметричен Config Cache Projector) — Policy читает consent-данные на каждое сообщение (hot path), не при bootstrap", поэтому проекция идёт в **Runtime Redis**, не Configuration Redis.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Redis-путь протестирован против реального протокола Redis (`miniredis`), `ResyncFromPostgres` — против реального локального PostgreSQL 17.

**CODE_REVIEW.md — что исправлено:**
* **CRITICAL, компромисс не в этом сервисе — тот же `COALESCE(cv.status, 'active')` баг с consent-стороны.** `ApplyConsentChange`'s `SRem`/revoke-ветка (`status=="archived"`) фактически недостижима в проде, пока `config-event-publisher` не резолвит `status` правильно для `subscriber_consent` — что структурно невозможно сегодня (см. `services/config-event-publisher/README.md`). Не баг этого сервиса, зафиксировано здесь тоже, т.к. проявляется в его логике.
* **MEDIUM — `ApplyConsentChange` treats any status other than exactly "archived" as "add to blacklist" with no positive validation.** Раньше любой незнакомый `status` тихо трактовался как `"active"` (SADD) — непоследовательно с более строгой валидацией `ParsePayload`'s `scope_type` в этой же функции. Теперь `status ∉ {"active","archived"}` явно отклоняется ошибкой, чтобы баг конфигурации выше по пайплайну не прятался за молчаливым "добавить в блэклист".
* **MEDIUM — "Full Postgres resync on Redis loss not implemented" (было self-disclosed).** Реализовано: `internal/projector/projector.go::ResyncFromPostgres` читает **все** строки `policy.subscriber_consent` (единственный source of truth), атомарно перестраивает каждый затронутый `consent:*_blacklist:{msisdn}` ключ (write-to-temp-SET-then-`RENAME` — читатели никогда не видят пустой/частично заполненный набор в процессе), и удаляет любой существующий `consent:*_blacklist:*` ключ, для которого в PostgreSQL больше нет ни одной строки (полностью отозванный opt-out, иначе оставшийся бы фантомом в Redis навсегда). Вызывается автоматически при каждом старте (`RESYNC_ON_START=true` по умолчанию) до объявления `/readyz` готовым — см. `cmd/consent-cache-projector/main.go`. Ошибка ресинка не фатальна (логируется как `CRITICAL`, сервис продолжает на текущем состоянии Redis) — Postgres может быть временно недоступен при обычном rolling restart, а не только после инцидента.
* **Cross-cutting, CRITICAL — Kafka autocommit decoupled from processing success.** Особенно значимо здесь — `consent:*` единственный НЕ эфемерный ключ-класс из трёх Redis-кластеров (`data_infrastructure_spec.md §2.1`), и до этого фикса восстановление через `ResyncFromPostgres` при старте было единственной защитой от того, что тихо потерянный offset НАВСЕГДА теряет opt-out/revocation. `NewConsumer` теперь передаёт `kgo.DisableAutoCommit()`; consume-цикл (та же оркестрация `processRecords`, что в `config-cache-projector`) коммитит offset только после успешного `apply_consent_change`.
* **`/readyz` never reflects real downstream health.** `internal/health` теперь поддерживает `SetDependencyChecks` — `/readyz` реально пингует Redis и Kafka.
* **No `sync.WaitGroup` before `Close()` on shutdown.** `main.go` теперь ждёт `wg.Wait()` перед закрытием Kafka/Redis соединений.
* **Тестируемость** — `processRecords` протестирована без живого Kafka-брокера (та же схема тестов, что `config-cache-projector`), `ApplyConsentChange` теперь покрыта тестом на неизвестный/пустой `status`, `ResyncFromPostgres` покрыта двумя реальными Postgres+Redis тестами (восстановление из PostgreSQL на пустом Redis, удаление фантомного ключа, отсутствующего в PostgreSQL).

```bash
cd services/consent-cache-projector
go build ./...
go test ./...
```

## Что реализовано

| Компонент | Где | Как проверено |
|---|---|---|
| Фильтрация `config.changes` по `entity_type=subscriber_consent` | `internal/kafkaio/consumer.go::DecodeConsentEvent` | `consumer_test.go` — другие `entity_type` тихо игнорируются (`nil, nil`), `subscriber_consent` без `entity_id` — ошибка, мусорные байты — ошибка |
| Разбор payload (`config_schemas/subscriber_consent.schema.json`) | `internal/projector/projector.go::ParsePayload` | `projector_test.go` — валидный payload, отсутствующий `msisdn`, неизвестный `scope_type` |
| Проекция в `consent:category_blacklist:{msisdn}` / `consent:sender_blacklist:{msisdn}` (SET) | `internal/projector/projector.go::ApplyConsentChange` | `projector_test.go` — реальные `SADD`/`SREM`/`SISMEMBER` на `miniredis`: `status="active"` добавляет, `status="archived"` убирает (opt-out отозван), CATEGORY и SENDER блэклисты независимы, неизвестный/пустой `status` отклоняется |
| Full resync из PostgreSQL (`data_infrastructure_spec.md §1.9c` runbook) | `internal/projector/projector.go::ResyncFromPostgres`, вызывается при старте (`cmd/consent-cache-projector/main.go`) | `projector_test.go` — реальный локальный PostgreSQL: восстановление блэклиста из `policy.subscriber_consent` на пустом Redis, удаление фантомного ключа, отсутствующего в PostgreSQL |

`ConfigChangeEvent.status` здесь означает не "активная версия конфигурации" (как для version-based entity_type), а **"этот opt-out сейчас в силе"** (`active`) vs **"opt-out отозван"** (`archived`) — `subscriber_consent` не version-based, PRIMARY KEY `(msisdn, scope_type, scope_value, channel)` append/delete (`config_schemas/subscriber_consent.schema.json` докстринг).

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в остальных Go-сервисах Субагента 1.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера.**
* **`ResyncFromPostgres` вызывается только автоматически при старте пода**, не как отдельно триггерируемая операторская команда/эндпоинт — на обычном k8s-деплойменте это покрывает и "потеря Redis" (следующий рестарт пода восстановит), и "рутинный рестарт", но не даёт способа принудительно перезапустить ресинк без рестарта пода (напр. через сигнал/HTTP-эндпоинт), если понадобится вручную во время инцидента без падения самого пода.
* **`channel` не участвует в ключе Redis** — `consent:category_blacklist:{msisdn}` общий для всех каналов, хотя PRIMARY KEY в PostgreSQL включает `channel`. На практике сейчас только `SMS` (HLD — multichannel в Фазе 8), но при появлении EMAIL/PUSH с разными consent-правилами на тот же `(msisdn, scope_value)` эта схема ключей должна быть пересмотрена — задокументировано, не скрыто.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.