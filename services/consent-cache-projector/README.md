# Consent Cache Projector

**Основание:** `development_plan.md` — Субагент 1, Control plane. `data_infrastructure_spec.md` §1.9c: "Consent Cache Projector (Go, симметричен Config Cache Projector) — Policy читает consent-данные на каждое сообщение (hot path), не при bootstrap", поэтому проекция идёт в **Runtime Redis**, не Configuration Redis.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Redis-путь протестирован против реального протокола Redis (`miniredis`).

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
| Проекция в `consent:category_blacklist:{msisdn}` / `consent:sender_blacklist:{msisdn}` (SET) | `internal/projector/projector.go::ApplyConsentChange` | `projector_test.go` — реальные `SADD`/`SREM`/`SISMEMBER` на `miniredis`: `status="active"` добавляет, `status="archived"` убирает (opt-out отозван), CATEGORY и SENDER блэклисты независимы |

`ConfigChangeEvent.status` здесь означает не "активная версия конфигурации" (как для version-based entity_type), а **"этот opt-out сейчас в силе"** (`active`) vs **"opt-out отозван"** (`archived`) — `subscriber_consent` не version-based, PRIMARY KEY `(msisdn, scope_type, scope_value, channel)` append/delete (`config_schemas/subscriber_consent.schema.json` докстринг).

## Реальная сквозная проверка контрактов

`internal/proto/gen/` — тот же паттерн, что в остальных Go-сервисах Субагента 1.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера.**
* **Full resync runbook при полной потере Runtime Redis** — `data_infrastructure_spec.md` §1.9c явно требует "принудительный full resync из PostgreSQL... зафиксировано как явная runbook-процедура" — этот сервис реализует только incremental-проекцию из `config.changes`, полный resync (перечитать `policy.subscriber_consent` из PostgreSQL и заново заполнить оба SET) не реализован в этом срезе. Отдельная задача Фазы 6 (`development_plan.md` 6.6: "Runbook-и: полный ресинк Consent Cache Projector").
* **`channel` не участвует в ключе Redis** — `consent:category_blacklist:{msisdn}` общий для всех каналов, хотя PRIMARY KEY в PostgreSQL включает `channel`. На практике сейчас только `SMS` (HLD — multichannel в Фазе 8), но при появлении EMAIL/PUSH с разными consent-правилами на тот же `(msisdn, scope_value)` эта схема ключей должна быть пересмотрена — задокументировано, не скрыто.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.