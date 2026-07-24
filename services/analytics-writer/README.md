# Analytics Writer

**Основание:** `development_plan.md` — Субагент 1, Writer-периметр. `services_specifictaion.md` §7.2: `incoming.messages`/`stage.completed`/`message.lifecycle` → ClickHouse batch insert, материализованные агрегаты. В отличие от Lifecycle Writer, **читает `stage.completed`** — детальная пер-стадийная история идёт сюда, не в PostgreSQL (HLD §18).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. ClickHouse-путь протестирован против **реального локального ClickHouse** (brew cask, запущен в этой сессии — не мок, реальный `INSERT`/`SELECT` round-trip через `clickhouse-go/v2`).

```bash
# ClickHouse cask помечен deprecated (не проходит Gatekeeper) — снят карантин один раз:
xattr -d com.apple.quarantine /opt/homebrew/bin/clickhouse
clickhouse server -- --path=/tmp/clickhouse-data --listen_host=127.0.0.1 &

cd services/analytics-writer
go build ./...
go test ./...
```

## Открытый вопрос — схема ClickHouse спроектирована здесь, не из документов

**`data_infrastructure_spec.md` не содержит раздела ClickHouse** — ни одна таблица/поле не специфицированы ни в одном документе этой сессии (в отличие от PostgreSQL, где все 17 таблиц — `migrations/`). `internal/store/store.go` проектирует рабочую схему сам:

```sql
CREATE TABLE analytics.stage_events (
    event_type, message_id, partner_id, stage_name, outcome,
    reason_code, lifecycle_status, occurred_at, ingested_at
) ENGINE = MergeTree() ORDER BY (occurred_at, message_id)

CREATE MATERIALIZED VIEW analytics.stage_events_hourly_mv
ENGINE = SummingMergeTree() ORDER BY (hour, stage_name, outcome)
AS SELECT toStartOfHour(occurred_at), stage_name, outcome, count() ...
```

Одна широкая денормализованная таблица (типичный ClickHouse-паттерн) вместо трёх узких PostgreSQL-таблиц Lifecycle Writer — `event_type` различает происхождение строки (`incoming`/`stage_completed`/`lifecycle`), большинство полей пусты для события, к которому они не относятся. Это рабочее предположение, не согласованная с Главным агентом или каким-либо документом схема — реальная production-схема (particionирование по времени, TTL, codec для `occurred_at`) требует отдельного проектирования.

## Что реализовано по service_internal_methods.md §6.2

| Метод | Где | Как проверено |
|---|---|---|
| `on_event` | `internal/core/records.go` — `FromIncomingMessage`/`FromStageCompleted`/`FromLifecycleEvent` (чистые функции) | `records_test.go` — маппинг всех значений `Outcome`/`MessageLifecycleStatus`/`StageName` в строки |
| `batch_buffer` | `cmd/analytics-writer/main.go::buffer` | Покрыто через wiring + `store` тесты |
| `flush_batch` | `internal/store/store.go::FlushBatch` — `clickhouse-go/v2` native batch API | `store_test.go` — **реальный INSERT + SELECT readback на локальном ClickHouse**: 2 строки вставлены и найдены по `message_id`, пустой batch — no-op |
| `refresh_materialized_view` | `internal/store/store.go::EnsureSchema` — `CREATE MATERIALIZED VIEW ... POPULATE` (ClickHouse обновляет MV инкрементально на каждый INSERT в исходную таблицу, отдельного "refresh по расписанию" для обычной, не `REFRESH`-based MV не требуется) | `store_test.go::TestEnsureSchemaIsIdempotent` — повторный вызов не падает (`IF NOT EXISTS`) |

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `main.go` использует реальный `franz-go`, компилируется, не проверялся против `kind`+Strimzi.
* **Партиционирование/TTL/codec ClickHouse** — не настроены, схема минимальна (см. "Открытый вопрос" выше).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.