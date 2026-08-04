# Analytics Writer

**Основание:** `development_plan.md` — Субагент 1, Writer-периметр. `services_specifictaion.md` §7.2: `incoming.messages`/`stage.completed`/`message.lifecycle` → ClickHouse batch insert, материализованные агрегаты. В отличие от Lifecycle Writer, **читает `stage.completed`** — детальная пер-стадийная история идёт сюда, не в PostgreSQL (HLD §18).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, `-race` чисто. ClickHouse-путь протестирован против **реального локального ClickHouse** (brew cask, запущен в этой сессии — не мок, реальный `INSERT`/`SELECT` round-trip через `clickhouse-go/v2`).

**CODE_REVIEW.md — что исправлено после первого прохода ревью:**
* **HIGH — тихая, необратимая потеря данных при сбое записи + автокоммит независимо от успеха.** Тот же класс бага и то же исправление, что в сестринском `lifecycle-writer` (см. его README): `flush()` раньше обнулял буфер ДО подтверждения успешного `FlushBatch`, а `kgo.NewClient` не передавал `kgo.DisableAutoCommit()`. Переписано на ту же модель: consume-цикл только буферизует, commit смещений — полностью ручной (`client.CommitRecords`), происходит только после подтверждённого успеха записи; при сбое весь снятый набор (включая `*kgo.Record`) возвращается в буфер (`buffer.restore`) для повтора. Безопасно повторять частично уже вставленный набор — см. следующий пункт.
* **HIGH/MEDIUM — отсутствие дедупликации редоставленных событий искажало агрегаты.** `analytics.stage_events` была `MergeTree()` без версии/dedup-ключа — at-least-once Kafka redelivery (обычное дело на rebalance/restart) вставляла событие второй раз как новую строку, молча раздувая `count()`-агрегаты, которые Backoffice/Partner API report-запросы строят прямо по этой таблице. Исправлено: таблица теперь `ReplacingMergeTree()` по `(occurred_at, event_id, message_id)` — новая колонка `event_id` (стабильный идентификатор конкретного события: `message_id` для `incoming`, `stage_execution_id`-эквивалент `event_id` из `StageCompletedEvent`, `event_id` из `MessageLifecycleEvent`). Дедупликация происходит при фоновом merge, не синхронно на INSERT — поэтому `backoffice-api`/`partner-api`'s `Report()`-запросы теперь читают таблицу с `FROM analytics.stage_events FINAL` (форсирует merge-on-read; дороже обычного `SELECT`, приемлемо для low-QPS report-эндпоинта). Регрессионный тест против реального ClickHouse: `internal/store/store_test.go` `TestRedeliveredEventDoesNotDoubleCountAfterMerge` (вставляет одно и то же событие дважды, `OPTIMIZE TABLE FINAL`, проверяет `count()=1`). Материализованный агрегат `stage_events_hourly_mv` НЕ исправлен этим срезом — он нигде не читается ни одним известным потребителем репозитория (grep подтверждает; Backoffice/Partner API строят агрегаты напрямую из `stage_events`), инкрементальные MV не видят будущих merge-time дедупликаций базовой таблицы, поэтому его собственная корректность при дублях остаётся под вопросом до тех пор, пока он не станет реально используемым — см. комментарий в `store.go`.
* **HIGH — нет дренирования буфера при graceful shutdown.** Тот же паттерн и то же исправление, что в `lifecycle-writer` — `sync.WaitGroup` вокруг обеих goroutine + финальный `flush()` перед выходом.
* **Low — неограниченный рост буфера при затяжном простое ClickHouse.** Раньше сбой просто терял данные (ограничивая память ценой потери); теперь, когда сбой не теряет данные, буфер мог бы расти неограниченно при долгой недоступности ClickHouse — OOM risk. Добавлен простой backpressure: `runConsumeLoop` приостанавливает `PollFetches`, пока буфер не опустится ниже `maxBufferedRecords` (100k), вместо накопления без предела; непрочитанные записи остаются в Kafka, не в памяти процесса.
* **Low — fetch-level ошибки от `PollFetches` не инспектировались.** `fetches.EachError` теперь логируется (topic/partition/ошибка), не отбрасывается молча.

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
    event_type, event_id, message_id, partner_id, stage_name, outcome,
    reason_code, lifecycle_status, occurred_at, ingested_at
) ENGINE = ReplacingMergeTree() ORDER BY (occurred_at, event_id, message_id)

CREATE MATERIALIZED VIEW analytics.stage_events_hourly_mv
ENGINE = SummingMergeTree() ORDER BY (hour, stage_name, outcome)
AS SELECT toStartOfHour(occurred_at), stage_name, outcome, count() ...
```

`ReplacingMergeTree`/`event_id` — добавлены после CODE_REVIEW.md (см. ниже), закрывают дедупликацию при at-least-once redelivery.

Одна широкая денормализованная таблица (типичный ClickHouse-паттерн) вместо трёх узких PostgreSQL-таблиц Lifecycle Writer — `event_type` различает происхождение строки (`incoming`/`stage_completed`/`lifecycle`), большинство полей пусты для события, к которому они не относятся. Это рабочее предположение, не согласованная с Главным агентом или каким-либо документом схема — реальная production-схема (particionирование по времени, TTL, codec для `occurred_at`) требует отдельного проектирования.

## Что реализовано по service_internal_methods.md §6.2

| Метод | Где | Как проверено |
|---|---|---|
| `on_event` | `internal/core/records.go` — `FromIncomingMessage`/`FromStageCompleted`/`FromLifecycleEvent` (чистые функции) | `records_test.go` — маппинг всех значений `Outcome`/`MessageLifecycleStatus`/`StageName` в строки |
| `batch_buffer` | `cmd/analytics-writer/main.go::buffer` — `drain`/`restore`, не теряет данные при сбое записи (см. CODE_REVIEW.md выше) | `cmd/analytics-writer/buffer_test.go` |
| `flush_batch` | `internal/store/store.go::FlushBatch` — `clickhouse-go/v2` native batch API; commit смещений только после подтверждённого успеха | `store_test.go` — **реальный INSERT + SELECT readback на локальном ClickHouse**: 2 строки вставлены и найдены по `message_id`, пустой batch — no-op, redelivery одного и того же `event_id` не задваивает `count()` после merge |
| `refresh_materialized_view` | `internal/store/store.go::EnsureSchema` — `CREATE MATERIALIZED VIEW ... POPULATE` (ClickHouse обновляет MV инкрементально на каждый INSERT в исходную таблицу, отдельного "refresh по расписанию" для обычной, не `REFRESH`-based MV не требуется) | `store_test.go::TestEnsureSchemaIsIdempotent` — повторный вызов не падает (`IF NOT EXISTS`) |

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `main.go` использует реальный `franz-go`, компилируется, не проверялся против `kind`+Strimzi.
* **Партиционирование/TTL/codec ClickHouse** — не настроены, схема минимальна (см. "Открытый вопрос" выше).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.