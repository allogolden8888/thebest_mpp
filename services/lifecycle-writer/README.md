# Lifecycle Writer

**Основание:** `development_plan.md` — Субагент 1, Writer-периметр. `services_specifictaion.md` §7.1: `incoming.messages`/`message.lifecycle`/DLQ-топики → PostgreSQL (read model, lifecycle history, DLQ record). `stage.completed` сознательно не читается (детальная пер-стадийная история — только в ClickHouse через Analytics Writer, HLD §18).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. PostgreSQL-путь протестирован против реального локального PostgreSQL 17.

```bash
cd services/lifecycle-writer
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §6.1

| Метод | Где | Как проверено |
|---|---|---|
| `on_event` | `internal/core/records.go` — `FromIncomingMessage`/`FromLifecycleEvent`/`FromDlqRecord` (чистые функции) | `records_test.go` — маппинг полей, `MessageLifecycleStatus` → строка, `original_command` реально сериализуется отдельно от всего `DlqRecord` |
| `batch_buffer` | `cmd/lifecycle-writer/main.go::buffer` — накопление per-таблица (`history`/`dlq`) между тиками | Косвенно (структура покрыта через `store` тесты + main wiring) |
| `flush_batch` | `internal/store/store.go` — `pgx.Batch` batch insert/upsert | `store_test.go` — реальные вставки на PostgreSQL: `InsertReadModel`/`UpdateReadModel` (идемпотентно, `ON CONFLICT DO NOTHING`), `BatchInsertLifecycleHistory`, `BatchInsertDlq` |

## Открытые вопросы (задокументированы, не скрыты)

1. **`pipeline_id`/`pipeline_version` в `message_read_model`.** `IncomingMessage` (platform-contracts/events/message_events.proto) не несёт эти поля — они резолвятся Pipeline Engine уже после приёма (`resolve_pipeline_version`) и не публикуются обратно ни в одном событии, которое Lifecycle Writer читает. `FromIncomingMessage` заполняет их пустой строкой (столбцы `NOT NULL`, `migrations/V004`) — заполнение реальным значением потребовало бы либо нового поля в `IncomingMessage`, либо отдельного события от Pipeline Engine (решения Главного агента, владеет `pipeline-engine`/`platform-contracts`).
2. **`operator.dlr.dlq` не обрабатывается.** У этого топика другая полезная нагрузка (не `DlqRecord`) — DLR Manager (Главный агент) публикует истёкшие DLR-корреляции в другом формате, не специфицированном для Lifecycle Writer в этой сессии. `internal/kafkaio.DlqTopics` перечисляет только 6 `stage.*.dlq`, не `operator.dlr.dlq`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `main.go` использует реальный `franz-go`, компилируется, не проверялся против `kind`+Strimzi.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.