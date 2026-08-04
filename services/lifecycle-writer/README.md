# Lifecycle Writer

**Основание:** `development_plan.md` — Субагент 1, Writer-периметр. `services_specifictaion.md` §7.1: `incoming.messages`/`message.lifecycle`/DLQ-топики → PostgreSQL (read model, lifecycle history, DLQ record). `stage.completed` сознательно не читается (детальная пер-стадийная история — только в ClickHouse через Analytics Writer, HLD §18).

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. PostgreSQL-путь протестирован против реального локального PostgreSQL 17.

**CODE_REVIEW.md — что исправлено после первого прохода ревью:**
* **HIGH — тихая, необратимая потеря данных при сбое записи.** `flush()` раньше снимал и обнулял `buf.history`/`buf.dlq` ПОД ЛОКОМ до подтверждения успешной записи — при сбое `BatchInsertLifecycleHistory`/`BatchInsertDlq` (транзиентный сбой Postgres) уже снятые данные были потеряны безвозвратно, единственный след — строка в логе. Дополнительно `InsertReadModel`/`UpdateReadModel` писались синхронно по одной строке в consume-цикле — та же проблема "запись без подтверждения", просто без буфера. Плюс `kgo.NewClient` не передавал `kgo.DisableAutoCommit()` — offset коммитился раз в 5с независимо от того, успела ли запись пройти. Переписано: consume-цикл теперь только буферизует (никаких синхронных записей в hot path, заодно закрывает MEDIUM-находку о несоответствии §6.1, который описывает батчинг ВСЕХ таблиц вместе); commit смещений полностью ручной (`client.CommitRecords`) и происходит только после подтверждённого успеха всех batch-записей; при сбое весь снятый набор (включая связанные `*kgo.Record`) возвращается в буфер (`buffer.restore`) для повтора на следующем тике — ничего не теряется, SQL идемпотентен (`ON CONFLICT DO NOTHING` / lifecycle_version-guard, см. ниже), так что повторная попытка частично уже применённого набора безопасна. Новые тесты: `cmd/lifecycle-writer/buffer_test.go` (drain/restore контракт).
* **HIGH — нет дренирования буфера при graceful shutdown.** SIGTERM просто отменял контекст без ожидания консьюмер/flush-горутин — обычный rolling deploy терял накопленное с последнего тика. Теперь `sync.WaitGroup` вокруг обеих goroutine + финальный `flush()` перед выходом.
* **MEDIUM — `UpdateReadModel` не защищён от out-of-order/повторной доставки.** Redelivery старого `SUBMITTED` ПОСЛЕ уже применённого `DELIVERED` (партиционный rebalance) откатывал партнёр-facing read model назад. Добавлена колонка `lifecycle_version` (`migrations/V019__message_read_model_lifecycle_version.sql`) — `UPDATE ... WHERE lifecycle_version < $new`, обновление применяется только если оно новее уже применённого. Тест: `store_test.go` `TestUpdateReadModelIgnoresOutOfOrderRedelivery`.
* **MEDIUM — read model писался не батчем вместе с history/dlq.** Новые `BatchInsertReadModel`/`BatchUpdateReadModel` (тот же `pgx.Batch`-паттерн, что уже был у history/dlq), используются в реальном consume-пути; одиночные `InsertReadModel`/`UpdateReadModel` оставлены для точечных вызовов/тестов.

```bash
cd services/lifecycle-writer
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §6.1

| Метод | Где | Как проверено |
|---|---|---|
| `on_event` | `internal/core/records.go` — `FromIncomingMessage`/`FromLifecycleEvent`/`FromDlqRecord` (чистые функции) | `records_test.go` — маппинг полей, `MessageLifecycleStatus` → строка, `original_command` реально сериализуется отдельно от всего `DlqRecord` |
| `batch_buffer` | `cmd/lifecycle-writer/main.go::buffer` — накопление по всем 4 таблицам (`inserts`/`updates`/`history`/`dlq`) + связанные `*kgo.Record` между тиками, `drain`/`restore` — не теряет данные при сбое записи (см. CODE_REVIEW.md выше) | `cmd/lifecycle-writer/buffer_test.go` |
| `flush_batch` | `internal/store/store.go` — `pgx.Batch` batch insert/upsert; commit смещений — только после подтверждённого успеха всех четырёх batch-записей | `store_test.go` — реальные вставки на PostgreSQL: `InsertReadModel`/`BatchInsertReadModel`, `UpdateReadModel`/`BatchUpdateReadModel` (идемпотентно, `ON CONFLICT DO NOTHING` / lifecycle_version-guard), `BatchInsertLifecycleHistory`, `BatchInsertDlq` |

## Открытые вопросы (задокументированы, не скрыты)

1. **`pipeline_id`/`pipeline_version` в `message_read_model`.** `IncomingMessage` (platform-contracts/events/message_events.proto) не несёт эти поля — они резолвятся Pipeline Engine уже после приёма (`resolve_pipeline_version`) и не публикуются обратно ни в одном событии, которое Lifecycle Writer читает. `FromIncomingMessage` заполняет их пустой строкой (столбцы `NOT NULL`, `migrations/V004`) — заполнение реальным значением потребовало бы либо нового поля в `IncomingMessage`, либо отдельного события от Pipeline Engine (решения Главного агента, владеет `pipeline-engine`/`platform-contracts`).
2. **`operator.dlr.dlq` не обрабатывается.** У этого топика другая полезная нагрузка (не `DlqRecord`) — DLR Manager (Главный агент) публикует истёкшие DLR-корреляции в другом формате, не специфицированном для Lifecycle Writer в этой сессии. `internal/kafkaio.DlqTopics` перечисляет только 6 `stage.*.dlq`, не `operator.dlr.dlq`.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `main.go` использует реальный `franz-go`, компилируется, не проверялся против `kind`+Strimzi.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.