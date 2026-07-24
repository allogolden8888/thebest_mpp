# DLR Correlation Writer

**Основание:** `development_plan.md` Фаза 2.1, восьмой сервис "ходового скелета" (Главный агент) — пишет `operator_id + smsc_message_id → message_id` корреляцию, нужную DLR Manager для сопоставления сырых DLR с исходными сообщениями (`service_internal_methods.md` §4.1). Первый Go-сервис Главного агента в этой сессии (остальные Go-сервисы — у Субагента 1), первый сервис, реально протестированный против **живой** локальной PostgreSQL, не только скомпилированный против клиента.

**Статус:** `go build ./... && go test ./...`, **15/15 тестов проходят**, 3 из них — реальные round-trip'ы против настоящего локального PostgreSQL 17 (`migrations/V009__dlr_correlation.sql` уже применена в этой песочнице), не пропущены/замоканы.

```bash
cd services/dlr-correlation-writer
go build ./...
go test ./...
```

## Реальная находка: партиции `dlr_correlation` никто не создаёт после бутстрапа

`migrations/V015__partition_maintenance.sql` создаёт почасовые партиции `dlr.dlr_correlation` только на "текущий час + следующие 4 часа" **на момент применения миграций**, и явным комментарием документирует: `dlr.create_correlation_partition` дальше "вызывается любым внешним планировщиком (k8s CronJob, pg_cron, приложение) раз в час". Ни такого CronJob, ни `pg_cron`-задачи, ни вызывающего кода нигде в репозитории не существует (проверено `grep` по `create_correlation_partition`). **Не гипотеза — реально воспроизведено**: первый прогон `pg_writer_test.go` против настоящей БД упал с `no partition of relation "dlr_correlation" found for row` (SQLSTATE 23514), потому что бутстрап-окно миграции уже истекло к моменту запуска этой сессии.

Поскольку этот сервис — единственный писатель в эту таблицу, он взял на себя самообслуживание: `PgWriter.EnsurePartition(ctx, hour)` (дешёвый идемпотентный `CREATE TABLE IF NOT EXISTS`) вызывается для текущего и следующего часа перед каждым flush в `cmd/dlr-correlation-writer/main.go` — не полагается на планировщик, которого не существует. Регрессия доказана `TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow`: без `EnsurePartition` insert в заведомо небутстрапленный час (текущее время + 1 год) реально падает, с ним — проходит. **Не решает:** произвольно устаревший (backfill/replay) `submitted_at` — партиция для него всё ещё не создаётся автоматически; и retention (`dlr.drop_old_correlation_partitions`) по-прежнему не вызывается ниоткуда — это отдельная забота (вероятно, отдельный CronJob вне скоупа этого сервиса).

## Что реализовано по service_internal_methods.md §4.1

| Метод | Где | Примечание |
|---|---|---|
| `on_submit_accepted` | `internal/kafkaio/consumer.go::DecodeOperatorSubmitAccepted` | Чистая функция; `smsc_message_id` осознанно необязателен ("не все операторы возвращают его синхронно", `operator_events.proto`), `operator_id`/`message_id`/`stage_execution_id` обязательны |
| `batch_buffer` | `internal/writer/buffer.go::BatchBuffer` | Чистая, потокобезопасная; `Add` сигнализирует flush по размеру, `Snapshot` НЕ чистит буфер (неудачный flush не должен терять записи) |
| `flush_batch` | `internal/writer/pg_writer.go::PgWriter.Flush` + `cmd/dlr-correlation-writer/main.go` | По размеру (`Add` возвращает true) ИЛИ по таймеру (`flushInterval`, реализовано через `context.WithTimeout` на каждый цикл поллинга — не отдельная горутина/мьютекс поверх буфера) |

## `COPY/batch insert` — почему `INSERT ... ON CONFLICT DO NOTHING`, не голый `COPY`

Спецификация говорит "COPY/batch insert", но чистый PostgreSQL `COPY` не поддерживает `ON CONFLICT`. At-least-once redelivery `operator.submit.accepted` (тот же класс, что уже задокументирован во всех Kafka-consumer'ах этой сессии — сбой между успешным flush и offset commit) means один и тот же `CorrelationRecord` может дойти до flush дважды. `PRIMARY KEY (operator_id, smsc_message_id, segment_id, submitted_at)` уже существует в V009 именно для этого — используется batched `INSERT ... ON CONFLICT DO NOTHING` через `pgx.Batch`/`SendBatch` (один pipelined round-trip, не N последовательных) — тот же идемпотентный паттерн, что уже реально проверен для `billing_ledger` (`migrations/README.md`). Доказано `TestFlushIsIdempotentUnderRedelivery`: повторный `Flush` того же `CorrelationRecord` не создаёт вторую строку.

## Kafka commit — коммит только после успешного flush, autocommit выключен явно

`CODE_REVIEW.md` нашло cross-cutting баг у всех трёх Go-сервисов Субагента 1: `franz-go`'s дефолтный autocommit коммитит позицию раз в 5с **независимо от успеха обработки** — неудачная запись в БД теряется навсегда, если её оффсет уже автокоммитнулся. Здесь это не воспроизведено с самого начала: `kgo.DisableAutoCommit()` при создании клиента, `Consumer.CommitRecords` вызывается **только** из `main.go`'s `flush()` **после** успешного `PgWriter.Flush` — если запись в PostgreSQL не удалась, оффсет не коммитится, буфер не чистится, та же партия переобрабатывается на следующем цикле (безопасно — `ON CONFLICT DO NOTHING` делает повтор идемпотентным).

## Тесты — что доказано

15 тестов:
* `internal/kafkaio` (6) — декодирование `OperatorSubmitAccepted`: валидная запись, осознанно опциональный `smsc_message_id`, обязательные `operator_id`/`message_id`/`stage_execution_id`, мусорные байты не паникуют.
* `internal/writer` — `BatchBuffer` (7): flush-по-размеру, `Snapshot` не чистит буфер (защита от потери при неудачном flush), `MaxOffsets` — per-partition верхняя граница коммита.
* `internal/writer` — `PgWriter` (3, **реально против живой PostgreSQL, не моки**): `TestFlushAgainstRealPostgres` — настоящий batch insert, читается обратно; `TestFlushIsIdempotentUnderRedelivery` — повтор не задваивает; `TestEnsurePartitionRequiredBeforeInsertPastBootstrapWindow` — регрессия на находку выше.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Consumer` использует настоящий `franz-go`, компилируется, декодирование протестировано, но `PollOnce`/`NewConsumer` не проверялись против `kind`+Strimzi (тот же паттерн, что у всех Kafka-сервисов этой сессии).
* **`docker build` не выполнялся** — недоступен Docker daemon в этом окружении.
* **Poison-message на `operator.submit.accepted` не продвигает оффсет партиции**, если это единственная запись с последнего успешного flush — тот же класс, что уже задокументирован (не исправлен) в нескольких сервисах `CODE_REVIEW.md`; не DLQ-путь для этого топика в этом срезе (нет ни одного DLQ во всём репозитории для входных Kafka-топиков, самодокументированное решение, не пробел конкретно этого сервиса).
* **Тесты против живой PostgreSQL пишут настоящие строки в локальную БД `mpp` и не удаляют их за собой** — `operator_id` в тестовых записях начинается с `test-op-`, легко отличить/почистить вручную; не влияет на корректность тестов (уникальные `smsc_message_id` через UUID на каждый запуск), но не cleanup-safe для переиспользуемой dev-БД.
* **Backfill/replay с произвольно устаревшим `submitted_at`** — `EnsurePartition` создаёт только текущий/следующий час, не прошлые — см. находку выше.
