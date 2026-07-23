# Scheduler — Critical Sweep

**Основание:** `development_plan.md` — Субагент 1, Фаза 3.2. Пересмотрен в `services_specifictaion.md` §3.1: больше не Kafka Streams-группа со своим changelog — раз в ~1с опрашивает шардированный Redis sorted set `deadlines:{bucket}` (Pipeline Engine пишет дедлайн туда атомарно вместе с CAS-переходом, `cas_transition_and_track_deadline`), публикует retry/timeout/DLQ.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. Redis-путь протестирован против реального протокола Redis (`miniredis`, встроенный сервер, не мок транспорта).

```bash
cd services/scheduler-critical-sweep
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §2.1

| Метод | Где | Как проверено |
|---|---|---|
| `tick_sweep` | `internal/redisio/client.go::TickSweep` — обход `ZRANGEBYSCORE deadlines:{bucket} -inf now` по всем bucket | `client_test.go` — реальный `ZADD`/`ZRANGEBYSCORE` на `miniredis`, просроченные записи находятся, будущие — нет, bucket сохраняется в результате |
| `load_execution_state` | `internal/redisio/client.go::LoadExecutionState` — `HGETALL exec:{message_id}` | `client_test.go` — реальный `HSET`/`HGETALL`, парсинг `attempt`/`deadline` из строк |
| `clear_deadline` | `internal/redisio/client.go::ClearDeadline` — `ZREM` | `client_test.go` — запись реально удаляется из ZSET |
| `evaluate_retry_policy` | `internal/sweep/decision.go::EvaluateRetryPolicy` | `decision_test.go` — Retry/Exhausted/NotRetryable (см. ниже) |
| `check_execution_control` | `internal/sweep/decision.go::CheckExecutionControl` + `internal/controlsnapshot` (локальный snapshot `execution.control`) | `decision_test.go`, `controlsnapshot/snapshot_test.go` — GLOBAL PAUSED блокирует все стадии, STAGE-scope PAUSED блокирует только свою стадию, compaction (повторный `Apply`) перезаписывает |
| `publish_retry` / `publish_timeout_result` / `publish_dlq` | `internal/kafkaio/builders.go` (чистая сборка сообщений) + `internal/kafkaio/publisher.go` (реальный `franz-go` клиент) | `builders_test.go` — attempt+1 при retry, `TIMED_OUT`/`retryable=false` при timeout, `RETRY_EXHAUSTED` + `original_command` при DLQ; топики по `StageName` сверены с `service_io_contracts.md` |
| `on_manual_command` | `internal/kafkaio/consumer.go::DecodeManualCommand` + `ManualCommandConsumer` | `consumer_test.go` — round-trip через реальный `proto.Marshal`/`Unmarshal`, отклоняет запись без `stage_execution_id` и мусорные байты |

`cmd/scheduler-critical-sweep/main.go` — сборка: health-сервер `:9090`, три фоновых цикла (sweep-тик раз в 1с, консьюмер `scheduler.critical.commands`, консьюмер `execution.control` для `controlsnapshot`).

## Три исхода retry policy, не два

`service_internal_methods.md` §2.1 перечисляет `publish_timeout_result` ("ExpiredEntry без retry") и `publish_dlq` ("Exhausted") как **два разных метода на два разных исхода** — не один и тот же путь. Поэтому `RetryDecision` в `internal/sweep/types.go` — три состояния, не два: `NotRetryable` (стадия вообще не ретраится по таймауту, `MaxAttempts=0` → `TIMED_OUT` сразу) отдельно от `Exhausted` (ретраится, но попытки кончились → DLQ). В этом первом срезе все стадии используют единый `defaultRetryPolicy{MaxAttempts: 3}` (`cmd/.../main.go`) — `pipeline.schema.json` не специфицирует retry-параметры per-stage (только граф переходов по `Outcome`), различный профиль per-stage не реализован.

## Открытый вопрос — reverse index stage_execution_id → message_id (для Главного агента)

`data_infrastructure_spec.md` §2.1 документирует `deadlines:{bucket}` (ZSET, member = `stage_execution_id`) и `exec:{message_id}` (HASH, ключ по `message_id`) — но не документирует, как получить `message_id` по `stage_execution_id` из просроченной записи. Это генеральный разрыв: Critical Sweep физически не может дойти от `ExpiredEntry` до `LoadExecutionState` без этого шага.

**Решение в этом срезе** (`internal/redisio/client.go::ResolveMessageID`, `stageExecIndexKey`): предполагается дополнительный `STRING`-ключ `stage_exec_index:{stage_execution_id} → message_id`, который должен писать Pipeline Engine той же Lua-транзакцией, что и `cas_transition_and_track_deadline` (симметрично уже существующему `ZADD` в `deadlines:{bucket}`). **Это не тихо принятое решение** — Pipeline Engine и схема Runtime Redis принадлежат Главному агенту (`development_plan.md` "Координация" п.2); нужно либо подтвердить этот ключ, либо изменить member ZSET на составной (`message_id:stage_execution_id`), что потребует правки на стороне Pipeline Engine в любом случае.

Аналогичное ограничение — `BuildRetryCommand`/`BuildDlqRecord.original_command` в `internal/kafkaio/builders.go` собирают **частичную** `StageExecuteCommand`: `exec:{message_id}` не хранит `payload_ref`/`stage_extension` исходной команды, только `pipeline_version/node_id/stage_execution_id/current_state/attempt/deadline/last_applied_event_id`. Republish на стадию с частичной командой предполагает, что стадия-потребитель дочитывает контекст сообщения самостоятельно (`msgctx:{message_id}`) — это не проверено ни в одном документе LLD этой сессии.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon в этой песочнице.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher`/`ManualCommandConsumer`/`ControlSnapshotConsumer` используют настоящий `franz-go`, компилируются, но не проверялись против `kind`+Strimzi.
* **`on_manual_command` не выполняет принудительную обработку** — в этом срезе консьюмер `scheduler.critical.commands` только логирует `FORCE_TIMEOUT`/`FORCE_RETRY`; прямой путь в обход обычного sweep (загрузить `ExecutionState` по `stage_execution_id` из команды, применить `ActionTimeout`/`ActionRetry` немедленно) — механический повтор `processTick`, но не вынесен в переиспользуемую функцию в этом срезе.
* **Reverse index `stage_exec_index`** — см. "Открытый вопрос" выше; без реального Pipeline Engine, пишущего этот ключ, `ResolveMessageID` в проде будет всегда возвращать ошибку "не найдено".
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков (`swept_total`, `retried_total`, `dlq_total`).
