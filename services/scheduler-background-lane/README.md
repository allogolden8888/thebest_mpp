# Scheduler — Background Lane

**Основание:** `development_plan.md` — Субагент 1, Фаза 3.2. `services_specifictaion.md` §3.1: DLR correlation retry, notification retry, фоновые задачи — Kafka Streams Processor API обоснован той же причиной, что у Standard Lane (реальная transactional stateful нагрузка на явную команду).

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Топология прогоняется через `TopologyTestDriver`.

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
cd services/scheduler-background-lane
mvn test
```

## Что реализовано по service_internal_methods.md §2.3

| Метод | Где | Как проверено |
|---|---|---|
| `on_background_command` | `topology/BackgroundCommandProcessor.process()` — пишет `BackgroundTask` в `background-tasks-store` (persistent, keyed by `source_event_id`) | `BackgroundLaneTopologyTest` |
| `tick_delay_queue` | `core/DueTaskSelector.selectDue` (чистая функция) + punctuator (1с) в `BackgroundCommandProcessor.tick` | `DueTaskSelectorTest` — отбирает только задачи с наступившим `due_at`; `BackgroundLaneTopologyTest` — задача с будущим `due_at` не диспатчится |
| `dispatch_dlr_retry` | `topology/DispatchBuilder.buildDlrRetryWakeup` + именованный sink `dlr-sink` → `operator.dlr.unresolved` | `BackgroundLaneTopologyTest.dlrRetryTaskDispatchedOnceDue` — `attempt` увеличивается, топик верный |
| `dispatch_notification_retry` | `topology/DispatchBuilder.buildNotificationRetry` + именованный sink `notification-sink` → `notification.retry` | `BackgroundLaneTopologyTest.notificationRetryTaskDispatchedToNotificationTopic` |
| `checkpoint_state` / `restore_from_changelog` | Встроено в Kafka Streams (`Stores.persistentKeyValueStore` — автоматический changelog + restore) | Не тестировано отдельно — требует реального рестарта, не только `TopologyTestDriver` |

Диспатч использует **два фиксированных именованных sink** (`dlr-sink`/`notification-sink`, `context.forward(record, childName)`), не dynamic `TopicNameExtractor` — у двух типов задач разная protobuf-схема payload (`SchedulerBackgroundTask` vs `NotificationRetryTask`), не только разное имя топика, поэтому dynamic extractor, парсящий один и тот же байтовый формат, был бы некорректен.

## Открытые вопросы (задокументированы, не скрыты)

1. **`dispatch_dlr_retry` payload.** В `platform-contracts` нет отдельного protobuf-сообщения "DLR unresolved wake-up" — только `OperatorDlr` (сырое событие оператора) и `SchedulerBackgroundTask` (сама задача). Реализовано как republish самой `SchedulerBackgroundTask` с `attempt+1` на `operator.dlr.unresolved` — предположение, что DLR Manager читает именно этот формат и повторяет `lookup_correlation` по `source_event_id`. Не подтверждено кодом DLR Manager (сервис Главного агента).
2. **`dispatch_notification_retry` — отсутствующий `message_id`.** `SchedulerBackgroundTask` несёт только `source_event_id` (event_id исходного `message.lifecycle`), не `message_id`. `NotificationRetryTask.message_id` в этом срезе не заполняется (protobuf default = "") — Partner Notification Service должен уметь резолвить `message_id` из `lifecycle_event_id` самостоятельно; это не проверено ни в одном документе LLD этой сессии.
3. **Дублирование `stateful-processing-runtime`** — тот же компромисс, что в `services/scheduler-standard-lane/README.md` ("Дублирование `stateful-processing-runtime`"): общий Java-модуль с Message State Resolver (Главный агент) не вынесен отдельно в этом срезе, логика продублирована локально.

## Реальная сквозная проверка контрактов

`src/main/java/uz/mpp/platformcontracts/` сгенерирован из `platform-contracts/common/{enums,types,stage_contract}.proto`, `events/scheduler_events.proto` (`SchedulerBackgroundTask`, `NotificationRetryTask`), `events/operator_events.proto`. Регенерация:

```bash
cd services/scheduler-background-lane
protoc --proto_path=../../platform-contracts --java_out=src/main/java \
  common/enums.proto common/types.proto common/stage_contract.proto \
  events/scheduler_events.proto events/operator_events.proto
```

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — только `TopologyTestDriver`.
* **`exactly_once_v2`** указан в конфигурации, не проверен интеграционно (нужен реальный транзакционный брокер).
* **Нет отдельной обработки `evaluate_correlation_window`/expire** — это ответственность DLR Manager (Главный агент), Background Lane только диспатчит по `due_at`, не решает, когда задача должна перестать ретраиться (кроме `BackgroundTask.isExpired`, который в этом срезе не используется в `tick()` — задача диспатчится независимо от `deadline`, экспирацию должен применять источник, публикующий `scheduler.background.commands`, повторно решая не регистрировать новую попытку).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.
