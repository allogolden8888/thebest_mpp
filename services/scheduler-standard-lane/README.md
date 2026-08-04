# Scheduler — Standard Lane

**Основание:** `development_plan.md` — Субагент 1, Фаза 3.2. `services_specifictaion.md` §3.1: PAUSED hold, controlled release, reconciliation deadlines — реальная transactional stateful нагрузка на явную команду (не наблюдение за всем потоком, как раньше у Critical Sweep), поэтому Kafka Streams Processor API + RocksDB обоснованы.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25, см. ниже). Топология целиком прогоняется через `TopologyTestDriver` (официальный offline-инструмент Kafka Streams — не мок, реальная обработка через state store и punctuator, без брокера).

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
cd services/scheduler-standard-lane
mvn test
```

## Проверено кодревью (CODE_REVIEW.md)

Standard Lane имела 2 CRITICAL и 3 HIGH находки — все, кроме одной (частично), закрыты.

* **CRITICAL #1 — необработанный `IllegalArgumentException` на `ExecutionControlState.UNSPECIFIED`/`UNRECOGNIZED` крашил `GlobalStreamThread`, и поскольку `addGlobalStore` реплеит `execution.control` с нуля при каждом рестарте, одна такая запись давала перманентный crash loop.** `topology/ControlUpdateProcessor.java` — `Enum.valueOf(...)` заменён на `toState(...)`, safe switch-маппинг, возвращающий `null` (не throw) для незнакомых значений; такая запись логируется и пропускается, обработка остальных продолжается. Тот же defensive паттерн, что уже был в `scheduler-background-lane`. Тест: `StandardLaneTopologyTest.unspecifiedControlStateIsLoggedAndSkippedNotCrashing` — реальный `TopologyTestDriver` прогон через `UNSPECIFIED`-запись, подтверждает, что топология продолжает нормально работать после неё.
* **CRITICAL #2 — `ControlSnapshot` проверял только GLOBAL/STAGE; PARTNER/PARTNER_STAGE/OPERATOR_ROUTE-паузы никогда не блокировали release, хотя store-сторона уже хранила их.** `core/ControlSnapshot.java` — новые overload'ы `isPaused(stageName, scope, scopeId)`/`rampRate(stageName, scope, scopeId)`, проверяющие ВСЕ применимые ключи (GLOBAL, STAGE, собственный scope held item'а, и производный PARTNER_STAGE для PARTNER-scoped items — конвенция `scope_id="{partner_id}:{stage}"`, та же, что `billing-reconciliation`'s `ExecutionControlClient`). `topology/HoldCommandProcessor.releaseTick()` теперь передаёт `item.scope()`/`item.scopeId()` в обе проверки, per-item, не один раз на весь batch стадии. Тесты: 5 новых в `ControlSnapshotTest` (PARTNER/OPERATOR_ROUTE/PARTNER_STAGE-производный ключ/rampRate min/isActive overload) + `StandardLaneTopologyTest.partnerScopedPauseBlocksReleaseEvenWhenGlobalAndStageAreActive` (end-to-end через реальную топологию).
* **HIGH #3 — poison-pill: `SchedulerHoldCommand` с неизвестным `stage_name` не имел case в `Topics.stageTopic()`'s switch, throw происходил ВНУТРИ `TopicNameExtractor` на publish, ДО `store.delete(...)` — застревал в store навсегда, крашил каждый следующий тик/рестарт.** `topology/Topics.hasTopic(...)` — та же switch-логика, что `stageTopic()` (один источник истины, ловит его `IllegalArgumentException`, не дублирует список). `HoldCommandProcessor.process()` отклоняет неизвестный `stage_name` ДО `store.put(...)` — поison item физически не может попасть в store. Defense-in-depth: `store.delete(item.stageExecutionId())` в `releaseTick()` теперь идёт ДО `publishRelease(...)`, не после. Тест: `StandardLaneTopologyTest.unknownStageNameHoldCommandIsRejectedNotStored`.
* **HIGH #4 — held items не сортировались по `heldAtEpochMs`; RocksDB-backed store итерирует в KEY order (по `stageExecutionId`), не по времени hold'а — заявленная FIFO/no-starvation гарантия `FairScheduler` не выполнялась.** `HoldCommandProcessor.groupByStage()` теперь сортирует каждый список по `heldAtEpochMs` перед передачей в `FairScheduler`/`ReleaseBatchSelector`. Тест: `StandardLaneTopologyTest.releaseOrderFollowsHeldAtTimeNotStageExecutionIdKeyOrder` — намеренно конструирует `stageExecutionId`, чей алфавитный порядок ПРОТИВОПОЛОЖЕН порядку `held_at`, и подтверждает, что released-первым item — тот, что held раньше, не тот, чей ключ меньше.
* **HIGH #5 — `bucketsByStage` — обычное instance-поле, а Kafka Streams создаёт один `HoldCommandProcessor` на каждую assigned-партицию `scheduler.standard.commands` (партиционирован по `message_id`, не по `stage_name`) — задуманный "один bucket на стадию" лимит существует отдельно на каждую партицию, эффективный aggregate rate может быть кратен документированному. НЕ полностью исправлено** — полноценный фикс требует rate limiter, разделяемого между партициями/репликами (например Redis CAS, тот же паттерн, что `pipeline-engine` уже использует для `ExecutionState`, или Redis-Lua паттерн `billing-service`/`partner-rest-receiver`) — архитектурно самая сложная находка в группе, не сделана в этом проходе. Применена задокументированная ЧАСТИЧНАЯ митигация: `BUCKET_CAPACITY`/`BUCKET_REFILL_PER_SECOND` делятся на реальное число партиций топика (8, `data_infrastructure_spec.md`, не догадка) — сужает наихудший single-instance случай (все партиции на одном инстансе), но НЕ чинит multi-replica или partial-rebalance случаи — см. развёрнутый комментарий в `HoldCommandProcessor.java`.

`mvn test`: **31/31** (было 22/22 до этого прохода).

## Что реализовано по service_internal_methods.md §2.2

| Метод | Где | Как проверено |
|---|---|---|
| `on_hold_command` | `topology/HoldCommandProcessor.process()` — пишет `HeldItem` в `standard-holds-store` (persistent, keyed by `stage_execution_id`) | `StandardLaneTopologyTest` — hold command реально попадает в state store через `TopologyTestDriver` |
| `on_control_state_change` | `topology/ControlUpdateProcessor` (Kafka Streams **global store** на `execution.control`) + `core/ControlSnapshot` | `ControlSnapshotTest` (чистая логика: GLOBAL PAUSED доминирует, STAGE-scope изолирован, min по иерархии для ramp rate) + `StandardLaneTopologyTest` (реальный проход через global store processor) |
| `release_backlog_batch` | `core/ReleaseBatchSelector.selectBatch` | `ReleaseBatchSelectorTest` — ramp rate ограничивает batch как долю backlog, token bucket независимо ограничивает сверху, полный ramp+токены отпускают весь backlog |
| `apply_fair_scheduling` | `core/FairScheduler.applyFairScheduling` (round-robin по `scope:scope_id`) | `FairSchedulerTest` — партнёр с большим backlog не вытесняет партнёра с меньшим; проверено также внутри `ReleaseBatchSelectorTest` (fair scheduling внутри batch) |
| `apply_token_bucket` | `core/TokenBucket` | `TokenBucketTest` — permit/deny по ёмкости, пополнение со временем, не превышает ёмкость |
| `publish_release` | `topology/ReleaseCommandBuilder` (чистая сборка) + dynamic sink (`StageTopicNameExtractor`, топик по `stage_name` из уже собранной команды) | `StandardLaneTopologyTest` — released item реально уходит на `stage.billing`/`stage.routing` в зависимости от `stage_name`, и только когда `execution.control` разрешает (не PAUSED) |
| `on_reconciliation_deadline` | `topology/HoldCommandProcessor.reconciliationDeadlineTick` — отдельный punctuator (30с) | Не покрыт отдельным тестом в TopologyTestDriver (тот же 1-секундный release-punctuator уже освобождает `DELIVERY_RECONCILIATION` при ACTIVE control раньше, чем успевает сработать 30-секундный тик — см. "Интерпретация" ниже); реализация проверена вручную чтением кода, не тестом |
| `checkpoint_state` / `restore_from_changelog` | Встроено в Kafka Streams: `Stores.persistentKeyValueStore` автоматически создаёт changelog-топик и восстанавливает store при rebalance/рестарте; `addGlobalStore` реплеит `execution.control` с начала при каждом старте, воссоздавая `ControlSnapshot` тем же `ControlUpdateProcessor`, что и live-обновления | Не тестировано отдельно (потребовало бы реального рестарта процесса/кластера, не только `TopologyTestDriver`) |

## Интерпретация `on_reconciliation_deadline`

`service_internal_methods.md` §2.2 перечисляет `on_reconciliation_deadline` как отдельный метод от `release_backlog_batch`, но не детализирует, чем именно триггер отличается по семантике. Реализовано как периодический **nudge** (`reconciliationDeadlineTick`, раз в 30с) для `DELIVERY_RECONCILIATION`-holds, который republish'ит команду, **не удаляя** запись из store (в отличие от `release_backlog_batch`, который release'ит и удаляет) — предположение: Reconciliation должен переопрашивать оператора по таймеру независимо от того, снят ли hold обычным release. Это интерпретация, не подтверждённая явно в LLD — задокументирована как находка, не скрыта.

## Дублирование `stateful-processing-runtime`

`services_specifictaion.md` §3.2 описывает общий internal Java-модуль `stateful-processing-runtime` (naming changelog, transactional topology, restore listener, rebalance hooks, fencing, recovery metrics) для Scheduler (обоих lane) и Message State Resolver. Message State Resolver — сервис Главного агента (`development_plan.md` "Распределение между агентами"), в отдельной директории `services/message-state-resolver/`, которой ещё нет на момент этого среза. Вынести реальный общий Maven-модуль сейчас означало бы либо создавать директорию за пределами границ Субагента 1, либо координацию с Главным агентом до того, как обе стороны реализованы — вместо этого логика (naming топиков, persistent store + auto-changelog) продублирована локально в `topology/`. Консолидация в настоящий shared-модуль — задача для координации после того, как обе стороны существуют.

## Реальная сквозная проверка контрактов, не только описание

`src/main/java/uz/mpp/platformcontracts/` — Java-код, сгенерированный `protoc --java_out` из тех же `platform-contracts/common/{enums,types,stage_contract}.proto`, `events/scheduler_events.proto`, `events/config_and_control.proto`, что уже провалидированы `protoc --descriptor_set_out`. Регенерация:

```bash
cd services/scheduler-standard-lane
protoc --proto_path=../../platform-contracts --java_out=src/main/java \
  common/enums.proto common/types.proto common/stage_contract.proto \
  events/scheduler_events.proto events/config_and_control.proto
```

## Открытый вопрос — та же проблема реконструкции команды, что у Critical Sweep

`SchedulerHoldCommand` несёт только `message_id`, `stage_execution_id`, `scope`, `scope_id`, `stage_name`, `held_at` — не полную `StageExecuteCommand` (`payload_ref`/`stage_extension`). `topology/ReleaseCommandBuilder` собирает частичную команду. См. `services/scheduler-critical-sweep/README.md` "Открытый вопрос" — тот же кросс-сервисный разрыв в Runtime Redis / Kafka-контрактах, нужна координация с Главным агентом (владеет Pipeline Engine).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon в этой песочнице.
* **Ни разу не запущено против реального Kafka-брокера** — только `TopologyTestDriver`, не `kind`+Strimzi.
* **`exactly_once_v2`** указан в `StreamsConfig` (`Main.java`), но транзакционное поведение не проверялось интеграционно (требует реального брокера с транзакционным координатором).
* **Per-partner/per-scope token bucket ёмкость/refill** — единый профиль на все стадии (не задокументирован отдельной JSON Schema в `config_schemas/`), не per-partner. Также см. "Проверено кодревью" HIGH #5 выше — bucket сейчас per-partition, не разделяется между репликами/партициями (частичная, не полная митигация).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков (`held_total`, `released_total` и т.п.).
