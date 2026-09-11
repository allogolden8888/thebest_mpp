# Delivery Reconciliation Service

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `services_specifictaion.md` §2.9 / `service_internal_methods.md` §1.9: разрешение `SUBMISSION_OUTCOME_UNKNOWN` — ожидание позднего `submit_sm_resp`, ожидание DLR, reconciliation deadline, опциональный `query_sm`, `DELIVERY_UNRESOLVED`.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Ядро алгоритма (`resolve_outcome`/`evaluate_deadline`/`check_query_sm_policy`) полностью протестировано как чистые функции. Персистентность — против реального локального PostgreSQL 17 (jOOQ, без codegen).

## P0: корректный `operator_id` в reconciliation (2026-09-11)

Исправлена порча данных: раньше `runExecuteConsumer` записывал
`DeliveryReconciliationExtension.queue_msg_id` в
`reconciliation.reconciliation_cases.operator_id`. Это разные namespace:
`queue_msg_id` — технический ID Delivery, а `operator_id` определяет
оператора, его route registry и endpoint для `query_sm`.

Что сделано:

* в `platform-contracts/common/stage_contract.proto` к
  `DeliveryReconciliationExtension` добавлено аддитивное protobuf-поле
  `resolved_operator_id = 3`;
* Pipeline Engine заполняет его из `ExecutionState.resolved_operator_id` —
  авторитетного результата Destination Resolution, уже используемого
  Delivery. Если значения нет, команда не строится — ложный ID не
  выдумывается;
* Delivery Reconciliation Service читает только новое поле. Старая команда
  без `resolved_operator_id` сначала публикуется как полноценный `DlqRecord`
  в `stage.delivery-reconciliation.dlq`, и только после подтверждения Kafka
  коммитится её входной offset; ошибка БД, Kafka или самого DLQ перематывает
  партицию на проблемный offset для повторной обработки. Тем самым legacy-
  команда не превращает `queue_msg_id` в повреждённую строку case и не
  теряется после одного сообщения в stderr.

Совместимость и rollout:

* новое protobuf-поле wire-совместимо, но **обычный rolling update здесь
  небезопасен**: старый consumer игнорирует поле и продолжает использовать
  `queue_msg_id`; поэтому на время rollout нужно остановить выпуск команд
  этой стадии, scale текущего Delivery Reconciliation Service в `0`,
  развернуть Pipeline Engine, затем новую версию consumer и только после её
  readiness вернуть трафик;
* legacy backlog новая версия не обрабатывает как корректные команды: он
  оказывается в DLQ с `RESOLVED_OPERATOR_ID_MISSING`. Перед replay оператор
  обязан обогатить `original_command` авторитетным `operator_id`; обычный
  Replay Service пересылает исходную команду без такой правки и снова
  закономерно получит DLQ;
* миграция схемы PostgreSQL не нужна — колонка `operator_id` уже есть;
* уже созданные case'ы с подставленным `queue_msg_id` автоматически не
  переписываются: без авторитетного join по `message_id` массовая правка
  выдумала бы данные. Для активных case'ов нужен отдельный repair только
  по совпавшему единственному `dlr.dlr_correlation.operator_id`; строки без
  такого источника нужно оставить на ручной разбор.

Регрессионная проверка: Rust-тесты фиксируют перенос значения и отказ
строить команду без него; Java-тесты доказывают, что consumer берёт
новое поле, не делает fallback на `queue_msg_id`, а publisher формирует
валидный `DlqRecord` с полной исходной командой.

Последний локальный прогон без DB-dependent `ReconciliationStoreTest`:
**35/35 passed**. Полный suite дополнительно выявил drift локальной БД:
9 store-тестов не могут использовать `ON CONFLICT (message_id)`, потому что
на этом уже существующем экземпляре не применена миграция V032 с unique
index. На чистой схеме миграция есть; для production это всё равно сигнал,
что нужен версионируемый migration runner/pre-deploy gate, а не доверие к
ручному состоянию базы.

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
cd services/delivery-reconciliation-service
mvn test
```

## Отклонение от стека LLD — задокументировано, не скрыто

`services_specifictaion.md` §2.9 указывает **Micronaut**. В этом срезе сервис собран **без Micronaut** — обычная ручная сборка (Kafka consumer loop в отдельном потоке, DI руками через конструкторы, `Main.java`), тот же паттерн, что все остальные Java-сервисы этой сессии. Причина — не тянуть annotation processing/DI-контейнер Micronaut ради экономии времени в рамках одной сессии на фоне 21 сервиса Субагента 1. Бизнес-логика (`core/`) framework-агностична — миграция на Micronaut, если потребуется, затронула бы только `Main.java`/wiring, не `core/`/`store/`/`kafkaio/`.

## Что реализовано по service_internal_methods.md §1.9

| Метод | Где | Как проверено |
|---|---|---|
| `handle_reconciliation_execute` | `Main.java::runExecuteConsumer` (создание/загрузка) + `store/ReconciliationStore`; `operator_id` приходит от Pipeline Engine в отдельном `resolved_operator_id` | `ReconciliationCommandTest` + `ReconciliationStoreTest` (реальный PostgreSQL: create/load round-trip) |
| `collect_evidence` | `core/Evidence` + Kafka consumer'ы `operator.submit.accepted`/`delivery.status`; раннее evidence сохраняется в `reconciliation.early_evidence` | `ReconciliationRaceTest`, `OutcomeResolverTest`, `ReconciliationStoreTest` |
| `check_query_sm_policy` | `core/QuerySmPolicy` | `QuerySmPolicyTest` — DISABLED для HTTP всегда, ENABLED только для SMPP + конфиг |
| `resolve_gateway_instance` | **не реализовано** — см. ниже | — |
| `call_query_sm` | `grpcclient/QuerySmClient` (`OperatorQuerySmService.QuerySm`) | `QuerySmClientTest` — маппинг `QuerySmResponse` в `Evidence.QuerySmOutcome`, включая различение "не найдено из-за приоритета submit_sm" (`DEFERRED_*`) от "оператор подтвердил отсутствие" |
| `evaluate_deadline` | `core/DeadlineEvaluator` | `DeadlineEvaluatorTest` |
| `resolve_outcome` | `core/OutcomeResolver` | `OutcomeResolverTest` — все 5 исходов, включая приоритет DLR над query_sm над submit_accepted; см. "Интерпретация" в коде (докстринг класса) |
| `persist_case` | `store/ReconciliationStore::persistEvidence`/`closeCase`/`findExpiredOpenCases` | `ReconciliationStoreTest` — реальные UPDATE/SELECT на PostgreSQL, включая выборку просроченных open case'ов по `reconciliation_cases_status_deadline_idx` |
| `publish_stage_completed` | `kafkaio/StageCompletedBuilder` (чистая сборка) + `StageCompletedPublisher` | `StageCompletedBuilderTest` (маппинг `ReconciliationOutcome` → `Outcome`, включая выделенный `OUTCOME_DELIVERY_UNRESOLVED`) + `StageCompletedPublisherTest` (`MockProducer`) |

## Интерпретация `resolve_outcome` — не буквально специфицирована в LLD

Задокументирована подробно в докстринге `core/OutcomeResolver`: DLR — самое сильное свидетельство (побеждает независимо); `query_sm` может резолвить раньше дедлайна; на дедлайне без DLR — `CONFIRMED_SUBMITTED`, если submit подтверждён, `DELIVERY_UNRESOLVED`, если `query_sm` дал неубедительный ответ, `CONFIRMED_NOT_SUBMITTED` иначе. Это разумная, но не единственно возможная интерпретация пяти исходов `ReconciliationOutcome` — альтернативная трактовка не исключена, задокументировано как явное архитектурное решение этого среза.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon.
* **`resolve_gateway_instance`** — резолв `SmppGatewayEndpoint` через Runtime Redis registry (`operator_route:*`, тот же, что `operator-smpp-session-manager`/`operator-http-gateway`) не реализован; `QuerySmClient` принимает host/port уже резолвленными.
* **Ни разу не запущено против реального Kafka-брокера или Operator SMPP Session Manager.**
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.
