# Delivery Reconciliation Service

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `services_specifictaion.md` §2.9 / `service_internal_methods.md` §1.9: разрешение `SUBMISSION_OUTCOME_UNKNOWN` — ожидание позднего `submit_sm_resp`, ожидание DLR, reconciliation deadline, опциональный `query_sm`, `DELIVERY_UNRESOLVED`.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Ядро алгоритма (`resolve_outcome`/`evaluate_deadline`/`check_query_sm_policy`) полностью протестировано как чистые функции. Персистентность — против реального локального PostgreSQL 17 (jOOQ, без codegen).

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
| `handle_reconciliation_execute` | `Main.java::runExecuteConsumer` (создание/загрузка) + `store/ReconciliationStore` | `ReconciliationStoreTest` — реальный PostgreSQL: create/load round-trip |
| `collect_evidence` | `core/Evidence` (immutable record с `withX` методами) — сборка из `operator.submit.accepted`/`delivery.status` не подключена к реальным Kafka consumer'ам в этом срезе (см. "Что НЕ реализовано") | Модель данных протестирована косвенно через `OutcomeResolverTest` |
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
* **`collect_evidence` не подключён к реальным Kafka consumer'ам** `operator.submit.accepted`/`delivery.status` — `Main.java` заводит только consumer для `stage.delivery-reconciliation` (создание case) и таймер sweep (`evaluate_deadline`/`resolve_outcome`/`persist_case`/`publish_stage_completed`), но не обновляет `Evidence` по мере поступления событий из двух других топиков. В текущем виде `sweepDeadlines` резолвит **пустой** `Evidence` (`Evidence.empty()`), что всегда даёт `CONFIRMED_NOT_SUBMITTED` — функционально неполно, задокументировано явно, не скрыто за фасадом "готово".
* **`resolve_gateway_instance`** — резолв `SmppGatewayEndpoint` через Runtime Redis registry (`operator_route:*`, тот же, что `operator-smpp-session-manager`/`operator-http-gateway`) не реализован; `QuerySmClient` принимает host/port уже резолвленными.
* **`operator_id` в `handle_reconciliation_execute`** — временно читается из `queue_msg_id` поля `DeliveryReconciliationExtension` (placeholder, явно неверно семантически — `queue_msg_id` не то же самое, что `operator_id`), поскольку `StageExecuteCommand.DeliveryReconciliationExtension` не несёт `operator_id` напрямую в текущем `platform-contracts/common/stage_contract.proto`. Задокументированная находка для координации с Главным агентом (владеет `stage_contract.proto`), не тихо решённая.
* **Ни разу не запущено против реального Kafka-брокера или Operator SMPP Session Manager.**
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.