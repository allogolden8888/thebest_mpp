# A2P MPP — Platform Contracts

**Основание:** `hld.md`, `services_specifictaion.md`, `service_io_contracts.md`, `service_internal_methods.md`, `data_infrastructure_spec.md`
**Артефакт:** `platform-contracts/` — реальные `.proto`-схемы, первый слой LLD. Всё остальное (точные сигнатуры методов, алгоритмы, error-таксономия по каждому сервису) опирается на эти схемы и делается следующим шагом, сервис за сервисом.

---

# 1. Структура репозитория

```text
platform-contracts/
  common/
    enums.proto             — все общие enum'ы платформы
    types.proto              — Money, PartnerContext, per-channel payload, StagePayloadRef
    stage_contract.proto     — StageExecuteCommand / StageCompletedEvent (общий контракт, HLD §6)
  events/
    message_events.proto     — IncomingMessage, MessageLifecycleEvent, DeliveryStatusEvent
    operator_events.proto    — OperatorSubmitAccepted, OperatorDlr
    billing_events.proto     — LedgerEvent
    config_and_control.proto — ConfigChangeEvent, ExecutionControlRecord
    scheduler_events.proto   — SchedulerCriticalCommand, SchedulerHoldCommand, SchedulerBackgroundTask, NotificationRetryTask
    dlq_record.proto         — DlqRecord
  grpc/
    operator_gateway.proto   — OperatorSubmitService, OperatorQuerySmService
    partner_gateway.proto    — PartnerDeliverSmService
    internal_control.proto   — ConfigService, ExecutionControlService, ReplayService
```

Соответствует репозиторию `platform-contracts` из `services_specifictaion.md` §10. Codegen — на все три стека: Rust (`prost`/`tonic`), Java (`protoc-gen-grpc-java`), Go (`protoc-gen-go` + `protoc-gen-go-grpc`), как зафиксировано в §9.1/§9.2 того же документа.

---

# 2. Принципы, применённые в схемах

**Тело сообщения не дублируется.** `StageExecuteCommand` несёт `StagePayloadRef` (ссылку на `msgctx:{message_id}` в Runtime Redis), а не body — решение зафиксировано в `service_internal_methods.md` §0 и было ключевым для capacity-модели (маленькие Kafka-сообщения). Единственное место, где body передаётся полностью — `IncomingMessage`.

**Расширения по стадии — `oneof`, а не общий набор опциональных полей.** `StageExecuteCommand.stage_extension` и `StageCompletedEvent.stage_result` — per-stage сообщения в `oneof`, а не плоский список необязательных полей на все стадии сразу. Это форсирует на уровне схемы то, что уже зафиксировано текстом в HLD/методах — какое поле кому принадлежит.

**Деньги — целочисленно.** `Money` — `currency_code` + `minor_units` (int64), не float. Обоснование то же, что для всей финансовой модели (HLD §15) — избежать ошибок округления.

**Канал и протокол — независимые enum'ы.** `Channel` и `Protocol` не связаны в схеме напрямую (нет enum'а "SMS-over-SMPP") — ровно то разделение измерений, которое зафиксировано в HLD §3.1.1.

**`config.changes` несёт непрозрачный JSON, не typed proto.** Единственное сознательное исключение из "всё типизировано": `ConfigChangeEvent.payload_json` — потому что форма конфигурации отличается по каждому `entity_type` и меняется чаще, чем стоило бы жёсткой protobuf-схемы каждого варианта. Версионирование обеспечивает поле `version`, а не protobuf-контракт содержимого.

**`DlqRecord` несёt полную исходную команду.** Не только метаданные — `original_command: StageExecuteCommand` целиком, чтобы Replay Service мог сделать republish, не запрашивая ничего дополнительно (HLD §20).

**Совместимость.** Правила из `services_specifictaion.md` §9.1 применяются без изменений: `schema_version` неявно — через growth-only field numbering; unknown fields игнорируются (стандартное поведение proto3); удаление существующих полей запрещено — только `reserved`; breaking change — новая версия пакета (`mpp.common.v2` и т.д.) или новый топик.

---

# 3. Соответствие Kafka-топик → protobuf-сообщение

Полная спецификация топиков (партиции, retention, producer/consumer) — `data_infrastructure_spec.md` §3. Здесь — только привязка к конкретной схеме.

| Топик | Сообщение |
|---|---|
| `incoming.messages` | `mpp.events.v1.IncomingMessage` |
| `stage.destination-resolution` | `mpp.common.v1.StageExecuteCommand` (`stage_extension = destination_resolution`) |
| `stage.policy` | `StageExecuteCommand` (`stage_extension = policy`) |
| `stage.billing` | `StageExecuteCommand` (`stage_extension = billing`) |
| `stage.routing` | `StageExecuteCommand` (`stage_extension = routing`) |
| `stage.delivery` | `StageExecuteCommand` (`stage_extension = delivery`) |
| `stage.delivery-reconciliation` | `StageExecuteCommand` (`stage_extension = delivery_reconciliation`) |
| `stage.completed` | `mpp.common.v1.StageCompletedEvent` (`stage_result` — соответствующий стадии вариант) |
| `delivery.status` | `mpp.events.v1.DeliveryStatusEvent` |
| `message.lifecycle` | `mpp.events.v1.MessageLifecycleEvent` |
| `operator.submit.accepted` | `mpp.events.v1.OperatorSubmitAccepted` |
| `operator.dlr` / `operator.dlr.unresolved` / `operator.dlr.dlq` | `mpp.events.v1.OperatorDlr` |
| `notification.retry` | `mpp.events.v1.NotificationRetryTask` |
| `billing.ledger` | `mpp.events.v1.LedgerEvent` |
| `config.changes` | `mpp.events.v1.ConfigChangeEvent` |
| `execution.control` | `mpp.events.v1.ExecutionControlRecord` |
| `scheduler.critical.commands` | `mpp.events.v1.SchedulerCriticalCommand` |
| `scheduler.standard.commands` | `mpp.events.v1.SchedulerHoldCommand` |
| `scheduler.background.commands` | `mpp.events.v1.SchedulerBackgroundTask` |
| `stage.*.dlq` (5 топиков) | `mpp.events.v1.DlqRecord` |
| `scheduler.standard.state.changelog`, `scheduler.background.state.changelog`, `message-state.changelog` | внутренние Kafka Streams state store, сериализация — предмет LLD конкретного сервиса (Standard/Background Lane, Message State Resolver), не часть platform-contracts |

## gRPC

| Вызов | Сервис/RPC |
|---|---|
| Delivery → Operator SMPP Session Manager / Operator HTTP Gateway | `mpp.grpc.v1.OperatorSubmitService/Submit` |
| Delivery Reconciliation → Operator SMPP Session Manager | `mpp.grpc.v1.OperatorQuerySmService/QuerySm` |
| Partner Notification Service → Partner SMPP Gateway | `mpp.grpc.v1.PartnerDeliverSmService/DeliverSm` |
| Backoffice API → Configuration Service | `mpp.grpc.v1.ConfigService/*` |
| Backoffice API → Execution Control Service; Billing Reconciliation → Execution Control Service | `mpp.grpc.v1.ExecutionControlService/*` |
| Backoffice API → Replay Service | `mpp.grpc.v1.ReplayService/RequestReplay` |

---

# 4. Что осталось незакрытым в этой версии контрактов

Осознанно не специфицировано здесь — предмет следующего шага LLD, по каждому сервису отдельно:

* ~~точная форма `payload_json` в `ConfigChangeEvent` по каждому `entity_type`~~ — закрыто, см. `config_schemas/` (9 JSON Schema файлов + `validate_all.py`, 20/20 примеров совпали с ожиданием, включая семантические проверки за пределами JSON Schema — обход графа пайплайна, ссылочная целостность `active_route_id`);
* ~~формальная state machine допустимых переходов `MessageLifecycleStatus`~~ — закрыто, см. `state_machines.md` §1 (`state_machines/message_lifecycle.py`, 10/10 тестов);
* маппинг операторских DLR-кодов в `normalized_status` (per-operator, LLD DLR Manager);
* Lua-скрипты для атомарных операций Runtime Redis / Billing Redis (CAS+deadline, атомарный charge) — алгоритм и гарантии (fencing по `account_epoch`, идемпотентность по `charge_id`) формализованы и протестированы в `state_machines.md` §3 (`state_machines/billing_account_state.py`, 8/8 тестов); сама реализация как Lua/Redis Function ещё не написана;
* ~~alghoritm template matching с нормализацией обхода банвордов~~ — закрыто, см. `policy_matching/` (`template_matching.py` 7/7 тестов, `banword_normalization.py` 7/7 тестов, на настоящем `pyahocorasick` и реальных данных из чата).

Дополнительно закрыто сверх исходного списка: гистерезис Execution Control (ACTIVE/DEGRADED/PAUSED), ранее существовавший только как параметры без алгоритма — см. `state_machines.md` §2 (`state_machines/execution_control_hysteresis.py`, 5/5 тестов).

Список не исчерпывающий — это те точки, которые уже видны как обязательные из существующих 6 документов.
