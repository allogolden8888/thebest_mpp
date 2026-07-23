# A2P Message Processing Platform

## High-Level Design

**Версия:** 1.0 RC
**Статус:** для повторной оценки CTO
**Дата:** 21 июля 2026 года

---

## 1. Назначение документа

Документ описывает высокоуровневую архитектуру A2P-платформы обработки SMS-сообщений.

Платформа должна:

* принимать сообщения партнёров по SMPP и REST;
* выполнять операторские и партнёрские проверки;
* тарифицировать сообщения;
* выбирать оператора и маршрут;
* отправлять сообщения в SMSC;
* принимать и нормализовывать DLR;
* возвращать статусы партнёрам;
* хранить оперативную историю;
* предоставлять отчётность и административное управление;
* выдерживать пиковую нагрузку до 20 000 входящих сообщений в секунду.

Документ фиксирует границы сервисов, потоки данных, модели состояния, гарантии обработки и ключевые компромиссы. Внутренние алгоритмы, точные схемы таблиц, protobuf-контракты и инфраструктурный sizing относятся к Low-Level Design.

---

# 2. Архитектурная схема

```mermaid
flowchart TB

%% ==========================================================
%% EXTERNAL SYSTEMS
%% ==========================================================

subgraph External["Внешние системы"]
    RestPartners["REST-клиенты партнёров"]
    SmppPartners["SMPP-партнёры"]
    Operators["Мобильные операторы / SMSC"]
    BackofficeUsers["Пользователи Backoffice"]
end

%% ==========================================================
%% ENTRY AND SMPP SESSION LAYER
%% ==========================================================

subgraph Entry["Приём трафика (Partner-facing)"]
    RestReceiver["Partner REST Receiver<br/><br/>Stateless<br/>Protocol validation<br/>Authentication<br/>Rate limiting<br/>Generate message_id / trace_id"]

    PartnerGateway["Partner SMPP Gateway<br/><br/>StatefulSet<br/>bind / unbind<br/>submit_sm / deliver_sm<br/>query_sm партнёра<br/>enquire_link<br/>Stable instance identity"]
end

subgraph OperatorEgress["Исходящий трафик (Operator-facing)"]
    OperatorSession["Operator SMPP Session Manager<br/><br/>Stateful<br/>Operator SMPP binds<br/>Window / throttling<br/>Reconnect<br/>submit_sm_resp / DLR<br/>Priority: submit_sm over query_sm<br/>tps_limit — локальный инвариант владеющего инстанса"]

    OperatorHttpGateway["Operator HTTP Gateway<br/><br/>Stateful ownership (тот же registry-паттерн, что SMPP)<br/>Исходящий submit по HTTP<br/>Входящий DLR webhook<br/>tps_limit — локальный инвариант владеющего инстанса"]
end

RestPartners --> RestReceiver
SmppPartners <--> PartnerGateway
OperatorSession <--> Operators
OperatorHttpGateway <-->|"HTTP submit +<br/>DLR webhook"| Operators

%% ==========================================================
%% MAIN KAFKA TOPICS
%% ==========================================================

subgraph KafkaMain["Kafka: основной поток"]
    Incoming["incoming.messages<br/><br/>Immutable message context"]

    DestinationResolutionTopic["stage.destination-resolution"]
    PolicyTopic["stage.policy"]
    BillingTopic["stage.billing"]
    RoutingTopic["stage.routing"]
    DeliveryTopic["stage.delivery"]
    ReconciliationTopic["stage.delivery-reconciliation"]

    StageCompleted["stage.completed"]

    DeliveryStatus["delivery.status"]
    MessageLifecycle["message.lifecycle"]

    OperatorSubmitAccepted["operator.submit.accepted"]
    OperatorDLR["operator.dlr"]
    OperatorDLRUnresolved["operator.dlr.unresolved"]
    OperatorDLRDLQ["operator.dlr.dlq"]
    NotificationRetryTopic["notification.retry"]

    BillingLedgerTopic["billing.ledger"]

    ConfigChanges["config.changes<br/><br/>Compacted"]

    ExecutionControlTopic["execution.control<br/><br/>Compacted<br/>ACTIVE / DEGRADED / PAUSED"]
end

RestReceiver -->|"Publish успешно,<br/>затем HTTP ACK"| Incoming
PartnerGateway -->|"Publish успешно,<br/>затем submit_sm_resp"| Incoming

%% ==========================================================
%% PIPELINE
%% ==========================================================

subgraph PipelineLayer["Pipeline и бизнес-стадии"]
    Pipeline["Pipeline Engine<br/><br/>Sequential conditional graph<br/>Без fan-out / fan-in<br/>Versioned local snapshots<br/>Resolve next stage<br/>Atomic CAS transition"]

    DestinationResolution["Destination Resolution Service<br/><br/>Резолв оператора по номерному диапазону<br/>Channel-нейтральное имя (SMS сегодня,<br/>ESP/platform — зарезервировано под email/push)<br/>Repeatable computation"]

    Policy["Policy Service<br/><br/>Template matching + категоризация<br/>Anti-spam по частоте<br/>Time-of-day по категории<br/>Consent-блэклисты (категория, отправитель)<br/>Банворды + нормализация обхода<br/>Sender validation<br/>Repeatable computation"]

    Billing["Billing Service<br/><br/>Tariff resolution<br/>Cold / Hot billing<br/>Idempotent financial operation"]

    Routing["Routing Service<br/><br/>Route + protocol selection<br/>внутри уже известного оператора<br/>Route versioning<br/>Failover primary/reserve"]

    Delivery["Delivery Service<br/><br/>Protocol-aware dispatch (SMPP / HTTP)<br/>Idempotency control<br/>SUBMITTED / FAILED / UNKNOWN"]

    DeliveryReconciliation["Delivery Reconciliation Service<br/><br/>Passive evidence collection<br/>Wait for DLR / late submit response<br/>Optional query_sm<br/>Operator-specific deadline"]
end

Incoming --> Pipeline
StageCompleted --> Pipeline

Pipeline -->|"DestinationResolutionExecute"| DestinationResolutionTopic
Pipeline -->|"PolicyExecute"| PolicyTopic
Pipeline -->|"BillingExecute"| BillingTopic
Pipeline -->|"RoutingExecute"| RoutingTopic
Pipeline -->|"DeliveryExecute"| DeliveryTopic
Pipeline -->|"UNKNOWN outcome"| ReconciliationTopic
Pipeline -->|"Hold command<br/>message_id + stage_execution_id + scope"| StandardCommands

DestinationResolutionTopic --> DestinationResolution
PolicyTopic --> Policy
BillingTopic --> Billing
RoutingTopic --> Routing
DeliveryTopic --> Delivery
ReconciliationTopic --> DeliveryReconciliation

DestinationResolution -->|"StageCompleted<br/>resolved operator_id"| StageCompleted
Policy -->|"StageCompleted"| StageCompleted
Billing -->|"StageCompleted"| StageCompleted
Routing -->|"StageCompleted"| StageCompleted
Delivery -->|"StageCompleted"| StageCompleted
DeliveryReconciliation -->|"RESOLVED / UNRESOLVED"| StageCompleted

Delivery <-->|"Instance-addressed RPC<br/>(protocol=SMPP)"| OperatorSession
Delivery <-->|"Instance-addressed RPC<br/>(protocol=HTTP)"| OperatorHttpGateway
DeliveryReconciliation -.->|"query_sm только по конфигурации"| OperatorSession

%% ==========================================================
%% SCHEDULER LANES
%% ==========================================================

subgraph SchedulerCapability["Scheduler Capability"]
    CriticalSweep["Scheduler Critical Sweep<br/><br/>Периодический опрос Redis sorted set (~1с)<br/>Stage timeout<br/>Stage retry<br/>Billing-critical deadlines<br/>Не читает stage-топики"]

    StandardScheduler["Scheduler Standard Lane<br/><br/>PAUSED holds<br/>Controlled backlog release<br/>Reconciliation deadlines"]

    BackgroundScheduler["Scheduler Background Lane<br/><br/>DLR correlation retry<br/>Notification retry<br/>Low-priority tasks"]

    StatefulRuntime["Shared Stateful Processing Runtime<br/><br/>Embedded KV<br/>Kafka transactions<br/>Changelog recovery<br/>Partition fencing<br/><br/>Используется Standard и Background Lane;<br/>Critical Sweep не имеет embedded state"]
end

subgraph SchedulerKafka["Kafka: Scheduler"]
    CriticalCommands["scheduler.critical.commands"]
    StandardCommands["scheduler.standard.commands"]
    BackgroundCommands["scheduler.background.commands"]

    StandardChangelog["scheduler.standard.state.changelog"]
    BackgroundChangelog["scheduler.background.state.changelog"]
end

CriticalCommands --> CriticalSweep
StandardCommands --> StandardScheduler
BackgroundCommands --> BackgroundScheduler

StandardScheduler <--> StandardChangelog
BackgroundScheduler <--> BackgroundChangelog

StatefulRuntime -.-> StandardScheduler
StatefulRuntime -.-> BackgroundScheduler

CriticalSweep <-->|"Poll просроченных дедлайнов<br/>(sorted set по shard-бакетам)"| RuntimeRedis

CriticalSweep -->|"Delayed retry"| DestinationResolutionTopic
CriticalSweep -->|"Delayed retry"| PolicyTopic
CriticalSweep -->|"Delayed retry"| BillingTopic
CriticalSweep -->|"Delayed retry"| RoutingTopic
CriticalSweep -->|"Delayed retry"| DeliveryTopic
CriticalSweep -->|"Timeout / retry exhausted"| StageCompleted

StandardScheduler -->|"Controlled release"| DestinationResolutionTopic
StandardScheduler -->|"Controlled release"| PolicyTopic
StandardScheduler -->|"Controlled release"| BillingTopic
StandardScheduler -->|"Controlled release"| RoutingTopic
StandardScheduler -->|"Controlled release"| DeliveryTopic
StandardScheduler -->|"Reconciliation wake-up"| ReconciliationTopic

BackgroundScheduler -->|"Retry unresolved DLR"| OperatorDLRUnresolved
BackgroundScheduler -->|"Retry notification"| NotificationRetryTopic

%% ==========================================================
%% EXECUTION CONTROL
%% ==========================================================

subgraph Control["Execution Control"]
    AvailabilityController["Stage Availability Controller<br/><br/>Health<br/>Kafka lag<br/>Error rate<br/>Manual override"]

    ExecutionControl["Execution Control Service<br/><br/>Scope composition<br/>Hysteresis<br/>Dwell time<br/>Admission limits<br/>Dispatch ramp-up"]

    ControlSnapshot["Local execution-control snapshot<br/><br/>GLOBAL<br/>STAGE<br/>PARTNER<br/>PARTNER_STAGE<br/>OPERATOR_ROUTE"]
end

AvailabilityController --> ExecutionControl
CriticalSweep -.->|"Sweep lateness"| ExecutionControl
StandardScheduler -.->|"Hold backlog"| ExecutionControl
BackgroundScheduler -.->|"Background pressure"| ExecutionControl

ExecutionControl --> ExecutionControlTopic
ExecutionControlTopic --> ControlSnapshot

ControlSnapshot -.-> RestReceiver
ControlSnapshot -.-> PartnerGateway
ControlSnapshot -.-> Pipeline
ControlSnapshot -.-> CriticalSweep
ControlSnapshot -.-> StandardScheduler
ControlSnapshot -.-> Billing
ControlSnapshot -.-> Routing
ControlSnapshot -.-> Delivery

%% ==========================================================
%% RUNTIME REDIS
%% ==========================================================

RuntimeRedis[("Runtime Redis Cluster<br/><br/>Pipeline execution state<br/>Deadline sorted set (sharded)<br/>Rate limit sync (periodic, не per-message)<br/>Partner SMPP session registry<br/>Operator route registry (SMPP + HTTP,<br/>единый паттерн владения)<br/><br/>Atomic CAS<br/>noeviction<br/>TTL + safety margin")]

Pipeline <-->|"Atomic transition by<br/>message_id + stage_execution_id<br/>+ ZADD/ZREM дедлайна в том же Lua-вызове"| RuntimeRedis

RestReceiver <-->|"Rate limits (периодическая синхронизация,<br/>не на каждое сообщение)"| RuntimeRedis
PartnerGateway <-->|"Rate limits (периодическая синхронизация)<br/>Session registry"| RuntimeRedis
OperatorSession <-->|"Route registry (SMPP)"| RuntimeRedis
OperatorHttpGateway <-->|"Route registry (HTTP)"| RuntimeRedis
Delivery -.->|"Lookup route instance<br/>(протокол из Routing)"| RuntimeRedis
DeliveryReconciliation -.->|"Lookup route instance"| RuntimeRedis

%% ==========================================================
%% DLR FLOW
%% ==========================================================

subgraph DLRLayer["DLR и корреляция"]
    DLRCorrelationWriter["DLR Correlation Writer<br/><br/>Batch insert<br/>operator_id + smsc_message_id<br/>→ message_id"]

    DLRManager["DLR Manager<br/><br/>Normalize operator DLR<br/>Correlation lookup<br/>Validate DLR data"]

    MessageStateResolver["Message State Resolver<br/><br/>Partitioned stateful service<br/>Status transition validation<br/>No invalid regression<br/>Lifecycle versioning"]

    MessageStateStore["Embedded Message State<br/><br/>Disposable local KV"]

    MessageStateChangelog["message-state.changelog<br/><br/>Authoritative compacted state"]
end

OperatorSession -->|"Accepted submit_sm"| OperatorSubmitAccepted
OperatorSession -->|"Raw DLR"| OperatorDLR

OperatorSubmitAccepted --> DLRCorrelationWriter
OperatorDLR --> DLRManager

DLRManager <-->|"DLR correlation lookup"| PostgreSQL

DLRManager -->|"Mapping отсутствует"| BackgroundCommands
OperatorDLRUnresolved --> DLRManager
DLRManager -->|"Correlation window expired"| OperatorDLRDLQ

DLRManager -->|"Normalized status"| DeliveryStatus

StageCompleted --> MessageStateResolver
DeliveryStatus --> MessageStateResolver

MessageStateResolver <--> MessageStateStore
MessageStateResolver <--> MessageStateChangelog
StatefulRuntime -.-> MessageStateResolver

MessageStateResolver -->|"Partner-visible transition"| MessageLifecycle

OperatorSubmitAccepted --> DeliveryReconciliation
DeliveryStatus --> DeliveryReconciliation

%% ==========================================================
%% CONFIGURATION
%% ==========================================================

subgraph Configuration["Configuration Control Plane"]
    ConfigService["Configuration Service<br/><br/>Validation<br/>Immutable version<br/>Config + outbox<br/>One PostgreSQL transaction"]

    ConfigPublisher["Config Event Publisher<br/><br/>Transactional outbox reader"]

    ConfigProjector["Config Cache Projector<br/><br/>Build Redis projection"]

    LocalConfigSnapshot["Local immutable config snapshots<br/><br/>Pipeline<br/>Destination Resolution<br/>Policy<br/>Billing<br/>Routing<br/>Delivery<br/>Partner REST Receiver<br/>Partner SMPP Gateway<br/>Operator SMPP Session Manager<br/>Operator HTTP Gateway"]
end

ConfigService -->|"Config + config_outbox"| PostgreSQL
PostgreSQL --> ConfigPublisher
ConfigPublisher --> ConfigChanges

ConfigChanges --> ConfigProjector
ConfigProjector --> ConfigRedis

ConfigChanges --> LocalConfigSnapshot
ConfigRedis -.->|"Bootstrap / cache miss"| LocalConfigSnapshot
PostgreSQL -.->|"Immutable-version fallback"| LocalConfigSnapshot

LocalConfigSnapshot -.-> Pipeline
LocalConfigSnapshot -.-> DestinationResolution
LocalConfigSnapshot -.-> Policy
LocalConfigSnapshot -.-> Billing
LocalConfigSnapshot -.-> Routing
LocalConfigSnapshot -.-> Delivery
LocalConfigSnapshot -.-> DeliveryReconciliation
LocalConfigSnapshot -.-> RestReceiver
LocalConfigSnapshot -.-> PartnerGateway
LocalConfigSnapshot -.-> OperatorSession
LocalConfigSnapshot -.-> OperatorHttpGateway

ConfigRedis[("Configuration Redis Cluster<br/><br/>Bootstrap cache<br/>Versioned configuration<br/>Not used per message")]

%% ==========================================================
%% BILLING
%% ==========================================================

subgraph BillingLayer["Billing"]
    BillingOutboxPublisher["Billing Outbox Publisher<br/><br/>Read durable stream<br/>Publish ledger event"]

    BillingLedgerWriter["Billing Ledger Writer<br/><br/>Double-entry ledger<br/>Unique charge_id<br/>Compensating adjustments"]

    BillingReconciliation["Billing Reconciliation<br/><br/>Detect / classify / alert<br/>Freeze partner through control plane<br/>Fenced recovery only"]
end

BillingRedis[("Billing Redis Cluster<br/><br/>Hot balances<br/>account_state / account_epoch<br/>Charge deduplication<br/>Atomic mutation + durable stream<br/><br/>AOF<br/>noeviction")]

Billing <-->|"charge_id = stage_execution_id<br/>Check account_epoch"| BillingRedis

BillingRedis --> BillingOutboxPublisher
BillingOutboxPublisher --> BillingLedgerTopic
BillingLedgerTopic --> BillingLedgerWriter
BillingLedgerWriter --> PostgreSQL

BillingReconciliation <--> BillingRedis
BillingReconciliation <--> PostgreSQL
BillingReconciliation -->|"gRPC: freeze / unfreeze"| ExecutionControl

%% ==========================================================
%% STORAGE AND PROJECTIONS
%% ==========================================================

subgraph Data["Хранение и проекции"]
    PostgreSQL[("PostgreSQL<br/><br/>Single logical platform DB<br/>Configuration + outbox<br/>Message read model<br/>Lifecycle history<br/>DLQ record + replay audit<br/>Billing ledger<br/>DLR correlation<br/>Reconciliation cases")]

    ClickHouse[("ClickHouse<br/><br/>Analytics<br/>Reports<br/>Aggregations<br/>Long-range statistics")]

    LifecycleWriter["Lifecycle Writer<br/><br/>Independent consumer group<br/>Batch PostgreSQL writes"]

    AnalyticsWriter["Analytics Writer<br/><br/>Independent consumer group<br/>Batch ClickHouse writes"]
end

DLRCorrelationWriter --> PostgreSQL
DeliveryReconciliation <-->|"Reconciliation cases"| PostgreSQL

Incoming --> LifecycleWriter
MessageLifecycle --> LifecycleWriter
OperatorDLRDLQ --> LifecycleWriter
LifecycleWriter --> PostgreSQL

Incoming --> AnalyticsWriter
StageCompleted --> AnalyticsWriter
MessageLifecycle --> AnalyticsWriter
AnalyticsWriter --> ClickHouse

%% ==========================================================
%% DLQ
%% ==========================================================

StageDLQ["Stage DLQ Topics<br/><br/>stage.destination-resolution.dlq<br/>stage.policy.dlq<br/>stage.billing.dlq<br/>stage.routing.dlq<br/>stage.delivery.dlq<br/>stage.delivery-reconciliation.dlq"]

CriticalSweep -->|"Poison / safe retry exhausted"| StageDLQ
StageDLQ --> LifecycleWriter

%% ==========================================================
%% PARTNER NOTIFICATIONS AND APIs
%% ==========================================================

subgraph Management["Partner и Management Plane"]
    Notification["Partner Notification Service<br/><br/>deliver_sm<br/>REST callback<br/>WebSocket<br/>Retry / TTL"]

    BackofficeUI["Backoffice UI"]
    BackofficeAPI["Backoffice API"]

    PartnerAPI["Partner API<br/><br/>Status<br/>Message search<br/>Reports"]

    ReplayService["Controlled DLQ Replay<br/><br/>TTL check<br/>Idempotency check<br/>Billing check<br/>Delivery ambiguity check"]
end

MessageLifecycle --> Notification
NotificationRetryTopic --> Notification

Notification -->|"Lookup instance + session_epoch"| RuntimeRedis
Notification -->|"mTLS gRPC to exact StatefulSet pod"| PartnerGateway
Notification --> PartnerChannels["REST callback / WebSocket"]
Notification -->|"Notification retry task<br/>(on delivery failure)"| BackgroundCommands

BackofficeUsers --> BackofficeUI
BackofficeUI --> BackofficeAPI

BackofficeAPI -->|"gRPC"| ConfigService
BackofficeAPI -->|"gRPC"| ExecutionControl
BackofficeAPI --> PostgreSQL
BackofficeAPI --> ClickHouse
BackofficeAPI -->|"gRPC"| ReplayService
BackofficeAPI -->|"gRPC: force timeout/retry<br/>restricted task_type, audited"| CriticalCommands

RestPartners --> PartnerAPI
PartnerAPI --> PostgreSQL
PartnerAPI --> ClickHouse

ReplayService <-->|"Read DLQ record /<br/>write replay audit"| PostgreSQL

ReplayService --> DestinationResolutionTopic
ReplayService --> PolicyTopic
ReplayService --> BillingTopic
ReplayService --> RoutingTopic
ReplayService --> DeliveryTopic
ReplayService --> ReconciliationTopic

%% ==========================================================
%% OBSERVABILITY
%% ==========================================================

subgraph Observability["Observability"]
    OTel["OpenTelemetry Collector Tier<br/><br/>Tail-based sampling<br/>Shard by trace_id"]

    Prometheus["Prometheus<br/><br/>100% metrics"]

    Tempo["Tempo<br/><br/>Sampled traces"]

    LogStore["Structured Log Storage<br/><br/>100% errors<br/>Rate-limited info/debug"]

    Grafana["Grafana"]
end

AllServices["Все сервисы"] -.->|"Metrics / traces / logs"| OTel

OTel --> Prometheus
OTel --> Tempo
OTel --> LogStore

Prometheus --> Grafana
Tempo --> Grafana
LogStore --> Grafana
```

---

# 3. Контекст и границы системы

## 3.1. Входящие интерфейсы

Платформа принимает сообщения от партнёров **по двум протоколам одновременно** — это не «SMPP с REST как альтернативой», а равноправные входные каналы:

* по REST — Partner REST Receiver;
* по SMPP через постоянные bind-сессии партнёров — Partner SMPP Gateway.

Partner REST Receiver не хранит состояние соединения и масштабируется горизонтально.

Partner SMPP Gateway является stateful-компонентом. TCP-соединение и SMPP bind принадлежат конкретному экземпляру Gateway. Поэтому компонент разворачивается со стабильной идентичностью экземпляров.

Партнёрская конфигурация (credentials, IP allowlist, разрешённые приложения) доставляется в Partner REST Receiver и Partner SMPP Gateway тем же механизмом, что и остальная конфигурация платформы — через `config.changes` и локальный immutable snapshot (см. §16). Отдельного канала для партнёрской конфигурации не существует.

## 3.2. Исходящие интерфейсы

Симметрично входу — **в сторону мобильных операторов также используются два протокола**, и они не взаимоисключающие альтернативы «либо-либо на всю платформу», а выбор на уровне конкретного маршрута к конкретному оператору:

* SMPP — Operator SMPP Session Manager;
* HTTP — Operator HTTP Gateway (submit исходящий, DLR — входящий webhook).

Для одного оператора одновременно активен только один протокол (primary), второй — резервный на случай отказа. Оба протокола используют один и тот же паттерн владения маршрутом (registry в Runtime Redis, HLD §11.3) — активный маршрут закреплён за одним инстансом, что делает `tps_limit` локальным инвариантом владеющего инстанса вне зависимости от протокола. Комбинированная координация лимита между протоколами не нужна, поскольку одновременно активен только один.

Operator SMPP Session Manager отвечает за:

* bind и reconnect;
* `enquire_link`;
* SMPP window;
* throttling;
* приоритетизацию команд;
* обработку `submit_sm_resp`;
* приём DLR.

Operator HTTP Gateway отвечает за:

* отправку submit по HTTP оператору;
* приём DLR через входящий webhook от оператора;
* throttling в рамках `tps_limit`, закреплённого за владеющим инстансом;
* reconnect/retry на уровне HTTP-клиента при временной недоступности оператора.

## 3.1.1. Каналы и протоколы — независимые измерения

Платформа сейчас — SMS-движок, но контракт сообщения с первого дня несёт независимое от протокола поле `channel` (HLD §6): единственное заполненное значение сегодня — `SMS`, значения `EMAIL`/`PUSH` зарезервированы. Протокол (`SMPP`/`HTTP` для SMS сегодня; `SMTP`/`APNS`/`FCM` — зарезервировано) — атрибут конкретного inbound/outbound адаптера и конкретного маршрута, не канала. Это два независимых измерения: `channel` определяет, какие бизнес-правила применяются (Policy, Billing-тариф, формат payload), `protocol` определяет, какой сервис физически передаёт байты. Добавление нового канала (email/push) означает новые inbound/outbound адаптеры под новый протокол и точечную донастройку Policy/Routing — не изменение Pipeline Engine, Billing, Scheduler, Message State Resolver и остального channel-агностичного ядра.

## 3.3. Вне текущего HLD

Документ не фиксирует:

* точное количество pod и VM;
* число Kafka partitions;
* физический sizing баз данных;
* точные protobuf и REST-схемы;
* конкретные индексы PostgreSQL;
* операторские SMPP-параметры;
* детальную UI-архитектуру;
* окончательные latency SLO.

---

# 4. Архитектурные драйверы

| Драйвер                  | Принятое решение                                  |
| ------------------------ | ------------------------------------------------- |
| Пиковая нагрузка         | До 20 000 входящих сообщений в секунду            |
| Средняя рабочая нагрузка | Около 12 000–14 000 сообщений в секунду           |
| Продолжительность пика   | Около 1,5–2 часов в сутки                         |
| Жизнь сообщения          | До 24 часов, около 90% завершаются за 2 часа      |
| Kafka retention          | Базовый ориентир — 48 часов                       |
| Processing model         | Асинхронный, event-driven                         |
| Доставка Kafka           | At-least-once                                     |
| Внешняя SMPP-доставка    | Exactly-once не гарантируется                     |
| Масштабирование          | Горизонтальное через Kafka consumer groups        |
| Analytics                | Отдельный ClickHouse                              |
| OLTP                     | Один логический PostgreSQL-компонент              |
| Hot state                | Физически разделённые Redis-кластеры              |
| Конфигурация             | Версионированные immutable snapshots              |
| Workflow                 | Последовательный условный граф без fan-out/fan-in |

---

# 5. Основные архитектурные решения

## 5.1. ACK до бизнес-обработки

До подтверждения партнёру выполняются только:

* проверка структуры запроса;
* протокольная валидация;
* аутентификация;
* проверка IP и приложения;
* rate limit;
* генерация `message_id` и `trace_id`;
* публикация `IncomingMessage` в Kafka.

ACK возвращается только после подтверждённой публикации в Kafka.

Policy, Billing, Routing и Delivery выполняются после ACK.

Это исключает зависимость latency партнёрского запроса от всех последующих бизнес-стадий.

## 5.2. Kafka как транспорт и журнал событий

Kafka используется для:

* приёма immutable message context;
* передачи команд стадиям;
* результатов стадий;
* DLR;
* lifecycle-событий;
* изменений конфигурации;
* billing ledger;
* управляющих сигналов;
* DLQ;
* changelog stateful-компонентов.

Kafka не является долгосрочным аналитическим хранилищем.

## 5.3. Последовательный Pipeline

Pipeline первой версии не является произвольным workflow engine.

Поддерживается:

* последовательное выполнение стадий;
* условное ветвление;
* пропуск стадий;
* разные pipeline для операторов, приложений и партнёров;
* immutable-версии pipeline.

Не поддерживается:

* параллельный fan-out;
* fan-in/join;
* создание нового типа стадии только конфигурацией.

Зарегистрированные типы стадий:

1. Destination Resolution — резолв оператора (для SMS) по номерному диапазону; channel-нейтральное имя, зарезервировано для аналогов у email/push;
2. Policy;
3. Billing;
4. Routing;
5. Delivery;
6. Delivery Reconciliation — специальная ветка для неопределённой отправки.

Destination Resolution всегда идёт первой стадией — Policy зависит от результата (правила сравнения с шаблонами различаются по оператору, HLD §6, `service_internal_methods.md` Policy Engine), и это единственная стадия, для которой порядок жёстко зафиксирован, а не следует из графа конфигурации.

**Второе жёстко зафиксированное правило: `REJECTED` от Policy не завершает pipeline напрямую — идёт в Billing.** Отклонённое Policy сообщение (по любой из проверок — шаблон/кодировка/отправитель/банворды/consent/время суток/anti-spam, единообразно) всё равно тарифицируется: Pipeline Engine диспетчеризует `BillingExtension` с `category = "BLOCKED"` (тот же путь, что для успешно категоризированного сообщения — HLD §15.1). Только **после** Billing pipeline завершается — Routing и Delivery пропускаются. Это условное ветвление, а не новая возможность Pipeline Engine (граф уже поддерживает conditional branching и skip stages) — но конкретно эта ветка жёстко зафиксирована, аналогично Destination Resolution, а не оставлена на усмотрение конфигурации пайплайна, потому что от неё зависит корректность биллинга, а не только бизнес-предпочтение партнёра.

`FAILED` (в отличие от `REJECTED`) в Billing не идёт — это технический сбой стадии (например, недоступность зависимости), а не бизнес-решение об отклонении; обрабатывается обычным retry/DLQ через Critical Sweep, тарификации не создаёт.

Добавление нового типа стадии требует нового топика, сервиса, контракта и релиза Pipeline Engine.

## 5.4. Синхронные вызовы

Сервисы не вызывают друг друга напрямую вне двух явно перечисленных категорий.

**Instance-addressed RPC** — вызов конкретного экземпляра stateful-компонента в обход обычной балансировки, всегда gRPC + mTLS:

* Delivery → Operator SMPP Session Manager (submit, protocol=SMPP);
* Delivery → Operator HTTP Gateway (submit, protocol=HTTP);
* Delivery Reconciliation → Operator SMPP Session Manager (`query_sm`, опционально);
* Partner Notification Service → Partner SMPP Gateway (`deliver_sm`).

Целевой инстанс резолвится через registry в Runtime Redis (§11.1, §11.3) — единый паттерн для SMPP и HTTP.

**Обычные внутренние вызовы** — между stateless control-plane сервисами, gRPC + mTLS со стандартной балансировкой Kubernetes, без instance addressing:

* Backoffice API → Configuration Service, Execution Control Service, Replay Service;
* Billing Reconciliation → Execution Control Service (freeze/unfreeze).

Полный перечень — services_specifictaion.md §9.3.

Всё остальное взаимодействие между сервисами идёт через Kafka, Redis или PostgreSQL — полная карта входов/выходов каждого сервиса зафиксирована в `service_io_contracts.md`.

---

# 6. Общий контракт выполнения стадии

Каждая stage-команда содержит:

```text
event_id
message_id
channel              (SMS сегодня; EMAIL / PUSH зарезервированы)
pipeline_id
pipeline_version
node_id
stage_name
stage_execution_id
attempt
deadline
message_ttl
config_versions
traceparent
payload_reference / immutable context   (дискриминирован по channel — HLD §3.1.1)
```

`channel` — обязательное поле с первого дня, даже пока платформа поддерживает только SMS: оно определяет, какой вариант дискриминированного payload заполнен, и какой набор Policy/Billing-правил применяется. Это заложенная сейчас основа для email/push, а не задел «на бумаге» — реальный формат для этих каналов не специфицируется в этой версии HLD.

`stage_execution_id` создаётся один раз на логическое выполнение узла.

При retry:

* `stage_execution_id` не меняется;
* `attempt` увеличивается;
* повторно проверяется TTL;
* повторно проверяется execution control;
* для Billing повторно проверяется `account_epoch`.

Результат стадии содержит:

```text
event_id
message_id
stage_execution_id
attempt
stage_name
outcome
reason_code
retryable
retry_after
result
traceparent
completed_at
```

Основные `outcome`:

* `SUCCEEDED`;
* `REJECTED`;
* `FAILED`;
* `TIMED_OUT`;
* `RETRY_EXHAUSTED`;
* `SUBMISSION_OUTCOME_UNKNOWN`;
* `DELIVERY_UNRESOLVED`.

---

# 7. Pipeline Engine

Pipeline Engine:

* подписан на `incoming.messages`;
* подписан на `stage.completed`;
* выбирает версию Pipeline;
* определяет следующий узел;
* публикует следующую stage-команду;
* применяет атомарный переход Execution State.

При пяти обязательных стадиях (Destination Resolution, Policy, Billing, Routing, Delivery) Pipeline обрабатывает приблизительно:

```text
20 000 × (1 incoming + 5 completion)
≈ 120 000 входящих событий в секунду
```

Поэтому Pipeline считается центральным hot-path компонентом, а не «лёгким оркестратором».

## 7.1. Конфигурация Pipeline

Pipeline Engine не читает конфигурацию из Redis на каждом событии.

Каждый экземпляр хранит локальные immutable snapshots, получаемые через `config.changes`.

Сообщение фиксирует:

* `pipeline_version`;
* версии Policy;
* версии Billing;
* версии Routing.

Сообщение продолжает обрабатываться по исходной версии даже после публикации новой конфигурации.

## 7.2. Runtime State

Runtime Redis хранит минимальное Execution State:

```text
message_id
pipeline_version
node_id
stage_execution_id
current_state
attempt
deadline
last_applied_event_id
```

Pipeline применяет результат через атомарный CAS:

```text
expected stage_execution_id
AND expected current state
→ new state
```

Это защищает от:

* повторного `StageCompleted`;
* позднего completion предыдущей попытки;
* Kafka redelivery;
* конкурентной обработки одного результата.

Тот же атомарный вызов дополнительно обновляет запись `deadline` сообщения в шардированном Redis sorted set (`ZADD` при диспетчеризации, `ZREM` при штатном завершении) — это не отдельная сетевая операция, а дополнительная команда внутри уже выполняемого Lua-скрипта CAS. Именно этот sorted set читает Critical Sweep (§9.1) вместо того, чтобы подписываться на поток stage-событий.

---

# 8. Execution Control и обратная связь

Execution Control Service управляет:

* admission rate на входе;
* dispatch rate из Scheduler;
* остановкой стадии;
* остановкой партнёра;
* остановкой конкретной стадии для партнёра;
* ограничением маршрута оператора.

## 8.1. Scope

Поддерживаются области:

```text
GLOBAL
STAGE
PARTNER
PARTNER_STAGE
OPERATOR_ROUTE
```

## 8.2. Состояния

```text
ACTIVE
DEGRADED
PAUSED
```

Эффективное состояние определяется по максимальной строгости:

```text
PAUSED > DEGRADED > ACTIVE
```

Эффективный rate:

```text
effective_rate = min(all applicable rate limits)
```

При `PAUSED` эффективный rate равен нулю.

Одинаковая библиотека расчёта effective control используется Receiver, Pipeline и Scheduler.

### Кто читает `execution.control` напрямую

Прямыми потребителями `execution.control` являются: Partner REST Receiver, Partner SMPP Gateway, Pipeline Engine, Critical Sweep, Standard Lane, Billing, Routing, Delivery.

Policy и Destination Resolution в этот список не входят осознанно: единственное действие каждого из них — вычисление, без внешнего или необратимого эффекта, и admission-проверка уже выполнена Pipeline Engine до диспетчеризации. Повторная проверка control state внутри них не защищает ни от чего нового.

Billing, Routing и Delivery читают `execution.control` напрямую, потому что каждый из них непосредственно предшествует необратимому или внешнему эффекту (списание денег, выбор внешнего маршрута, фактическая отправка в SMSC), а между решением Pipeline о диспетчеризации и фактическим исполнением стадии проходит время — сообщение могло провести часы в Scheduler на retry или hold. Поэтому эти три сервиса повторяют проверку непосредственно перед действием, тем же принципом, что и `account_epoch` в Billing (§15.3): admission-контроль на входе не заменяет проверку прямо перед необратимым шагом.

## 8.3. Гистерезис

Для каждого контролируемого сигнала настраиваются:

```text
enter_degraded_threshold
exit_degraded_threshold
enter_paused_threshold
exit_paused_threshold
enter_confirmation_window
exit_confirmation_window
min_state_duration
```

Выход из `PAUSED` происходит сначала в `DEGRADED`.

Система не возвращается сразу в `ACTIVE`.

Ручной `PAUSED` не снимается автоматически без `expires_at` или явной команды.

**LLD:** алгоритм переходов (confirmation window + min_state_duration, асимметрия быстрого спуска/осторожного подъёма) формализован и протестирован в `state_machines.md` §2 (`state_machines/execution_control_hysteresis.py`, 5/5 тестов) — включая доказанное свойство, что шум вокруг порога не вызывает flapping, а катастрофический скачок метрики может уйти из `ACTIVE` сразу в `PAUSED`, минуя `DEGRADED`.

## 8.4. Admission control

При перегрузке:

* Partner REST Receiver возвращает `429` или согласованный `503`;
* Partner SMPP Gateway возвращает `ESME_RTHROTTLED` либо согласованную системную ошибку;
* rate ограничивается только для затронутого scope.

Если Billing остановлен только для одного партнёра, остальные партнёры продолжают работать.

## 8.5. Ramp-up

После восстановления backlog освобождается постепенно:

```text
0% → 5% → 10% → 25% → 50% → 100%
```

Точные шаги задаются конфигурацией.

Scheduler использует:

* token bucket;
* fair scheduling;
* квоты по партнёру;
* квоты по типу задачи.

Если lag или error rate снова растут, release rate автоматически снижается.

## 8.6. Отказ самого Execution Control Service

Execution Control Service — единственный источник `execution.control`, поэтому его собственная недоступность рассматривается отдельно.

Потребители (Receiver, Partner Gateway, Pipeline, Scheduler lanes, Billing, Routing) держат локальный snapshot из compacted topic и не делают синхронных вызовов в сервис. При его отказе поведение fail-static:

* последнее известное состояние по каждому scope продолжает действовать без изменений;
* новые переходы `ACTIVE → DEGRADED`, `DEGRADED → PAUSED` и обратные переходы не создаются;
* уже выставленный `PAUSED`/`DEGRADED` не снимается автоматически;
* ручные `PAUSED` с истёкшим `expires_at` не продлеваются и не снимаются, пока сервис не восстановится.

Это осознанный компромисс: платформа теряет способность реагировать на новую деградацию, но не теряет уже принятые защитные ограничения и не открывает admission control настежь.

---

# 9. Scheduler

Scheduler является единой capability delayed execution, но разделён на три физически независимых компонента с разной архитектурой хранения состояния.

## 9.1. Critical Sweep (заменяет Critical Lane)

Обрабатывает:

* stage timeout;
* stage retry;
* Billing-critical timeout;
* retry exhaustion.

**Пересмотрено относительно первой версии.** Раньше этот компонент подписывался на все stage-топики и `stage.completed` целиком (≈160 000 событий/сек на пике) только чтобы знать, у каких сообщений истёк дедлайн — это делало его самой тяжёлой Java/Kafka Streams-группой в системе при том, что фактически востребованная информация (дедлайн) уже присутствует в Execution State в Runtime Redis.

Новая модель:

* Pipeline Engine пишет дедлайн в Redis sorted set (`ZADD`) тем же атомарным Lua-вызовом, которым выполняет CAS-переход Execution State (HLD §7.2) — не отдельным round-trip, а дополнительной командой внутри уже существующего вызова. Sorted set шардируется по бакетам (`deadlines:{bucket}`, `bucket = hash(stage_execution_id) % N`), чтобы не упираться в ограничение Redis Cluster на односегментные ключи.
* При успешном завершении стадии Pipeline Engine тем же способом удаляет запись (`ZREM`).
* Critical Sweep не подписан ни на один stage-топик. Раз в ~1 секунду он обходит бакеты sorted set (`ZRANGEBYSCORE ... -inf now`) и получает только реально просроченные записи — то есть маленькую долю от общего потока, а не весь поток.
* Для каждой просроченной записи Critical Sweep читает `attempt` и retry-policy из Execution State (тот же Runtime Redis) и либо публикует retry в исходный `stage.*`, либо `TIMED_OUT`/`RETRY_EXHAUSTED` в `stage.completed`, либо направляет в DLQ — поведение не изменилось, изменился только триггер (обнаружение через sweep, а не событие из changelog).
* Embedded state store (RocksDB/Pebble) и собственный changelog-топик Critical Sweep не нужны — авторитетное состояние (дедлайн) и так уже лежит в Runtime Redis, дублировать его в отдельном хранилище незачем. При падении и рестарте Critical Sweep просто продолжает опрашивать тот же sorted set, восстанавливать нечего.

`scheduler.critical.commands` сохраняется для ручного `FORCE_TIMEOUT`/`FORCE_RETRY` из Backoffice API по конкретному `stage_execution_id` — единственный producer, `task_type` ограничен разрешённым списком, каждое обращение аудируется.

**Компромисс:** точность обнаружения таймаута — не мгновенная, а с точностью до интервала опроса (~1 секунда). Для SMS-платформы это несущественно.

## 9.2. Standard Lane

Обрабатывает:

* hold при `PAUSED`;
* постепенное освобождение backlog;
* Delivery Reconciliation deadline;
* системные delayed dispatch.

Единственный производитель `scheduler.standard.commands` — Pipeline Engine. В момент, когда Pipeline при попытке диспетчеризации видит `PAUSED` для применимого scope, он не публикует `*Execute` в стадию напрямую, а публикует hold-команду в Standard Lane с полями `message_id`, `stage_execution_id`, `scope`, `stage_name` и условием возобновления (переход соответствующего scope в `ACTIVE`/`DEGRADED` с `admission_rate > 0`). Это делает hold видимым и инспектируемым событием, а не побочным эффектом внутренней логики Scheduler.

Дополнительно, при delayed retry и controlled release Critical Sweep и Standard Lane повторно проверяют `execution.control` перед публикацией — так же, как при retry повторно проверяется `account_epoch` для Billing (см. §6). Явная hold-команда от Pipeline и повторная проверка control state в Scheduler — два независимых уровня защиты, а не взаимоисключающие механизмы.

## 9.3. Background Lane

Обрабатывает:

* retry DLR-корреляции;
* retry партнёрских уведомлений;
* низкоприоритетные operational-задачи.

Всплеск нерезолвленных DLR не может задержать Billing timeout — потому что Critical Sweep (обработка billing-критичных дедлайнов) физически не зависит от Background Lane: разные компоненты, разное хранилище состояния.

## 9.4. Stateful Processing Runtime

Относится только к Standard и Background Lane — у них есть собственный embedded state (hold-очередь, delayed-задачи), для которого нужна transactional consistency между Kafka input, state store и Kafka output.

Каждый из этих двух lane использует:

* embedded Pebble/RocksDB;
* собственный Kafka changelog;
* отдельную consumer group;
* отдельный deployment pool.

Локальный KV не является source of truth. Authoritative state хранится в Kafka changelog. При рестарте или rebalance state восстанавливается из changelog.

Critical Sweep в этой модели не участвует — у него нет собственного состояния, он только читает и точечно обновляет Runtime Redis (§9.1).

---

# 10. Message State Resolver

`stage.completed` является внутренним техническим событием.

`delivery.status` является нормализованным результатом оператора.

Message State Resolver преобразует их в партнёрский `message.lifecycle`.

Он отвечает за:

* допустимость перехода;
* запрет регрессии статуса;
* дедупликацию событий;
* терминальные состояния;
* поздние DLR;
* версию lifecycle.

Пример недопустимого перехода:

```text
DELIVERED → UNDELIVERABLE
```

Пример допустимого позднего уточнения:

```text
DELIVERY_UNRESOLVED
→ LATE_DELIVERY_CONFIRMED
```

История не переписывается: публикуется новая версия lifecycle.

**LLD:** полная таблица допустимых переходов, терминальных статусов и entry points (включая случай, когда Reconciliation даёт первый статус напрямую, минуя `SUBMITTED`) формализована и протестирована в `state_machines.md` §1 (`state_machines/message_lifecycle.py`, 10/10 тестов).

## 10.1. Транзакционная гарантия

Authoritative state хранится в `message-state.changelog`.

Embedded KV является только локальной материализованной копией.

В одной Kafka-транзакции фиксируются:

1. новое состояние в `message-state.changelog`;
2. новое событие `message.lifecycle`;
3. commit input offset.

После Kafka commit committed state применяется в локальный KV.

При потере локального диска сервис полностью восстанавливается из changelog.

---

# 11. Исходящие и партнёрские сессии

## 11.1. Partner SMPP Gateway

Хранит:

* bind-сессии партнёров;
* TCP connections;
* SMPP sequence;
* session epoch;
* heartbeat.

Runtime Redis хранит registry:

```text
partner_id
system_id
session_id
gateway_instance_id
endpoint
session_epoch
heartbeat
```

Partner Notification Service вызывает конкретный экземпляр Gateway через mTLS gRPC.

Обычная балансировка между pod не используется.

Gateway проверяет `session_epoch`, чтобы не отправить `deliver_sm` в старую сессию после reconnect.

## 11.2. Operator SMPP Session Manager

Управляет SMPP-соединениями с SMSC.

Для каждого оператора задаются:

```text
max_window
tps_limit
bind_count
reconnect_policy
enquire_link_interval
dlr_supported
dlr_reliable
query_sm_enabled
query_sm_separate_bind
query_sm_rate_limit
```

Эти параметры доставляются в Operator SMPP Session Manager тем же механизмом config.changes/local snapshot, что и остальная конфигурация платформы (см. §16), отдельного канала не вводится.

## 11.3. Operator HTTP Gateway

Управляет HTTP-соединениями с операторами/SMSC, которые принимают submit по HTTP API вместо SMPP.

В отличие от SMPP, у HTTP нет постоянного сокета — но платформа сознательно не делает Gateway полностью stateless (обычная балансировка между N активных инстансов). Вместо этого используется тот же принцип владения, что у SMPP (§11.4): один активный маршрут — один владеющий инстанс. Причина — не техническое ограничение HTTP, а то, что `tps_limit` иначе становится задачей распределённой координации между независимыми репликами (см. HLD §3.2), а этой сложности мы уже сознательно избежали для SMPP.

Дополнительно отвечает за приём DLR: оператор присылает его HTTP-вебхуком, то есть у формально «исходящего» сервиса есть входящий HTTP-эндпоинт — так же, как у SMPP-версии есть входящая сторона того же сокета.

Для каждого оператора с HTTP-подключением задаются:

```text
endpoint_url
tps_limit
webhook_auth (подпись / токен для валидации входящего DLR)
retry_policy (на уровне HTTP-клиента, отдельно от Scheduler retry стадии)
reconnect_policy
```

## 11.4. Operator Route Registry

Единый паттерн владения для обоих протоколов. Каждый маршрут к оператору (SMPP bind или HTTP endpoint) принадлежит ровно одному экземпляру владеющего сервиса — то же архитектурное решение, что и для Partner SMPP Gateway. Ответственность за конкретный `(operator_id, route_id)` закрепляется за одним инстансом, поэтому `tps_limit` и `max_window` (для SMPP) остаются локальным инвариантом инстанса, а не задачей распределённой координации между независимыми репликами — независимо от протокола.

Runtime Redis хранит registry:

```text
operator_id
route_id
protocol           (SMPP | HTTP)
owning_instance_id
endpoint
route_epoch
heartbeat
```

Delivery Service и Delivery Reconciliation Service резолвят целевой инстанс через этот registry перед instance-addressed RPC, аналогично тому, как Partner Notification Service резолвит Partner SMPP Gateway.

Routing Service фиксирует в результате не только `route_version`, но и выбранные `route_id` + `protocol`, чтобы Delivery обращался к корректному владельцу и корректному сервису (SMPP Session Manager или HTTP Gateway).

Для одного оператора обычно настроен primary-маршрут и резервный (primary/reserve могут быть одного протокола — например, два SMPP bind — либо разных, например HTTP primary и SMPP reserve). Одновременно активен только один — поэтому комбинированная координация `tps_limit` между протоколами не требуется, лимит считается отдельно для каждого маршрута.

При падении владеющего инстанса его маршруты по heartbeat/TTL считаются недоступными, соответствующий `OPERATOR_ROUTE` переводится в `DEGRADED` через Execution Control, а маршруты переустанавливаются на доступном инстансе (или происходит failover на резервный маршрут) согласно `reconnect_policy`.

---

# 12. Политика `query_sm`

`query_sm` выключен по умолчанию.

Если оператор предоставляет надёжный DLR, автоматический `query_sm` не используется.

Он может быть включён только если:

* оператор не предоставляет DLR;
* DLR признан неполным или ненадёжным;
* оператор официально поддерживает `query_sm`;
* запросы укладываются в согласованные лимиты.

Предпочтительный вариант — отдельный bind.

Если отдельный bind оператор не предоставляет:

* используется общий SMPP-канал;
* `submit_sm` всегда имеет более высокий приоритет;
* `query_sm` использует только остаточную ёмкость window;
* при росте submit backlog `query_sm` приостанавливается;
* применяется жёсткий rate limit.

---

# 13. Delivery и неопределённый результат

Exactly-once отправка во внешнюю SMSC не гарантируется.

Возможен сценарий:

1. SMSC принял `submit_sm`;
2. ответ был потерян или не зафиксирован;
3. Delivery Service не знает, принял ли оператор сообщение.

В этом случае публикуется:

```text
SUBMISSION_OUTCOME_UNKNOWN
```

Повторный `submit_sm` автоматически не выполняется.

Delivery Reconciliation Service:

* ждёт поздний `submit_sm_resp`;
* ждёт DLR;
* использует локальные протокольные данные;
* при необходимости и только по конфигурации использует `query_sm`;
* работает до operator-specific deadline.

Результаты:

```text
CONFIRMED_SUBMITTED
CONFIRMED_NOT_SUBMITTED
DELIVERY_CONFIRMED
DELIVERY_FAILED
DELIVERY_UNRESOLVED
```

При `DELIVERY_UNRESOLVED`:

* сообщение не отправляется повторно;
* партнёр получает терминальный неопределённый статус;
* создаётся reconciliation case;
* поздний DLR добавляется как новое lifecycle-событие.

---

# 14. DLR-корреляция

DLR-корреляция не хранится в Runtime Redis.

После положительного `submit_sm_resp` публикуется:

```text
operator.submit.accepted
```

DLR Correlation Writer пакетно пишет в PostgreSQL:

```text
operator_id
smsc_message_id
message_id
stage_execution_id
segment_id
submitted_at
expires_at
```

Ключ включает `operator_id`, поскольку `smsc_message_id` может быть уникальным только внутри оператора.

Таблица партиционируется по времени.

Retention:

```text
operator DLR SLA
+ safety margin
```

Если DLR пришёл раньше записи correlation:

1. DLR Manager публикует background scheduler task;
2. Scheduler возвращает событие в `operator.dlr.unresolved`;
3. поиск выполняется повторно;
4. после истечения окна событие отправляется в `operator.dlr.dlq`;
5. создаётся алерт.

Отдельный DLR Retry Worker не используется.

---

# 15. Billing

## 15.0. Тариф — по категории сообщения, за сегмент

Цена — за сегмент сообщения, зависит от категории, присвоенной Policy (`PolicyResult.category`), а не только от партнёра/оператора/маршрута. Категории — партнёро-специфичные (например `SERVICE`, `TRANSACTION`, `ADVERTISING` — из шаблонов, `policy.policy_template.category`, `data_infrastructure_spec.md` §1.9b) плюс две служебные, обязательные в каждом тарифе:

* `UNTEMPLATED` — сообщение не совпало ни с одним шаблоном, но партнёр разрешил продолжить (HLD §5.3, конфигурация Policy) — обычно дороже обычных категорий: это цена за отказ партнёра от структурированных шаблонов;
* `BLOCKED` — Policy отклонил сообщение, по любой из проверок (HLD §5.3.1) — единая цена независимо от конкретной причины отклонения.

Структура тарифа и пример — `data_infrastructure_spec.md` §1.6a.

Поддерживаются два режима.

## 15.1. Cold / postpaid Billing

Баланс до отправки не проверяется. Billing Redis для cold-потока не используется.

Billing формирует тарификационное событие для последующего ledger и invoice.

Идемпотентность здесь обеспечивается не Redis CAS, а уникальным ограничением на `charge_id = stage_execution_id` при записи в PostgreSQL ledger через Billing Ledger Writer. Повторная доставка Kafka или retry стадии приводят к повторной вставке с тем же `charge_id`, которая отклоняется на уровне БД и не создаёт повторного тарификационного эффекта. Это отдельный, более слабый механизм идемпотентности, чем `account_epoch`-защищённый CAS в hot-потоке, и это осознанно: для cold billing нет живого баланса, который можно испортить гонкой, есть только целостность ledger.

## 15.2. Hot / prepaid Billing

Используется отдельный Billing Redis Cluster.

Хранятся:

* hot balances;
* charge deduplication;
* `account_state`;
* `account_epoch`;
* durable financial stream.

Финансовая операция выполняется атомарно:

```text
account_state == ACTIVE
AND account_epoch == expected_account_epoch
AND charge_id not processed
```

```text
charge_id = stage_execution_id
```

Повторное выполнение одного `stage_execution_id` не создаёт второе списание.

## 15.3. Freeze и account epoch

`account_epoch` увеличивается при каждом переходе:

```text
ACTIVE → FROZEN
FROZEN → ACTIVE
```

Команда, созданная до freeze или до unfreeze, считается устаревшей.

После освобождения hold формируется новая техническая попытка с актуальным epoch, но прежним `stage_execution_id`.

**LLD:** двухсостояниевая машина ACTIVE/FROZEN с fencing по `account_epoch` формализована и протестирована в `state_machines.md` §3 (`state_machines/billing_account_state.py`, 8/8 тестов) — включая доказанное свойство, что операция списания, "зависшая в полёте" при чтении старого epoch, отклоняется после freeze вместо ошибочного применения.

## 15.4. Ledger

Billing Redis выполняет атомарную мутацию и пишет durable stream/outbox.

Billing Outbox Publisher публикует `billing.ledger`.

Billing Ledger Writer создаёт double-entry ledger в PostgreSQL.

PostgreSQL является окончательным бухгалтерским source of truth.

Исходные финансовые записи не изменяются. Коррекции выполняются отдельными compensating entries.

## 15.5. Billing Reconciliation

В нормальном режиме:

```text
detect → classify → alert
```

Запрещён фоновый:

```text
SET balance = recalculated_balance
```

При существенном расхождении:

1. партнёр переводится в `PARTNER_STAGE/BILLING = PAUSED`;
2. hot billing аккаунта получает `FROZEN`;
3. ожидается завершение outbox;
4. баланс пересчитывается по ledger;
5. выполняется fenced CAS;
6. сохраняется audit trail;
7. epoch увеличивается;
8. аккаунт размораживается.

**LLD:** этот пошаговый recovery — то же самое, что `state_machines.md` §3.3 (`freeze` → `unfreeze` с fenced CAS по `expected_epoch`), с явно протестированным случаем отказа: если epoch уехал, пока считался пересчитанный баланс (шаг 4), CAS на шаге 5 отклоняется, а не тихо затирает более новое состояние.

---

# 16. Конфигурация

PostgreSQL является source of truth для конфигурации.

Configuration Service в одной транзакции пишет:

* immutable config version;
* `config_outbox`.

Config Event Publisher публикует `config.changes`.

Config Cache Projector обновляет Configuration Redis.

Hot-path сервисы получают событие и строят новый immutable local snapshot.

Полный список потребителей `config.changes`: Pipeline Engine, Destination Resolution, Policy, Billing, Routing, Delivery, Delivery Reconciliation, Partner REST Receiver, Partner SMPP Gateway, Operator SMPP Session Manager, Operator HTTP Gateway. Других каналов доставки конфигурации в hot path нет — если сервису нужны конфигурационные данные (тарифы, правила, партнёрские credentials, операторские параметры, номерные диапазоны), он получает их только через этот механизм, а не через прямое чтение PostgreSQL или отдельный API-вызов к Configuration Service.

## 16.1. Retention версий

В первой версии конфигурационные версии физически не удаляются из PostgreSQL.

Они могут быть архивированы, но остаются доступными для:

* незавершённых сообщений;
* рестарта сервисов;
* аудита;
* расследований;
* повторного построения snapshot.

Configuration Redis является восстанавливаемым кэшем и может хранить только текущие и активно используемые версии.

---

# 17. Хранилища

## 17.1. PostgreSQL

Используется один логический PostgreSQL-компонент.

Внутри могут использоваться отдельные схемы, таблицы, партиции и read replicas.

Хранит:

* конфигурацию и outbox;
* operational message read model;
* lifecycle history;
* DLR correlation;
* DLQ record (исходная stage-команда + причина, для отображения и replay);
* replay audit;
* billing ledger;
* reconciliation cases;
* данные Backoffice.

Оперативные SELECT Backoffice и Partner API могут обслуживаться read replica.

## 17.2. ClickHouse

Используется для:

* отчётности;
* аналитики;
* TPS-графиков;
* распределения статусов;
* статистики по партнёрам;
* статистики по операторам;
* финансовой аналитики;
* долгосрочного поиска.

ClickHouse не участвует в hot path.

## 17.3. Redis

Используются три физически изолированных кластера.

### Runtime Redis

* Execution State;
* deadline sorted set (шардированный, читается Critical Sweep);
* rate limits (периодическая синхронизация счётчиков, не per-message);
* partner session registry;
* operator route registry (SMPP + HTTP, единый паттерн владения, §11.4);
* `noeviction`;
* TTL + safety margin.

### Configuration Redis

* bootstrap cache;
* immutable config versions;
* не читается на каждом сообщении;
* потеря кэша восстанавливается из PostgreSQL.

### Billing Redis

* деньги и hot balances;
* AOF;
* `noeviction`;
* отдельный SLA и failover;
* атомарные финансовые операции.

---

# 18. Lifecycle и Analytics Writers

Используются два независимых deployment unit — и с разным набором входных топиков, это осознанное решение, не совпадение.

## Lifecycle Writer

Подписан на:

* `incoming.messages`;
* `message.lifecycle`;
* DLQ.

Пакетно пишет в PostgreSQL:

* карточку сообщения (read model, обновляется только по `message.lifecycle` — партнёрский статус, а не внутренний прогресс по стадиям);
* внешнее состояние;
* жизненный цикл;
* DLQ record (исходная stage-команда + причина).

**`stage.completed` намеренно не входит в список входов Lifecycle Writer.** PostgreSQL не хранит построчную историю по каждой стадии — только внешние, партнёрские переходы статуса. Детальная пер-стадийная история (что именно сделала Policy/Billing/Routing/Delivery по конкретному сообщению) доступна через ClickHouse (Analytics Writer, ниже) и через распределённые трейсы (§22). Если бы `stage.completed` также писался в PostgreSQL построчно, это добавило бы ещё ≈80 000 строк/сек к и без того плотной нагрузке записи (см. `capacity_model.md` §6.2) — сверх того, что физически нужно партнёрскому read model.

## Analytics Writer

Подписан на:

* `incoming.messages`;
* `stage.completed`;
* `message.lifecycle`.

Пакетно пишет в ClickHouse. Это единственное хранилище с полной пер-стадийной историей на объёме, для которого PostgreSQL не предназначен.

Проблема ClickHouse не блокирует PostgreSQL Writer.

Проблема PostgreSQL не блокирует Analytics Writer и не останавливает основной Pipeline.

---

# 19. Партнёрские статусы

Partner Notification Service подписывается на `message.lifecycle`, а не на внутренний `stage.completed`.

Доступные каналы:

* SMPP `deliver_sm`;
* REST callback;
* WebSocket;
* получение статуса через Partner API;
* partner-side `query_sm`.

Retry уведомлений выполняется через Scheduler Background Lane, тем же способом, что и retry DLR-корреляции (§14): при неудачной попытке доставки (партнёр недоступен, нет активной SMPP-сессии, ошибка callback) Notification Service публикует в `scheduler.background.commands` задачу `NOTIFICATION_RETRY`; Background Lane возвращает её в `notification.retry`, откуда Notification Service забирает её повторно. Отдельного, второго механизма retry внутри Notification Service нет — используется только Scheduler.

Отсутствие SMPP-сессии не блокирует Pipeline. Уведомление ожидает reconnect до notification TTL.

После исчерпания notification TTL push-доставка прекращается без перевода сообщения в DLQ — `message.lifecycle` уже содержит терминальный статус, он не теряется. Партнёр может получить его pull-запросом через Partner API в любой момент. Push (SMPP/callback/WebSocket) — best-effort с ограниченным числом попыток; Partner API — надёжный fallback без TTL.

---

# 20. DLQ и ручной replay

Для каждой стадии существует отдельный DLQ:

```text
stage.destination-resolution.dlq
stage.policy.dlq
stage.billing.dlq
stage.routing.dlq
stage.delivery.dlq
stage.delivery-reconciliation.dlq
```

DLQ используется для:

* poison messages;
* необрабатываемого контекста;
* устойчивого бага;
* исчерпания безопасных retry.

DLQ не используется как очередь ожидания при полном системном отказе стадии. Для этого используются Execution Control и Scheduler hold.

Перед replay проверяются:

* TTL сообщения;
* `stage_execution_id`;
* idempotency;
* финансовый side effect;
* `account_epoch`;
* неопределённый Delivery outcome;
* возможность внешнего дубликата;
* версия конфигурации.

Все действия replay журналируются.

Lifecycle Writer пишет в PostgreSQL не только метаданные ошибки, но полный DLQ record — исходную stage-команду (общий контракт §6) вместе с причиной попадания в DLQ. Replay Service читает этот record из PostgreSQL (не из Kafka DLQ-топика напрямую — ретеншн Kafka не рассчитан на то, что человек разберётся с инцидентом через несколько дней) и после прохождения всех проверок republish'ит исходную команду в соответствующий `stage.*` топик. Журнал replay-действий (кто, когда, какой `stage_execution_id`, результат проверок) также хранится в PostgreSQL, в той же логической базе, что и остальной аудит платформы (§25).

---

# 21. Гарантии и семантика доставки

Общая модель:

```text
Kafka delivery: at-least-once
Internal stateful processors: transactional consume-transform-produce
Business side effects: idempotency by stage_execution_id
External SMPP submit: may have ambiguous outcome
```

Exactly-once внутри границ всей системы не заявляется.

Дубли предотвращаются сочетанием:

* stable `stage_execution_id`;
* CAS в Runtime Redis;
* Kafka transactions;
* deduplication;
* уникального `charge_id`;
* ограниченного retry;
* запрета автоматического повторного submit при UNKNOWN.

---

# 22. Observability

Trace context передаётся в Kafka headers:

```text
traceparent
tracestate
message_id
stage_execution_id
```

## 22.1. Метрики

Собираются для 100% трафика:

* input TPS;
* stage TPS;
* latency;
* Kafka consumer lag;
* backlog;
* retry;
* timeout;
* DLQ;
* Redis latency;
* Scheduler task lateness;
* DLR correlation lag;
* UNKNOWN outcome rate;
* SMPP window usage;
* operator throttling;
* config propagation lag.

## 22.2. Tracing

OpenTelemetry Collector использует tail-based sampling.

Сохраняются 100% traces для:

* ошибок;
* timeout;
* retry;
* DLQ;
* медленных сообщений;
* `SUBMISSION_OUTCOME_UNKNOWN`;
* reconciliation;
* выбранных диагностируемых партнёров.

Happy path сохраняется ориентировочно в объёме 1–5%.

Collector шардируется по `trace_id`.

## 22.3. Логи

* error-логи — 100%;
* security/audit — 100%;
* info/debug — sampling и rate limiting;
* запрещается бесконтрольное логирование полного SMS payload.

---

# 23. Capacity planning

Расчёт должен учитывать внутреннюю амплификацию, а не только 20k входного TPS.

Ориентировочно:

```text
Pipeline:
20k incoming
+ 100k stage completion (5 обязательных стадий, включая Destination Resolution)
≈ 120k events/sec
```

Scheduler Critical Sweep (§9.1) намеренно не входит в эту амплификацию — он не подписан ни на один stage-топик и не масштабируется вместе с внутренним потоком событий, поэтому не создаёт собственной нагрузки на Kafka, сопоставимой с 160k events/sec, которые были у предыдущей версии дизайна (полное потребление stage-топиков ради отслеживания дедлайнов). Его нагрузка — периодический опрос Runtime Redis, не Kafka.

Дополнительно работают:

* Lifecycle Writer (только `incoming.messages` + `message.lifecycle` + DLQ — без `stage.completed`, см. §18);
* Analytics Writer (полный поток, включая `stage.completed` — единственный держатель детальной пер-стадийной истории, см. §18);
* Message State Resolver;
* DLR Manager;
* Config consumers;
* billing ledger;
* observability.

Полная количественная модель (throughput на ядро, число инстансов, узкие места) — `capacity_model.md`.

## 23.1. Нагрузочные сценарии

Обязательны:

1. steady-state на номинальном пике;
2. steady-state минимум на 2× целевого пика;
3. burst до 3×;
4. падение одного stage;
5. накопление и controlled recovery backlog;
6. Kafka rebalance;
7. потеря локального Scheduler store;
8. восстановление из changelog;
9. рост UNKNOWN outcomes;
10. массовый DLR;
11. деградация PostgreSQL;
12. деградация ClickHouse;
13. Billing Redis failover;
14. reconnect SMPP-сессий.

Критерий устойчивости — отсутствие бесконечного роста lag и автоматическое восстановление после burst или кратковременного отказа.

---

# 24. Поведение при отказах

| Отказ                                      | Поведение                                                                                  |
| ------------------------------------------ | ------------------------------------------------------------------------------------------ |
| Kafka недоступна                           | Новые сообщения не подтверждаются партнёру                                                 |
| Policy/Billing/Routing/Delivery недоступен | Стадия переходит DEGRADED/PAUSED, применяется admission control                            |
| Critical Scheduler недоступен              | Execution Control снижает или останавливает приём трафика                                  |
| Execution Control Service недоступен       | Локальные snapshot остаются на последнем известном состоянии (fail-static); новые PAUSED/DEGRADED не выставляются, уже выставленные не снимаются |
| Runtime Redis недоступен                   | Pipeline не может гарантировать переходы; приём контролируемо ограничивается               |
| Configuration Redis недоступен             | Сервисы продолжают работать на local snapshots                                             |
| PostgreSQL недоступен                      | Hot path продолжает работу до установленных backlog-порогов; CRUD конфигурации блокируется |
| ClickHouse недоступен                      | Analytics Writer накапливает lag, message processing продолжается                          |
| Billing Redis недоступен                   | Hot-billing партнёры ставятся на hold; cold-billing поток может продолжаться по политике   |
| Partner Gateway instance упал              | Сессия исчезает по heartbeat/TTL; партнёр переподключается                                 |
| Operator bind недоступен                   | Route переводится в DEGRADED/PAUSED; используется альтернативный маршрут, если доступен    |
| DLR не пришёл                              | По deadline формируется соответствующий unresolved/expired status                          |
| UNKNOWN submit                             | Повторная отправка запрещена; запускается reconciliation                                   |

---

# 25. Security

На уровне HLD фиксируются:

* TLS для внешних REST-интерфейсов;
* SMPP over TLS или защищённая операторская сеть, где доступно;
* IP allowlist;
* credential и application-level authentication;
* mTLS между внутренними сервисами;
* Kafka ACL по topic и consumer group;
* отдельные DB credentials для каждого сервиса;
* RBAC в Backoffice;
* аудит изменения конфигурации;
* аудит freeze/unfreeze;
* аудит DLQ replay;
* защита SMS payload в логах и UI;
* encryption at rest для хранилищ, где поддерживается;
* секреты не хранятся в конфигурационных таблицах в открытом виде.

Конкретный secrets manager и модель ключей определяются инфраструктурным дизайном.

---

# 26. Принятые ограничения

Архитектура сознательно принимает следующие ограничения:

* Pipeline не поддерживает произвольные плагины стадий без релиза;
* fan-out/fan-in отсутствует в первой версии;
* Exactly-once отправка в SMSC не обещается;
* поздние DLR могут создавать новую lifecycle-версию после терминального unresolved;
* Pipeline Engine и Scheduler являются горячими компонентами и требуют отдельного capacity testing;
* PostgreSQL DLR Correlation Store должен подтвердить пропускную способность нагрузочными тестами;
* query_sm не является основным механизмом статуса;
* автоматическая финансовая коррекция без fencing запрещена;
* Destination Resolution определяет оператора по статическому номерному диапазону; live HLR-резолв для сложных случаев переносимости номера (MNP) не входит в первую версию — задокументированное упрощение, не пробел;
* мультиканальность (email/push) заложена в контракты (`channel`, дискриминированный payload, `protocol` в маршрутах) и в разделение inbound/outbound адаптеров, но сами email/push адаптеры не специфицируются в этой версии HLD — только SMS.

---

# 27. Решения, перенесённые в LLD

На Low-Level Design остаются:

* точное число Kafka partitions;
* replica factor и min ISR;
* размеры Kafka batch;
* точный формат stage events;
* схемы PostgreSQL;
* партиционирование DLR correlation;
* алгоритм генерации `stage_execution_id`;
* Redis key schemas;
* Lua/Redis Functions Billing;
* scheduler timer wheel;
* RocksDB/Pebble tuning;
* cooperative rebalance;
* gRPC-контракты SMPP Gateway;
* операторские retry и timeout policy;
* lifecycle state machine;
* SLO p95/p99;
* инфраструктурный sizing;
* DR-топология;
* retention пользовательских данных.

---

# 28. Итог

Архитектура строится вокруг асинхронной обработки и immutable events, но не пытается скрыть stateful-части системы.

Stateful-компоненты обозначены явно:

* Partner SMPP Gateway;
* Operator SMPP Session Manager;
* Operator HTTP Gateway (тот же паттерн владения, что у SMPP);
* Scheduler lanes;
* Message State Resolver;
* Redis-кластеры;
* PostgreSQL;
* Kafka;
* ClickHouse.

Основные контуры отказоустойчивости:

* admission control;
* гистерезис;
* ограниченный backlog;
* controlled ramp-up;
* разделённые Scheduler lanes;
* формальная идемпотентность;
* DLQ;
* transactional outbox;
* changelog-восстановление;
* физическая изоляция финансового Redis;
* защита от неоднозначной внешней отправки.

**Данная версия считается полной HLD-базой для повторного архитектурного ревью и последующего перехода к LLD.**
