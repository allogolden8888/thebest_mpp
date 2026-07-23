# A2P MPP — Service I/O Contracts

**Основание:** `hld.md` (v1.0 RC) + `services_specifictaion.md` (v1.0)
**Назначение документа:** для каждого сервиса зафиксировать закрытый список входов (откуда, по какому каналу, какие данные) и выходов (куда, по какому каналу, какие данные). Сервисы не вызывают друг друга напрямую — любое взаимодействие проходит через Kafka topic, Redis-кластер, PostgreSQL или один из явно перечисленных gRPC-вызовов (instance-addressed или обычный внутренний, HLD §5.4).

Наблюдаемость (метрики/трейсы/логи в OpenTelemetry Collector, HLD §22) есть у каждого сервиса и не повторяется в каждой карточке.

Первая версия этого документа (см. историю изменений в конце) вскрыла 8 связей, не зафиксированных на диаграмме HLD. Все восемь закрыты правкой `hld.md` и `services_specifictaion.md`; в карточках ниже эти связи уже отражены как обычные, без пометок «пробел».

---

# 1. Data Plane и business-стадии

## 1.1. Partner REST Receiver (Rust)

**Состояние:** stateless.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| REST-клиенты партнёров | HTTP REST | запрос на отправку сообщения: партнёрские креды, msisdn/sender, тело, application_id |
| Runtime Redis | Redis (read, раз в ~1с, не на сообщение) | периодическая синхронизация локального token bucket rate limit |
| `execution.control` | Kafka (compacted, local snapshot) | admission state/rate по scope GLOBAL / PARTNER / PARTNER_STAGE / OPERATOR_ROUTE |
| `config.changes` | Kafka (compacted, local snapshot) | партнёрская конфигурация: credentials, IP allowlist, разрешённые приложения (HLD §16) |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `incoming.messages` | Kafka | Immutable message context: `message_id`, `trace_id`, партнёрский контекст, ссылка на payload |
| Runtime Redis | Redis (write, раз в ~1с) | синхронизация локального счётчика rate limit — не per-message round trip |
| REST-клиент | HTTP-ответ | ACK после подтверждённой публикации в Kafka, либо `429`/`503` при admission control |

---

## 1.2. Partner SMPP Gateway (Java, StatefulSet)

**Состояние:** SMPP bind-сессии, TCP-соединения, sequence numbers, `session_epoch` (in-memory, привязано к конкретному поду); зеркалируется в Runtime Redis registry.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| SMPP-партнёры | SMPP (TCP, постоянное соединение) | `bind`, `submit_sm`, `enquire_link`, partner-side `query_sm` |
| Partner Notification Service | gRPC mTLS (instance-addressed) | команда `deliver_sm` с проверкой `session_epoch` |
| Runtime Redis | Redis (read, раз в ~1с) | периодическая синхронизация локального token bucket rate limit |
| `config.changes` | Kafka (compacted, local snapshot) | партнёрская конфигурация: credentials, IP allowlist, приложения (HLD §16) |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `incoming.messages` | Kafka | Immutable message context (после успешного приёма `submit_sm`, до `submit_sm_resp`) |
| SMPP-партнёр | SMPP | `submit_sm_resp`, `deliver_sm` |
| Runtime Redis | Redis (write) | registry: `partner_id`, `system_id`, `session_id`, `gateway_instance_id`, `endpoint`, `session_epoch`, `heartbeat`; периодическая синхронизация rate limit counter |

**Синхронные вызовы, принимаемые сервисом:** Notification → Gateway (`deliver_sm`, instance-addressed).

---

## 1.3. Operator SMPP Session Manager (Java, stateful)

**Состояние:** SMPP binds к операторам, TCP-соединения, window/sequence state (in-memory); зеркалируется в Runtime Redis registry (HLD §11.4).

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Мобильные операторы / SMSC | SMPP (TCP) | `submit_sm_resp`, DLR, ответы `enquire_link` |
| Delivery Service | gRPC (instance-addressed через registry, `protocol=SMPP`) | команда submit: `queue_msg_id`, назначение, сегменты |
| Delivery Reconciliation Service | gRPC (instance-addressed, только если включено конфигурацией) | команда `query_sm` |
| Runtime Redis | Redis (read/write) | собственный route registry (namespace SMPP) |
| `config.changes` | Kafka (compacted, local snapshot) | операторские параметры: `max_window`, `tps_limit`, `bind_count`, `dlr_supported`, `query_sm_*` (HLD §11.2, §16) |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| Операторы / SMSC | SMPP | `submit_sm`, `enquire_link`, опционально `query_sm` |
| `operator.submit.accepted` | Kafka | принятый submit: `operator_id`, `smsc_message_id` (если доступен), корреляционные ключи |
| `operator.dlr` | Kafka | сырой DLR оператора |
| Delivery Service | gRPC-ответ | результат submit (accepted/rejected/ambiguous) |
| Delivery Reconciliation Service | gRPC-ответ | результат `query_sm` |
| Runtime Redis | Redis (write) | registry: `operator_id`, `route_id`, `protocol=SMPP`, `owning_instance_id`, `endpoint`, `route_epoch`, `heartbeat` |

**Синхронные вызовы, принимаемые сервисом:** Delivery → Operator SMPP Session Manager (submit), Delivery Reconciliation → Operator SMPP Session Manager (`query_sm`).

---

## 1.3a. Operator HTTP Gateway (Go)

**Состояние:** владение маршрутом (не сессией — у HTTP нет постоянного сокета), зеркалируется в тот же Runtime Redis registry, что и SMPP-версия (HLD §11.4).

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Операторы / SMSC | HTTP webhook (входящий) | DLR, с валидацией подписи/токена |
| Delivery Service | gRPC (instance-addressed через registry, `protocol=HTTP`) | команда submit: `queue_msg_id`, назначение, сегменты |
| Runtime Redis | Redis (read/write) | собственный route registry (namespace HTTP) |
| `config.changes` | Kafka (compacted, local snapshot) | операторские HTTP-параметры: `endpoint_url`, `tps_limit`, `webhook_auth`, `retry_policy`, `reconnect_policy` |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| Операторы / SMSC | HTTP (исходящий) | submit-запрос |
| `operator.submit.accepted` | Kafka | тот же формат, что у SMPP-версии — downstream не различает протокол |
| `operator.dlr` | Kafka | нормализованный из webhook-payload в тот же формат, что и сырой SMPP DLR |
| Delivery Service | gRPC-ответ | результат submit (accepted/rejected/ambiguous) |
| Runtime Redis | Redis (write) | registry: `operator_id`, `route_id`, `protocol=HTTP`, `owning_instance_id`, `endpoint`, `route_epoch`, `heartbeat` |

**Синхронные вызовы, принимаемые сервисом:** Delivery → Operator HTTP Gateway (submit).

---

## 1.4. Pipeline Engine (Rust)

**Состояние:** stateless сам по себе; Execution State — в Runtime Redis; конфигурация и execution-control — в локальных immutable snapshot.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `incoming.messages` | Kafka | Immutable message context |
| `stage.completed` | Kafka | результат стадии (от Destination Resolution/Policy/Billing/Routing/Delivery/Delivery Reconciliation/Critical Sweep) |
| `config.changes` | Kafka (compacted, local snapshot) | версии Pipeline/Destination Resolution/Policy/Billing/Routing |
| `execution.control` | Kafka (compacted, local snapshot) | admission/dispatch state по всем scope |
| Runtime Redis | Redis (read) | текущее Execution State для CAS |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.destination-resolution` | Kafka | `DestinationResolutionExecute` команда — всегда первая, до Policy (HLD §5.3) |
| `stage.policy` / `stage.billing` / `stage.routing` / `stage.delivery` | Kafka | `*Execute` команда (общий контракт HLD §6) |
| `stage.delivery-reconciliation` | Kafka | команда при `SUBMISSION_OUTCOME_UNKNOWN` |
| Runtime Redis | Redis (write) | атомарный CAS-переход Execution State **+ ZADD/ZREM дедлайна в `deadlines:{bucket}`** — та же Lua-транзакция, не отдельный round trip (читает Critical Sweep, §2.1) |
| `scheduler.standard.commands` | Kafka | hold-команда (`message_id`, `stage_execution_id`, `scope`, `stage_name`) при обнаружении `PAUSED` на дispatch (HLD §9.2) |

---

## 1.4a. Destination Resolution Service (Rust)

**Состояние:** таблица номерных диапазонов — в локальном immutable snapshot.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.destination-resolution` | Kafka | `DestinationResolutionExecute` команда |
| `config.changes` | Kafka (compacted, local snapshot) | таблица MSISDN prefix → `operator_id` |
| Configuration Redis | Redis (read, bootstrap/cache miss) | fallback при построении snapshot |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | `SUCCEEDED` с `resolved_operator_id`, либо `REJECTED` (номер вне известных диапазонов) |

`execution.control` сюда осознанно не подключён — как и у Policy, это чистое вычисление без внешнего эффекта, admission-проверка уже выполнена Pipeline Engine (HLD §8).

---

## 1.5. Policy Service (Rust)

**Состояние:** шаблоны/правила — в локальном snapshot, скомпилированном per-partner (карта `partner_id → автомат`, точечно обновляемая — `services_specifictaion.md` §2.5); consent-блэклисты и anti-spam счётчики — не в snapshot, читаются из Runtime Redis на каждое сообщение.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.policy` | Kafka | `PolicyExecute` команда, включая `resolved_operator_id` из Destination Resolution |
| Runtime Redis | Redis (read) | `msgctx:{message_id}` — тело сообщения, нужно для template matching и банвордов |
| Runtime Redis | Redis (read) | consent-блэклист абонента по категории и по отправителю (п. 4, 6 требований Policy) |
| Runtime Redis | Redis (read/write) | anti-spam счётчик частоты по абоненту+категории (п. 2) |
| `config.changes` | Kafka (compacted, local snapshot) | шаблоны (per-partner, per-operator), банворды, time-of-day правила, sender validation правила, версии |
| Configuration Redis | Redis (read, bootstrap/cache miss) | fallback при построении snapshot |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | `SUCCEEDED` (с присвоенной категорией) / `REJECTED` / `FAILED` + `reason_code` |
| Runtime Redis | Redis (write) | обновление anti-spam счётчика после пропущенного сообщения |

`execution.control` сюда осознанно не подключён: у Policy нет внешнего/необратимого эффекта, а admission-проверка уже выполнена Pipeline Engine до диспетчеризации (HLD §8, «Кто читает execution.control напрямую»).

---

## 1.6. Billing Service (Java)

**Состояние:** stateless сам по себе; баланс/дедупликация/`account_state`/`account_epoch` — в Billing Redis.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.billing` | Kafka | `BillingExecute` команда, включая `segment_count`/длину сообщения (посчитано один раз Pipeline Engine — Billing не читает Runtime Redis за телом сообщения, в отличие от Policy и Delivery) |
| `config.changes` | Kafka (compacted, local snapshot) | тарифная конфигурация |
| `execution.control` | Kafka (compacted, local snapshot) | admission state, включая `PARTNER_STAGE=BILLING` freeze |
| Billing Redis | Redis (read/write, atomic) | `account_state`, `account_epoch`, баланс, charge dedup по `charge_id = stage_execution_id` |
| Configuration Redis | Redis (read, bootstrap) | fallback |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | `SUCCEEDED` / `REJECTED` / `FAILED` (в т.ч. `ACCOUNT_FROZEN, retryable=true`) |
| Billing Redis | Redis (write) | атомарная финансовая мутация + запись в durable stream/outbox |

---

## 1.7. Routing Service (Rust)

**Состояние:** маршруты — в локальном immutable snapshot.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.routing` | Kafka | `RoutingExecute` команда, включая `resolved_operator_id` из Destination Resolution |
| `config.changes` | Kafka (compacted, local snapshot) | таблица маршрутов (SMPP + HTTP), версии |
| `execution.control` | Kafka (compacted, local snapshot) | admission state по `OPERATOR_ROUTE` |
| Configuration Redis | Redis (read, bootstrap) | fallback |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | выбранные `route_version`, `route_id`, `protocol` (`SMPP`/`HTTP`) внутри уже известного `operator_id` (HLD §11.4) |

---

## 1.8. Delivery Service (Java)

**Состояние:** stateless.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.delivery` | Kafka | `DeliveryExecute` команда, включая `route_id` + `protocol` из Routing |
| `config.changes` | Kafka (compacted, local snapshot) | конфигурация подготовки submit |
| `execution.control` | Kafka (compacted, local snapshot) | admission state по `OPERATOR_ROUTE` — повторная проверка непосредственно перед submit (HLD §8) |
| Runtime Redis | Redis (read) | `msgctx:{message_id}` (тело для сборки submit) + operator route registry — резолв инстанса по `operator_id` + `route_id`, единый registry для SMPP и HTTP |
| Configuration Redis | Redis (read, bootstrap) | fallback |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | `SUBMITTED` / `FAILED` / `SUBMISSION_OUTCOME_UNKNOWN` |
| Operator SMPP Session Manager | gRPC (instance-addressed, если `protocol=SMPP`) | submit-команда |
| Operator HTTP Gateway | gRPC (instance-addressed, если `protocol=HTTP`) | submit-команда |

**Синхронные вызовы, отправляемые сервисом:** Delivery → Operator SMPP Session Manager либо Operator HTTP Gateway, protocol-aware dispatch по результату Routing.

---

## 1.9. Delivery Reconciliation Service (Java)

**Состояние:** stateless сам по себе; reconciliation case — в PostgreSQL.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.delivery-reconciliation` | Kafka | команда при `SUBMISSION_OUTCOME_UNKNOWN` |
| `operator.submit.accepted` | Kafka | доказательство (поздний accepted submit, протокол-независимо) |
| `delivery.status` | Kafka | доказательство (нормализованный DLR, протокол-независимо) |
| `config.changes` | Kafka (compacted, local snapshot) | per-operator политика `query_sm`, deadline |
| Runtime Redis | Redis (read) | operator route registry (только SMPP — `query_sm` не определён для HTTP-операторов) |
| PostgreSQL | SQL (read) | существующий reconciliation case |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | `CONFIRMED_SUBMITTED` / `CONFIRMED_NOT_SUBMITTED` / `DELIVERY_CONFIRMED` / `DELIVERY_FAILED` / `DELIVERY_UNRESOLVED` |
| Operator SMPP Session Manager | gRPC (instance-addressed, опционально) | `query_sm` |
| PostgreSQL | SQL (write) | reconciliation case |

**Синхронные вызовы, отправляемые сервисом:** Reconciliation → Operator SMPP Session Manager (`query_sm`, опционально).

---

# 2. Stateful processing

## 2.1. Scheduler — Critical Sweep (Go)

**Состояние:** нет собственного (не Kafka Streams, не embedded KV). Авторитетные данные — Execution State и `deadlines:{bucket}` sorted set в Runtime Redis, которые пишет Pipeline Engine.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Runtime Redis | Redis (read, poll раз в ~1с) | `ZRANGEBYSCORE deadlines:{bucket} -inf now` по каждому bucket — просроченные `stage_execution_id` |
| Runtime Redis | Redis (read, только по найденным просрочкам) | `attempt`, `current_state` из Execution State для решения retry/exhausted |
| `scheduler.critical.commands` | Kafka | `FORCE_TIMEOUT` / `FORCE_RETRY` от Backoffice API — единственный producer, ограниченный список `task_type`, аудируется (HLD §9.1) |
| `execution.control` | Kafka (compacted, local snapshot) | admission/dispatch state — повторная проверка перед retry |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.destination-resolution` / `stage.policy` / `stage.billing` / `stage.routing` / `stage.delivery` | Kafka | delayed retry (тот же `stage_execution_id`, `attempt + 1`) |
| `stage.completed` | Kafka | `TIMED_OUT` / `RETRY_EXHAUSTED` |
| Stage DLQ (`stage.*.dlq`) | Kafka | poison / исчерпанные безопасные retry |
| Runtime Redis | Redis (write) | `ZREM` обработанной записи из `deadlines:{bucket}` |

Не подписан ни на один `stage.*` топик и не имеет своего changelog — заменяет прежнюю Kafka Streams-модель (HLD §9.1). Точность обнаружения таймаута — с точностью до интервала опроса (~1с), не мгновенная.

---

## 2.2. Scheduler — Standard Lane (Java, Kafka Streams)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `scheduler.standard.commands` | Kafka | hold-команда от Pipeline Engine (`message_id`, `stage_execution_id`, `scope`, `stage_name`) — единственный producer (HLD §9.2) |
| `execution.control` | Kafka (compacted, local snapshot) | admission/dispatch state |
| собственный changelog | Kafka | восстановление |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.destination-resolution` / `stage.policy` / `stage.billing` / `stage.routing` / `stage.delivery` | Kafka | controlled release (ramp-up после `PAUSED`), с повторной проверкой `execution.control` перед публикацией |
| `stage.delivery-reconciliation` | Kafka | reconciliation wake-up |
| Execution Control Service | сигнал hold backlog (Prometheus-метрики, не Kafka) | сигнал накопленного backlog |
| собственный changelog | Kafka (transactional) | запись state |

---

## 2.3. Scheduler — Background Lane (Java, Kafka Streams)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `scheduler.background.commands` | Kafka | `DLR_CORRELATION_RETRY` от DLR Manager; `NOTIFICATION_RETRY` от Partner Notification Service |
| собственный changelog | Kafka | восстановление |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `operator.dlr.unresolved` | Kafka | повторная попытка DLR-корреляции |
| `notification.retry` | Kafka | повторная попытка партнёрского уведомления |
| Execution Control Service | сигнал background pressure (Prometheus-метрики, не Kafka) | сигнал нагрузки фоновых задач |
| собственный changelog | Kafka (transactional) | запись state |

---

## 2.4. Message State Resolver (Java, Kafka Streams)

**Состояние:** `message-state.changelog` (authoritative) + локальный disposable RocksDB.

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `stage.completed` | Kafka | внутренний технический результат стадии |
| `delivery.status` | Kafka | нормализованный DLR |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `message.lifecycle` | Kafka | партнёрский переход статуса (в одной транзакции с изменением state и commit offset) |
| `message-state.changelog` | Kafka (transactional) | новое состояние: `current_external_status`, `lifecycle_version`, `terminal`, `last_event_id` |

---

# 3. Control Plane

## 3.1. Execution Control Service (Go, включает Stage Availability Controller)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Prometheus | HTTP (Prometheus API) | Kafka consumer lag, error rate по стадиям |
| Scheduler (все три lane) | Prometheus-метрики | сигналы backlog/lateness/hold/background pressure |
| Billing Reconciliation | gRPC | команда freeze/unfreeze партнёра (`PARTNER_STAGE=BILLING`) |
| Backoffice API | gRPC | manual override (scope, state, `expires_at`) |
| PostgreSQL | SQL (read) | сохранённые правила/overrides |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `execution.control` | Kafka (compacted) | `scope`, `scope_id`, `state`, `admission_rate`, `dispatch_rate`, `reason`, `version`, `expires_at` |
| PostgreSQL | SQL (write) | аудит изменений/override |

---

## 3.2. Configuration Service (Go)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Backoffice API | gRPC | CRUD-команда конфигурации |
| PostgreSQL | SQL (read) | текущие версии для валидации |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| PostgreSQL | SQL (write, одна транзакция) | immutable config version + `config_outbox` |

## 3.3. Config Event Publisher (Go)

**Вход:** PostgreSQL (read `config_outbox`).
**Выход:** `config.changes` — Kafka (compacted).

## 3.4. Config Cache Projector (Go)

**Вход:** `config.changes` — Kafka.
**Выход:** Configuration Redis — Redis (write, проекция).

---

# 4. DLR-сервисы

## 4.1. DLR Correlation Writer (Go)

**Вход:** `operator.submit.accepted` — Kafka.

**Выход:** PostgreSQL — batch insert: `operator_id`, `smsc_message_id`, `message_id`, `stage_execution_id`, `segment_id`, `submitted_at`, `expires_at`.

## 4.2. DLR Manager (Go)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `operator.dlr` | Kafka | сырой DLR оператора |
| `operator.dlr.unresolved` | Kafka | повторная попытка корреляции |
| PostgreSQL | SQL (read) | correlation lookup по `operator_id + smsc_message_id` |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `delivery.status` | Kafka | нормализованный статус |
| `scheduler.background.commands` | Kafka | `DLR_CORRELATION_RETRY`, если mapping не найден |
| `operator.dlr.dlq` | Kafka | окно корреляции истекло |

---

# 5. Billing platform services

## 5.1. Billing Outbox Publisher (Java)

**Вход:** Billing Redis (read durable stream).
**Выход:** `billing.ledger` — Kafka.

## 5.2. Billing Ledger Writer (Java)

**Вход:** `billing.ledger` — Kafka.
**Выход:** PostgreSQL (write double-entry ledger, compensating entries).

## 5.3. Billing Reconciliation (Java)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Billing Redis | Redis (read) | оперативный баланс |
| PostgreSQL | SQL (read) | ledger |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| Billing Redis | Redis (write, только fenced CAS после freeze+drain) | восстановленный баланс, новый `account_epoch` |
| PostgreSQL | SQL (write) | audit trail |
| Execution Control Service | gRPC | freeze/unfreeze команда для `PARTNER_STAGE=BILLING` |

---

# 6. Projection services

## 6.1. Lifecycle Writer (Go)

**Вход**

| Источник | Канал |
|---|---|
| `incoming.messages` | Kafka |
| `message.lifecycle` | Kafka |
| Stage DLQ (`stage.*.dlq`) | Kafka |
| `operator.dlr.dlq` | Kafka |

**Выход:** PostgreSQL — batch write (read model сообщения, lifecycle history, DLQ record с исходной stage-командой для replay).

`stage.completed` намеренно не читается — детальная пер-стадийная история идёт только в ClickHouse через Analytics Writer (см. HLD §18).

## 6.2. Analytics Writer (Go)

**Вход**

| Источник | Канал |
|---|---|
| `incoming.messages` | Kafka |
| `stage.completed` | Kafka |
| `message.lifecycle` | Kafka |

**Выход:** ClickHouse — batch write.

---

# 7. Partner и Management Plane

## 7.1. Partner Notification Service (Go)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| `message.lifecycle` | Kafka | партнёрский переход статуса |
| `notification.retry` | Kafka | повторная попытка после неудачной доставки |
| Runtime Redis | Redis (read) | lookup инстанса Partner SMPP Gateway + `session_epoch` |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| Partner SMPP Gateway | gRPC mTLS (instance-addressed) | команда `deliver_sm` |
| Партнёр | REST callback / WebSocket | статус |
| `scheduler.background.commands` | Kafka | задача `NOTIFICATION_RETRY` при неудачной попытке доставки |

**Синхронные вызовы, отправляемые сервисом:** Notification → Partner SMPP Gateway.

## 7.2. Partner API (Go)

**Вход**

| Источник | Канал |
|---|---|
| REST-клиенты партнёров | HTTP REST (статус/поиск/отчёты) |
| PostgreSQL | SQL (read) |
| ClickHouse | SQL (read) |

**Выход:** HTTP-ответ партнёру. Служит надёжным pull-fallback для статуса, если push-уведомление не доставлено (HLD §19).

## 7.3. Backoffice API (Go)

**Вход:** Backoffice UI — HTTP.

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| Configuration Service | gRPC | CRUD конфигурации |
| Execution Control Service | gRPC | manual override |
| Scheduler Critical Sweep | Kafka: `scheduler.critical.commands` | `FORCE_TIMEOUT` / `FORCE_RETRY` для конкретного `stage_execution_id` |
| PostgreSQL | SQL (read) | поиск сообщений, DLQ, reconciliation cases |
| ClickHouse | SQL (read) | отчётность |
| Replay Service | gRPC | создание/подтверждение replay |
| Backoffice UI | HTTP-ответ | — |

## 7.4. Replay Service (Go)

**Вход**

| Источник | Канал | Данные |
|---|---|---|
| Backoffice API | gRPC | replay-команда |
| PostgreSQL | SQL (read) | DLQ record (исходная stage-команда + причина), записанный Lifecycle Writer |

**Выход**

| Назначение | Канал | Данные |
|---|---|---|
| `stage.destination-resolution` / `stage.policy` / `stage.billing` / `stage.routing` / `stage.delivery` / `stage.delivery-reconciliation` | Kafka | republish после проверок (TTL, idempotency, billing side effect, delivery ambiguity) |
| PostgreSQL | SQL (write) | журнал replay-действия: кто, когда, `stage_execution_id`, результат проверок |

## 7.5. Backoffice UI (Vue 3)

**Вход:** пользователи Backoffice — браузер.
**Выход:** Backoffice API — HTTP (OpenAPI-generated client). Напрямую к PostgreSQL/ClickHouse не обращается.

---

# 8. История изменений

**v1** — первичный обход `hld.md` + `services_specifictaion.md`, обнаружено 8 связей без чёткого источника/канала.

**v2** (текущая) — все 8 закрыты правкой обоих документов:

1. Партнёрская/операторская конфигурация подключена к REST Receiver, Partner SMPP Gateway, Operator Session Manager через `config.changes` (HLD §16).
2. `scheduler.critical.commands` получил единственного producer'а — ручной `FORCE_TIMEOUT`/`FORCE_RETRY` из Backoffice API, аудируемый (HLD §9.1).
3. `scheduler.standard.commands` получил явного producer'а — hold-команда от Pipeline Engine при обнаружении `PAUSED` (HLD §9.2).
4. Зафиксирована рациональность неравномерного подключения `execution.control`: Policy полагается на admission-проверку Pipeline; Billing/Routing/Delivery проверяют повторно перед необратимым действием (HLD §8). Delivery добавлен в список прямых потребителей.
5. Retry партнёрских уведомлений явно проведён через Background Lane и новый топик `notification.retry`; описано поведение после исчерпания TTL — pull-fallback через Partner API (HLD §19).
6. Источник данных для Replay Service зафиксирован — PostgreSQL DLQ record, а не прямое чтение Kafka DLQ-топиков (HLD §20).
7. Журнал replay-действий закреплён за PostgreSQL, в общей базе с остальным аудитом платформы.
8. Ранее не помеченные связи (`BillingReconciliation → ExecutionControl`, `BackofficeAPI → ConfigService/ExecutionControl/ReplayService`) явно помечены как обычный внутренний gRPC, отдельная категория от instance-addressed RPC (HLD §5.4, services_specifictaion.md §9.3).

**v3** (текущая) — внедрены оптимизации капасити-модели (полный расчёт — `capacity_model.md`):

1. Scheduler Critical Lane заменён на Scheduler Critical Sweep: вместо потребления всех stage-топиков (≈160 000 событий/сек) — периодический опрос дедлайна в Runtime Redis, который Pipeline Engine пишет той же Lua-транзакцией, что и CAS. Убирает целую Kafka Streams/RocksDB группу и топик `scheduler.critical.state.changelog`.
2. Rate limit на REST Receiver и Partner SMPP Gateway — локальный token bucket с периодической синхронизацией в Runtime Redis (раз в ~1с), а не Redis-запрос на каждое сообщение.
3. Billing больше не читает `msgctx` из Runtime Redis — `segment_count`/длина приходят полем в `BillingExecute`, посчитанные один раз Pipeline Engine.
4. Lifecycle Writer больше не читает `stage.completed` — детальная пер-стадийная история идёт только в ClickHouse через Analytics Writer; PostgreSQL хранит только партнёрский read model и lifecycle history.
5. Policy компилирует правила per-partner, а не единым автоматом на все правила платформы.

**v4** (текущая) — заложена основа мультиканальности и протокольная двойственность исходящего трафика:

1. Новая стадия **Destination Resolution** — резолв оператора по номерному диапазону, всегда первая в pipeline, до Policy (правила шаблонов зависят от оператора). `Pipeline Engine → 120 000 событий/сек` вместо 100 000 (5 обязательных стадий вместо 4).
2. Новый сервис **Operator HTTP Gateway** (Go) — симметричен Operator SMPP Session Manager, но проще: нет постоянного сокета, только владение маршрутом через тот же Runtime Redis registry (не отдельная синхронизация лимита — по решению пользователя, одновременно активен только один протокол на оператора).
3. Переименования для протокольной симметрии: `REST Receiver` → **Partner REST Receiver**, `Operator Session Manager` → **Operator SMPP Session Manager**.
4. Operator Route Registry обобщён на оба протокола: `bind_id` → `route_id` + поле `protocol` (`SMPP`/`HTTP`).
5. Routing Service резолвит route + protocol внутри уже известного оператора (не оператора с нуля); Delivery Service делает protocol-aware dispatch на один из двух шлюзов.
6. Общий контракт стадии получил поле `channel` (SMS сегодня, EMAIL/PUSH зарезервированы) и дискриминированный по каналу payload — основа для будущих каналов без изменения Pipeline Engine/Billing/Scheduler/Message State Resolver.
7. Policy Engine получил детализированные требования (template matching + категоризация, anti-spam, time-of-day, consent-блэклисты, банворды, sender validation) и решение по хранению: per-partner incremental snapshot вместо БД в hot path, партиционирование по партнёру — задокументированный путь масштабирования.
