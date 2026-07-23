# A2P MPP — Внутренние методы сервисов

**Основание:** `hld.md`, `services_specifictaion.md`, `service_io_contracts.md`
**Назначение:** для каждого сервиса — внутренние методы обработки данных: название, вход, выход. Это разбивка входов/выходов сервиса (уже зафиксированных в `service_io_contracts.md`) на конкретные шаги обработки внутри него. Сигнатуры даны на уровне LLD-проектирования (типы — концептуальные, не финальные protobuf/Rust/Java-типы).

## Одно сквозное архитектурное решение, зафиксированное здесь впервые

Ни в HLD, ни в спецификации сервисов не был решён вопрос: дублируется ли тело сообщения (`body`) в каждом `stage.*` событии, или передаётся по ссылке. Дублирование раздувает каждое из ~80 000 событий `stage.completed`/сек и 4×20 000 команд `stage.*Execute`/сек на размер тела сообщения — это прямые лишние мегабайты/сек в Kafka без необходимости (Policy/Billing/Delivery нужен body, Routing и Pipeline Engine — нет).

**Решение:** `incoming.messages` несёт полный immutable context, включая body. Pipeline Engine, обработав `incoming.messages`, дополнительно кэширует body и служебные поля в Runtime Redis по ключу `msgctx:{message_id}` (TTL = `message_ttl` + safety margin — так же, как Execution State). Все `stage.*Execute` команды несут только `message_id` + управляющие поля (без body). Стадии, которым нужен body (Policy, Billing, Delivery), читают его из Runtime Redis по `message_id`. Это отражено ниже как метод `cache_message_context` у Pipeline Engine и `fetch_message_context` у потребителей.

---

# 1. Data Plane и business-стадии

## 1.1. Partner REST Receiver

| Метод | Вход | Выход |
|---|---|---|
| `validate_request_schema` | сырой HTTP-запрос | `ValidatedRequest` \| `ValidationError` |
| `authenticate_partner` | credentials из запроса, local config snapshot | `PartnerContext` \| `AuthError` |
| `check_ip_and_application` | `PartnerContext`, remote IP, `application_id` | `Allowed` \| `Denied` |
| `check_admission` | `ControlSnapshot`, `partner_id`, scope-ключи | `Admit` \| `Reject(retry_after)` |
| `check_rate_limit` | `partner_id`, `application_id`, локальный in-memory token bucket | `Allow` \| `Throttle` |
| `sync_rate_limit_counters` | таймер (~1с) | чтение/запись агрегированного счётчика в Runtime Redis — не на каждое сообщение |
| `generate_message_id` | — | `MessageId` |
| `generate_trace_id` | — | `TraceId` |
| `build_incoming_message` | `ValidatedRequest`, `MessageId`, `TraceId`, `PartnerContext` | `IncomingMessage` |
| `publish_incoming` | `IncomingMessage` | `KafkaAck` |
| `build_ack_response` | `KafkaAck` | `HttpResponse` (200/202 или 429/503) |

## 1.2. Partner SMPP Gateway

| Метод | Вход | Выход |
|---|---|---|
| `handle_bind` | `BindPdu`, local config snapshot | `BindResponse` \| `BindReject` |
| `register_session` | `session_id`, `partner_id`, `gateway_instance_id` | запись в Runtime Redis registry |
| `handle_unbind` | `UnbindPdu`, session | `UnbindResponse` |
| `validate_submit_pdu` | `SubmitSmPdu` | `ValidatedSubmit` \| `PduError` |
| `check_admission` | `ControlSnapshot`, `partner_id`/scope | `Admit` \| `ESME_RTHROTTLED` |
| `check_rate_limit` | `partner_id`, локальный in-memory token bucket | `Allow` \| `ESME_RTHROTTLED` |
| `sync_rate_limit_counters` | таймер (~1с) | синхронизация счётчика в Runtime Redis |
| `build_incoming_message` | `ValidatedSubmit`, session | `IncomingMessage` |
| `publish_incoming` | `IncomingMessage` | `KafkaAck` |
| `send_submit_sm_resp` | `KafkaAck`, PDU sequence | `SubmitSmResp` |
| `handle_deliver_sm_command` | gRPC-команда от Notification (instance-addressed) | проверка `session_epoch` → `DeliverSmPdu` \| `StaleEpochReject` |
| `send_deliver_sm` | `DeliverSmPdu`, активная сессия | `DeliverSmResp` |
| `handle_enquire_link` | `EnquireLinkPdu` | `EnquireLinkResp` |
| `heartbeat_tick` | таймер | обновление `heartbeat` в Runtime Redis registry |
| `handle_partner_query_sm` | `QuerySmPdu` от партнёра | `QuerySmResp` (из PostgreSQL read model через Partner API-совместимый lookup) |

## 1.3. Operator SMPP Session Manager

| Метод | Вход | Выход |
|---|---|---|
| `bind_operator` | operator config (из local snapshot) | `BindResult` |
| `register_route` | `operator_id`, `route_id`, `protocol=SMPP`, `owning_instance_id` | запись в Runtime Redis registry (общий с Operator HTTP Gateway) |
| `enforce_window_and_tps` | `operator_id`, текущее окно | `Permit` \| `Wait` |
| `handle_submit_command` | gRPC-команда от Delivery (instance-addressed) | `enforce_window_and_tps` → `submit_sm` |
| `send_submit_sm` | сегмент, `queue_msg_id` | PDU в SMPP-сессию |
| `handle_submit_sm_resp` | `SubmitSmResp` PDU | `SubmitOutcome` (accepted/rejected/ambiguous) |
| `publish_submit_accepted` | `SubmitOutcome` (accepted) | `KafkaAck` на `operator.submit.accepted` |
| `reply_submit_result` | `SubmitOutcome` | gRPC-ответ Delivery |
| `handle_raw_dlr` | `DeliverSmPdu` (DLR от оператора) | `RawDlr` |
| `publish_operator_dlr` | `RawDlr` | `KafkaAck` на `operator.dlr` |
| `enforce_query_sm_priority` | `operator_id`, текущая submit-нагрузка | `Permit` \| `Defer` (submit имеет приоритет) |
| `handle_query_sm_command` | gRPC-команда от Reconciliation (опционально) | `enforce_query_sm_priority` → `QuerySmResult` |
| `reconnect` | `operator_id`, `reconnect_policy` | `ReconnectResult` |
| `enquire_link_tick` | таймер | `EnquireLinkPdu` |

## 1.3a. Operator HTTP Gateway

| Метод | Вход | Выход |
|---|---|---|
| `register_route` | `operator_id`, `route_id`, `protocol=HTTP`, `owning_instance_id` | запись в Runtime Redis registry (общий с Operator SMPP Session Manager) |
| `enforce_tps` | `operator_id`, текущий счётчик (локальный, инвариант владеющего инстанса) | `Permit` \| `Wait` |
| `handle_submit_command` | gRPC-команда от Delivery (instance-addressed) | `enforce_tps` → HTTP submit-запрос |
| `send_http_submit` | сегмент, `queue_msg_id`, `endpoint_url` | HTTP-запрос оператору |
| `handle_http_submit_response` | HTTP-ответ оператора | `SubmitOutcome` (accepted/rejected/ambiguous) |
| `publish_submit_accepted` | `SubmitOutcome` (accepted) | `KafkaAck` на `operator.submit.accepted` (тот же формат, что у SMPP) |
| `reply_submit_result` | `SubmitOutcome` | gRPC-ответ Delivery |
| `handle_dlr_webhook` | входящий HTTP POST от оператора | проверка `webhook_auth` → `RawDlr` \| `AuthReject` |
| `normalize_and_publish_dlr` | `RawDlr` (webhook-формат) | `KafkaAck` на `operator.dlr` (нормализовано в тот же формат, что и сырой SMPP DLR) |
| `heartbeat_tick` | таймер | обновление `heartbeat` владения маршрутом в Runtime Redis registry |

## 1.4. Pipeline Engine

| Метод | Вход | Выход |
|---|---|---|
| `handle_incoming` | `IncomingMessage` | инициализация Execution State, первая стадия всегда `DestinationResolution` |
| `cache_message_context` | `IncomingMessage` | запись `msgctx:{message_id}` в Runtime Redis |
| `resolve_pipeline_version` | `partner_id`, `application_id`, local config snapshot | `PipelineVersion` (оператор ещё не известен на этом шаге — резолвится Destination Resolution) |
| `resolve_next_stage` | `ExecutionState`, `PipelineDefinition` | `NextStageDecision` \| `Terminal` — жёстко зафиксировано в двух местах графа (остальное — конфигурация): (1) `DestinationResolution` → `Policy` всегда первый переход; (2) `Policy.outcome == REJECTED` → всё равно `Billing` (не `Terminal` напрямую), см. `handle_stage_completed` ниже |
| `check_admission` | `ControlSnapshot`, scope-ключи текущей стадии | `Admit` \| `Hold` |
| `cas_transition_and_track_deadline` | `message_id`, expected state, new state, `deadline` | `CasResult` (success/conflict) — один атомарный Lua-вызов в Runtime Redis: CAS Execution State **+** `ZADD`/`ZREM` в `deadlines:{bucket}` (не отдельный round trip) |
| `build_stage_execute` | `NextStageDecision`, `ExecutionState` | `StageExecuteCommand`, включает `segment_count`/длину и `category` (из Policy-результата, включая `"BLOCKED"`) для Billing (без отдельного чтения контекста этой стадией) |
| `publish_stage_execute` | `StageExecuteCommand` | `KafkaAck` на соответствующий `stage.*` |
| `publish_hold_command` | `ExecutionState`, заблокированный scope | `KafkaAck` на `scheduler.standard.commands` |
| `handle_stage_completed` | `StageCompletedEvent` | обновление Execution State → `resolve_next_stage`. Особый случай: если завершилась `Billing`, а сохранённая в Execution State `category` (записанная при диспетчеризации Billing) равна `"BLOCKED"` — следующая стадия `Terminal`, Routing/Delivery пропускаются, независимо от того, что граф конфигурации мог бы предписать для обычного `SUCCEEDED` от Billing (HLD §5.3.1) |
| `finalize_pipeline` | `ExecutionState` (terminal) | освобождение Execution State (TTL) |

## 1.4a. Destination Resolution Service

| Метод | Вход | Выход |
|---|---|---|
| `handle_destination_resolution_execute` | `DestinationResolutionExecuteCommand` (msisdn/destination address, `channel`) | оркестрация ниже |
| `resolve_operator_by_range` | destination address, local snapshot (номерные диапазоны) | `resolved_operator_id` \| `NotFound` |
| `publish_stage_completed` | `resolved_operator_id` \| `NotFound` | `KafkaAck` (`SUCCEEDED` с `resolved_operator_id`, либо `REJECTED`) |

Live HLR-резолв для MNP не входит в первую версию (HLD §26) — `resolve_operator_by_range` работает только со статической таблицей.

## 1.5. Policy Service

Восемь под-проверок выполняются последовательно; порядок ниже — по стоимости и по зависимостям (категория из template matching нужна проверкам 3 и 4).

| Метод | Вход | Выход |
|---|---|---|
| `handle_policy_execute` | `PolicyExecuteCommand` (включает `resolved_operator_id`) | оркестрация проверок ниже |
| `fetch_message_context` | `message_id` | `MessageContext` (body и др.) из Runtime Redis |
| `validate_sender` | `MessageContext`, `PartnerContext`, `CompiledRuleset` | `SenderCheckResult` — требование 7, легитимность sender_id для партнёра/оператора |
| `check_sender_blacklist` | `msisdn`, `sender_id`, Runtime Redis (consent-блэклист по отправителю) | `Allowed` \| `Blocked` — требование 6 |
| `match_template` | `body`, `resolved_operator_id`, `CompiledRuleset` (мини-движок, правила per-operator) | `MatchedTemplate{category}` \| `NoMatch` — требование 1. Алгоритм: Aho-Corasick отбирает кандидатов по литеральным фрагментам шаблона, затем точечная проверка плейсхолдеров `%w`/`%d{n,m}` на кандидате (`data_infrastructure_spec.md` §1.9b) |
| `resolve_unmatched_behavior` | `NoMatch`, `partner_id`, local snapshot (per-partner политика) | `category = "UNTEMPLATED"` (продолжить) \| `category = "BLOCKED"` (отклонить) — требование 1, поведение при отсутствии шаблона зависит от партнёра, не от оператора |
| `check_banwords` | `body` (нормализованный), мультиязычный banword-список (узб. латиница/рус. кириллица/англ.) | `BanwordCheckResult` — требование 5 |
| `normalize_for_banwords` | сырой `body` | нормализованный текст: unicode NFKC + схлопывание кириллица↔латиница гомоглифов + снятие разделителей — отдельный шаг перед `check_banwords`, требование 5 |
| `check_category_blacklist` | `msisdn`, `category`, Runtime Redis (consent-блэклист по категории) | `Allowed` \| `Blocked` — требование 4, нужна категория из `match_template` |
| `check_time_of_day` | `category`, текущее время, local snapshot (расписание по категории) | `Allowed` \| `Blocked` — требование 3, нужна категория |
| `check_spam_frequency` | `msisdn`, `category`, Runtime Redis (счётчик частоты за окно) | `Allowed` \| `Throttled` — требование 2 |
| `increment_spam_counter` | `msisdn`, `category` | запись в Runtime Redis (только если сообщение пропущено) |
| `resolve_ruleset` | `partner_id`, `resolved_operator_id`, local snapshot | `CompiledRuleset` — скомпилированный **отдельно на партнёра/группу партнёров**, не единый автомат на все правила платформы; проверяется только релевантное подмножество (~2.5× дешевле по CPU, см. `capacity_model.md`); шаблоны внутри этой структуры дополнительно секционированы по `resolved_operator_id` |
| `aggregate_result` | набор `CheckResult` от всех восьми проверок | `PolicyOutcome`. `category` **всегда** непустая: из совпавшего шаблона (`SUCCEEDED`) / `"UNTEMPLATED"` (`SUCCEEDED`) / `"BLOCKED"` (`REJECTED`, любая из проверок 1/2/3/4/5/6/7 — единая категория и `reason_code` указывает конкретную причину, но категория для тарификации одна) |
| `publish_stage_completed` | `PolicyOutcome` | `KafkaAck` — `REJECTED` с `category="BLOCKED"` публикуется так же, как `SUCCEEDED`: Pipeline Engine всё равно диспетчеризует Billing (HLD §5.3.1) |

Восьмая проверка ("аналогичные для email/push") в методах не детализируется — она появится как новые реализации `match_template`/`check_*` под `channel != SMS`, не новый оркестрирующий метод.

## 1.6. Billing Service

| Метод | Вход | Выход |
|---|---|---|
| `handle_billing_execute` | `BillingExecuteCommand` | оркестрация ниже |
| `read_segment_hint` | `BillingExecuteCommand.segment_count` | длина/число сегментов — поле команды, без чтения Runtime Redis (посчитано один раз Pipeline Engine, см. §0) |
| `resolve_tariff` | `partner_id`, `category` (из `BillingExecuteCommand`, включая служебные `UNTEMPLATED`/`BLOCKED`), `segment_count`, local snapshot | `Tariff` — чистый lookup `price_per_segment[category] × segment_count`, без решения о продолжении pipeline (`data_infrastructure_spec.md` §1.6a) |
| `check_account_state` | `account_id`, Billing Redis | `Active` \| `Frozen` |
| `check_charge_dedup` | `charge_id` (= `stage_execution_id`), Billing Redis | `New` \| `AlreadyProcessed` |
| `apply_atomic_charge` | `account_id`, `amount`, `charge_id`, `account_epoch` | `ChargeResult` — Lua/Redis Function, атомарно с `check_account_state`+`check_charge_dedup` |
| `write_outbox_entry` | `ChargeResult` | запись в Billing Redis durable stream |
| `publish_stage_completed` | `ChargeResult` | `KafkaAck` (в т.ч. `ACCOUNT_FROZEN, retryable=true`) |

## 1.7. Routing Service

| Метод | Вход | Выход |
|---|---|---|
| `handle_routing_execute` | `RoutingExecuteCommand` (включает `resolved_operator_id` из Destination Resolution) | оркестрация ниже |
| `resolve_route_table` | `pipeline_version`, local snapshot | `RouteTable` (маршруты обоих протоколов) |
| `select_routes_for_operator` | `resolved_operator_id`, `RouteTable` | `CandidateRoutes[]` (SMPP и/или HTTP) |
| `filter_by_control_state` | `CandidateRoutes[]`, `ControlSnapshot` (`OPERATOR_ROUTE`) | `HealthyCandidates[]` |
| `select_route_and_protocol` | `HealthyCandidates[]`, Operator Route Registry (Runtime Redis) | `route_id`, `protocol` |
| `apply_failover` | `HealthyCandidates[]`, недоступность primary | `FinalRoute` (primary/reserve, может менять протокол) |
| `publish_stage_completed` | `FinalRoute` (`route_version`, `route_id`, `protocol`) | `KafkaAck` |

## 1.8. Delivery Service

| Метод | Вход | Выход |
|---|---|---|
| `handle_delivery_execute` | `DeliveryExecuteCommand` (включает `route_id`, `protocol`) | оркестрация ниже |
| `fetch_message_context` | `message_id` | `MessageContext` (body) |
| `check_control_state` | `ControlSnapshot`, `OPERATOR_ROUTE` из команды | `Admit` \| `Hold` (повторная проверка перед submit) |
| `segment_message` | body, encoding | `Segments[]` |
| `generate_queue_msg_id` | — | `QueueMsgId` |
| `resolve_gateway_instance` | `operator_id`, `route_id`, `protocol`, Runtime Redis registry | `GatewayEndpoint` — **protocol-aware**: `SMPP` → Operator SMPP Session Manager, `HTTP` → Operator HTTP Gateway |
| `call_submit` | `GatewayEndpoint`, `Segments[]`, `QueueMsgId` | `SubmitRpcResult` (gRPC, одинаковый контракт независимо от протокола) |
| `interpret_submit_result` | `SubmitRpcResult` | `SUBMITTED` \| `FAILED` \| `SUBMISSION_OUTCOME_UNKNOWN` |
| `publish_stage_completed` | результат | `KafkaAck` |

## 1.9. Delivery Reconciliation Service

| Метод | Вход | Выход |
|---|---|---|
| `handle_reconciliation_execute` | `ReconciliationExecuteCommand` | создание/загрузка `ReconciliationCase` |
| `collect_evidence` | `operator.submit.accepted`, `delivery.status`, локальные протокольные данные | `Evidence` |
| `check_query_sm_policy` | `operator_id`, local config snapshot | `Enabled` \| `Disabled` (только для `protocol=SMPP` — у HTTP нет `query_sm`-эквивалента) |
| `resolve_gateway_instance` | `operator_id`, `route_id`, Runtime Redis registry | `SmppGatewayEndpoint` |
| `call_query_sm` | `SmppGatewayEndpoint` | `QuerySmResult` (gRPC, только если `Enabled`) |
| `evaluate_deadline` | `ReconciliationCase`, `operator_specific_deadline` | `Continue` \| `Expire` |
| `resolve_outcome` | накопленный `Evidence` | `CONFIRMED_SUBMITTED` \| `CONFIRMED_NOT_SUBMITTED` \| `DELIVERY_CONFIRMED` \| `DELIVERY_FAILED` \| `DELIVERY_UNRESOLVED` |
| `persist_case` | `ReconciliationCase` | запись/обновление в PostgreSQL |
| `publish_stage_completed` | итоговый outcome | `KafkaAck` |

---

# 2. Stateful processing

## 2.1. Scheduler — Critical Sweep

Пересмотрено: больше не подписывается на stage-топики и не имеет embedded state store/changelog (HLD §9.1). Дедлайн пишет и удаляет Pipeline Engine той же атомарной операцией, что и CAS (`cas_transition_and_track_deadline`, §1.4).

| Метод | Вход | Выход |
|---|---|---|
| `tick_sweep` | таймер (~1с), список bucket'ов | обход `ZRANGEBYSCORE deadlines:{bucket} -inf now` по каждому bucket → `ExpiredEntries[]` |
| `load_execution_state` | `stage_execution_id` из `ExpiredEntries` | `attempt`, `current_state` из Runtime Redis (только по найденным просрочкам, не по всему потоку) |
| `on_manual_command` | событие `scheduler.critical.commands` (`FORCE_TIMEOUT`/`FORCE_RETRY`) | принудительная обработка конкретного `stage_execution_id`, минуя sweep |
| `check_execution_control` | `ExpiredEntry`, `ControlSnapshot` | `Proceed` \| `Hold` — повторная проверка перед retry |
| `evaluate_retry_policy` | `ExpiredEntry`, `attempt`, retry policy стадии | `Retry` \| `Exhausted` |
| `publish_retry` | `stage_execution_id` (тот же), `attempt + 1` | `KafkaAck` на исходный `stage.*` |
| `publish_timeout_result` | `ExpiredEntry` (без retry) | `KafkaAck` на `stage.completed` (`TIMED_OUT`) |
| `publish_dlq` | `Exhausted` | `KafkaAck` на `stage.*.dlq` |
| `clear_deadline` | обработанный `ExpiredEntry` | `ZREM` из `deadlines:{bucket}` в Runtime Redis |

## 2.2. Scheduler — Standard Lane

| Метод | Вход | Выход |
|---|---|---|
| `on_hold_command` | событие `scheduler.standard.commands` от Pipeline Engine | регистрация hold в state store |
| `on_control_state_change` | `execution.control` | `ReleaseDecision` для затронутых hold |
| `release_backlog_batch` | scope, текущий ramp-шаг | `ReleasedItems[]` |
| `apply_fair_scheduling` | `ReleasedItems[]` (несколько партнёров) | упорядоченный список для публикации |
| `apply_token_bucket` | scope, лимит | `Permit` \| `Deny` |
| `publish_release` | `ReleasedItem` (с повторной проверкой `execution.control`) | `KafkaAck` на исходный `stage.*` |
| `on_reconciliation_deadline` | таймер reconciliation | `KafkaAck` на `stage.delivery-reconciliation` (wake-up) |
| `checkpoint_state` / `restore_from_changelog` | изменение state store / рестарт-rebalance | транзакционная запись и восстановление через `scheduler.standard.state.changelog` (Kafka Streams EOS) |

## 2.3. Scheduler — Background Lane

| Метод | Вход | Выход |
|---|---|---|
| `on_background_command` | событие `scheduler.background.commands` (`DLR_CORRELATION_RETRY`/`NOTIFICATION_RETRY`) | регистрация задачи в state store |
| `tick_delay_queue` | внутренний таймер | `DueTasks[]` |
| `dispatch_dlr_retry` | `DueTask` (тип DLR) | `KafkaAck` на `operator.dlr.unresolved` |
| `dispatch_notification_retry` | `DueTask` (тип notification) | `KafkaAck` на `notification.retry` |
| `checkpoint_state` / `restore_from_changelog` | изменение state store / рестарт-rebalance | транзакционная запись и восстановление через `scheduler.background.state.changelog` (Kafka Streams EOS) |

## 2.4. Message State Resolver

| Метод | Вход | Выход |
|---|---|---|
| `on_stage_completed` | событие `stage.completed` | `CandidateTransition` |
| `on_delivery_status` | событие `delivery.status` | `CandidateTransition` |
| `load_current_state` | `message_id` | `CurrentState` (локальный RocksDB read) |
| `validate_transition` | `CurrentState`, `CandidateTransition` | `Valid` \| `Regression` (отклонить) \| `Duplicate` (по `event_id`) |
| `apply_transition` | `CurrentState`, валидный `CandidateTransition` | `NewState`, `lifecycle_version + 1` |
| `commit_transaction` | `NewState` | одна Kafka-транзакция: запись `message-state.changelog` + `message.lifecycle` + commit offset |
| `handle_late_evidence` | terminal `CurrentState` (`DELIVERY_UNRESOLVED`) + поздний `CandidateTransition` | `LATE_DELIVERY_CONFIRMED` как новая версия, без переписывания истории |

---

# 3. Control Plane

## 3.1. Execution Control Service

| Метод | Вход | Выход |
|---|---|---|
| `collect_signals` | Prometheus (lag, error rate), gRPC от Scheduler/BillingReconciliation | `RawSignals` |
| `evaluate_hysteresis` | scope, `RawSignal`, текущее состояние, пороги enter/exit | `CandidateState` |
| `apply_dwell_time` | scope, `CandidateState`, `min_state_duration` | `ConfirmedState` \| `Pending` |
| `compose_effective_rate` | все применимые записи по scope-иерархии | `effective_rate = min(...)` |
| `apply_manual_override` | gRPC-запрос от Backoffice API | `AppliedOverride` (с `expires_at`) |
| `compute_ramp_step` | scope, текущий `dispatch_rate`, ramp-политика | следующий шаг (`0→5→10→25→50→100%`) |
| `publish_control_record` | `scope`, `state`, `rate`, `reason`, `version` | `KafkaAck` на `execution.control` |
| `persist_override_audit` | `AppliedOverride` | запись в PostgreSQL |

## 3.2. Configuration Service

| Метод | Вход | Выход |
|---|---|---|
| `validate_config_change` | CRUD-запрос от Backoffice API | `ValidatedChange` \| `ValidationError` |
| `create_immutable_version` | `ValidatedChange` | `ConfigVersion` |
| `write_config_and_outbox` | `ConfigVersion` | одна PostgreSQL-транзакция: `config_versions` + `config_outbox` |
| `handle_crud_request` | gRPC-запрос | `ConfigResponse` |

## 3.3. Config Event Publisher

| Метод | Вход | Выход |
|---|---|---|
| `poll_outbox` | таймер/long-poll на PostgreSQL | `PendingEntries[]` |
| `publish_config_change` | `PendingEntry` | `KafkaAck` на `config.changes` |
| `mark_published` | опубликованная запись | `UPDATE` в PostgreSQL |

## 3.4. Config Cache Projector

| Метод | Вход | Выход |
|---|---|---|
| `on_config_change` | событие `config.changes` | `ProjectionUpdate` |
| `write_projection` | `ProjectionUpdate` | запись в Configuration Redis |

---

# 4. DLR-сервисы

## 4.1. DLR Correlation Writer

| Метод | Вход | Выход |
|---|---|---|
| `on_submit_accepted` | событие `operator.submit.accepted` | `CorrelationRecord` |
| `batch_buffer` | `CorrelationRecord` | накопление в буфере |
| `flush_batch` | буфер (по таймеру/размеру) | `COPY`/batch insert в PostgreSQL |

## 4.2. DLR Manager

| Метод | Вход | Выход |
|---|---|---|
| `on_raw_dlr` | событие `operator.dlr` | `ParsedDlr` |
| `normalize_operator_status` | `ParsedDlr`, `operator_id` | `NormalizedStatus` |
| `lookup_correlation` | `operator_id`, `smsc_message_id` | `CorrelationRecord` \| `NotFound` (PostgreSQL read) |
| `publish_delivery_status` | `NormalizedStatus` + `CorrelationRecord` | `KafkaAck` на `delivery.status` |
| `schedule_retry` | `NotFound` | `KafkaAck` на `scheduler.background.commands` (`DLR_CORRELATION_RETRY`) |
| `evaluate_correlation_window` | повторный `NotFound`, `expires_at` | `Continue` \| `Expire` |
| `publish_dlr_dlq` | `Expire` | `KafkaAck` на `operator.dlr.dlq` |

---

# 5. Billing platform services

## 5.1. Billing Outbox Publisher

| Метод | Вход | Выход |
|---|---|---|
| `poll_redis_stream` | Billing Redis durable stream | `PendingEntries[]` |
| `publish_ledger_event` | `PendingEntry` | `KafkaAck` на `billing.ledger` |
| `ack_stream_entry` | опубликованная запись | `XACK` в Billing Redis |

## 5.2. Billing Ledger Writer

| Метод | Вход | Выход |
|---|---|---|
| `on_ledger_event` | событие `billing.ledger` | `LedgerEntry` |
| `insert_double_entry` | `LedgerEntry` | `INSERT ... ON CONFLICT (charge_id) DO NOTHING` в PostgreSQL |
| `apply_compensating_entry` | `AdjustmentRequest` (от Reconciliation) | `INSERT` компенсирующей записи |

## 5.3. Billing Reconciliation

| Метод | Вход | Выход |
|---|---|---|
| `compute_drift` | баланс Billing Redis, баланс из PostgreSQL ledger | `DriftReport` |
| `classify_drift` | `DriftReport` | `Severity` |
| `trigger_freeze` | `account_id`, `Severity` выше порога | gRPC на Execution Control (`PARTNER_STAGE=BILLING, PAUSED`) |
| `await_outbox_drain` | `account_id` | подтверждение, что Billing Outbox Publisher обработал все pending записи |
| `recompute_balance` | ledger-записи из PostgreSQL | `RecomputedBalance` |
| `apply_fenced_cas` | `RecomputedBalance`, новый `account_epoch` | `CasResult` в Billing Redis |
| `persist_audit` | вся последовательность recovery | запись в PostgreSQL |
| `trigger_unfreeze` | завершённый recovery | gRPC на Execution Control (`ACTIVE`) |

---

# 6. Projection services

## 6.1. Lifecycle Writer

| Метод | Вход | Выход |
|---|---|---|
| `on_event` | событие из `incoming.messages`/`message.lifecycle`/DLQ-топиков (без `stage.completed` — детальная история идёт только в ClickHouse через Analytics Writer, HLD §18) | `NormalizedRecord` |
| `batch_buffer` | `NormalizedRecord` | накопление в буфере (per-таблица) |
| `flush_batch` | буфер | batch `UPSERT`/`INSERT` (COPY) в PostgreSQL: read model, lifecycle history, DLQ record |

## 6.2. Analytics Writer

| Метод | Вход | Выход |
|---|---|---|
| `on_event` | событие из `incoming.messages`/`stage.completed`/`message.lifecycle` | `NormalizedRecord` |
| `batch_buffer` | `NormalizedRecord` | накопление в буфере |
| `flush_batch` | буфер | batch insert в ClickHouse |
| `refresh_materialized_view` | по расписанию | обновление агрегатов ClickHouse |

---

# 7. Partner и Management Plane

## 7.1. Partner Notification Service

| Метод | Вход | Выход |
|---|---|---|
| `on_lifecycle_event` | событие `message.lifecycle` \| `notification.retry` | `NotificationTask` |
| `resolve_delivery_channel` | `partner_id`, local config snapshot | `Channel` (SMPP/REST/WebSocket) |
| `lookup_gateway_instance` | `partner_id`, Runtime Redis registry | `GatewayEndpoint` + `session_epoch` |
| `send_deliver_sm` | `GatewayEndpoint`, payload | `RpcResult` (gRPC на Partner SMPP Gateway) |
| `send_rest_callback` | callback URL, payload | `HttpResult` |
| `send_websocket_push` | активное соединение, payload | `WsResult` |
| `handle_delivery_failure` | неуспешный `RpcResult`/`HttpResult`/`WsResult` | `KafkaAck` на `scheduler.background.commands` (`NOTIFICATION_RETRY`) |
| `evaluate_ttl` | `NotificationTask`, `notification_ttl` | `Continue` \| `Expire` (без побочных эффектов, статус уже в lifecycle) |

## 7.2. Partner API

| Метод | Вход | Выход |
|---|---|---|
| `handle_status_query` | HTTP-запрос, `message_id`/`trace_id` | `MessageStatusResponse` (PostgreSQL read) |
| `handle_search_query` | HTTP-запрос, фильтры | `SearchResults` (PostgreSQL read) |
| `handle_report_query` | HTTP-запрос, период/агрегат | `ReportResponse` (ClickHouse read) |

## 7.3. Backoffice API

| Метод | Вход | Выход |
|---|---|---|
| `handle_config_crud` | HTTP-запрос от UI | gRPC-проксирование в Configuration Service |
| `handle_execution_control_override` | HTTP-запрос от UI | gRPC-проксирование в Execution Control Service |
| `handle_force_scheduler_command` | HTTP-запрос от UI | Kafka publish на `scheduler.critical.commands` |
| `handle_dlq_browse` | HTTP-запрос, фильтры | `DlqRecords` (PostgreSQL read) |
| `handle_replay_request` | HTTP-запрос, `stage_execution_id` | gRPC-проксирование в Replay Service |
| `handle_reconciliation_browse` | HTTP-запрос, фильтры | `ReconciliationCases` (PostgreSQL read) |
| `handle_report_query` | HTTP-запрос | `ReportResponse` (ClickHouse read) |

## 7.4. Replay Service

| Метод | Вход | Выход |
|---|---|---|
| `load_dlq_record` | `stage_execution_id` | `DlqRecord` из PostgreSQL |
| `check_ttl` | `DlqRecord`, текущее время | `Valid` \| `Expired` |
| `check_idempotency` | `DlqRecord.stage_execution_id` | подтверждение, что republish безопасен по контракту §6 |
| `check_billing_side_effect` | `DlqRecord` (если стадия — Billing) | `Safe` \| `Unsafe` (по `charge_id` в ledger) |
| `check_delivery_ambiguity` | `DlqRecord` (если стадия — Delivery) | `Safe` \| `Unsafe` (возможен внешний дубликат) |
| `republish` | прошедший все проверки `DlqRecord` | `KafkaAck` на исходный `stage.*` |
| `write_audit` | итог операции | запись в PostgreSQL (replay audit) |

## 7.5. Backoffice UI

Frontend-приложение, собственных методов обработки данных не имеет — все операции проксируются в Backoffice API через OpenAPI-сгенерированный клиент.
