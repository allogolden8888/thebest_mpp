# Operator SMPP Session Manager

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `services_specifictaion.md` §2.3: SMPP binds с операторами, reconnect, `enquire_link`, SMPP window, TPS/throttling, `submit_sm`, DLR, опциональный `query_sm`, приоритет `submit_sm` над `query_sm`. Тот же стек и то же архитектурное решение, что `partner-smpp-gateway` — внутренний `smpp-codec` на Netty, не JSMPP/cloudhopper (`codec/` — портирован из `services/partner-smpp-gateway/src/main/java/uz/mpp/partnersmpp/codec/`, идентичный протокол, разные пакеты).

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). **Полный клиентский цикл протестирован через реальный TCP round-trip** против `FakeSmscServer` (тестовый "фейковый оператор" — настоящий Netty-сервер на localhost, не мок): bind, submit_sm, приём DLR push от оператора. Redis-путь — против реального локального Redis (brew).

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
brew services start redis   # если ещё не запущен
cd services/operator-smpp-session-manager
mvn test
```

## Отличие от Partner SMPP Gateway: клиент, не сервер

Partner SMPP Gateway — SMPP-сервер (принимает binds от партнёров). Этот сервис — SMPP-**клиент** (ESME), сам подключается к оператору и делает bind. Поэтому `client/OperatorSmppClient` — Netty client bootstrap, не server; корреляция ответов (`bind_transceiver_resp`/`submit_sm_resp`) идёт по `sequence_number` через `Map<Integer, CompletableFuture<Pdu>>` в `OperatorSmppClientHandler` (простой "smpp-window") — в отличие от Partner-сервера, где ответы синхронны в рамках одного PDU-обработчика.

## Что реализовано по service_internal_methods.md §1.3

| Метод | Где | Как проверено |
|---|---|---|
| `bind_operator` | `client/OperatorSmppClient::bind` | `OperatorSmppClientTest.bindAndSubmitSmRoundTrip` — реальный TCP bind против `FakeSmscServer` |
| `register_route` | `registry/OperatorRouteRegistry` (Runtime Redis, `operator_route:{operator_id}:{route_id}`, общий с Operator HTTP Gateway) | `OperatorRouteRegistryTest` — реальный Redis: HSET-поля, unregister |
| `enforce_window_and_tps` | `core/TokenBucket` (тот же паттерн, что Standard Lane/Partner Gateway) | `OperatorSubmitServerTest.submitRejectedWhenTpsThrottled` — реальный сквозной путь через gRPC-хендлер |
| `handle_submit_command` / `send_submit_sm` / `handle_submit_sm_resp` / `reply_submit_result` | `grpcserver/OperatorSubmitServer` (`OperatorSubmitService.Submit`) | `OperatorSubmitServerTest` — реальный TCP submit_sm через `OperatorSmppClient` + `FakeSmscServer`, ACCEPTED/REJECTED/timeout→AMBIGUOUS |
| `publish_submit_accepted` | `kafkaio/OperatorEventPublisher::publishSubmitAccepted` | `OperatorEventPublisherTest` (`MockProducer`) + `OperatorSubmitServerTest` (реально публикуется только при ACCEPTED, не при throttled) |
| `handle_raw_dlr` / `publish_operator_dlr` | `client/OperatorSmppClientHandler` (получение `deliver_sm` от оператора) + `Main.java` (сборка `OperatorDlr`) + `kafkaio/OperatorEventPublisher::publishDlr` | `OperatorSmppClientTest.receivesDlrPushedByOperator` — реальный push DLR от `FakeSmscServer`, доставлен в `dlrSink`; `OperatorEventPublisherTest` — публикация |
| `enforce_query_sm_priority` | `core/PriorityGate` | `PriorityGateTest` — PERMIT/DEFER по количеству одновременных submit |
| `handle_query_sm_command` | `grpcserver/OperatorQuerySmServer` | Частично — см. "Что НЕ реализовано" (сам PDU `query_sm` не в объёме codec) |
| `enquire_link_tick` | `client/OperatorSmppClient::sendEnquireLink`, вызывается по таймеру в `Main.java` | `OperatorSmppClientTest` косвенно (сервер отвечает на `enquire_link`) |
| `reconnect` | `Main.java::connectAndBindWithRetry` — простой retry с линейным backoff | Не тестировано (нужен реальный обрыв TCP-соединения, не покрыто юнит-тестом) |

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального оператора** — только `FakeSmscServer` (тестовый TCP-сервер в этом репо, не настоящий SMSC).
* **`query_sm` PDU не реализован** в `codec/CommandId` (см. `services/partner-smpp-gateway/README.md` — то же ограничение унаследовано: только bind/submit/deliver/enquire_link/unbind/generic_nack). `OperatorQuerySmServer` реально проверяет приоритет (`enforce_query_sm_priority`), но не отправляет фактический query_sm оператору.
* **`reconnect_policy` — фиксированный линейный backoff** (`min(30s, attempt*1s)`), не из партнёрской/операторской конфигурации (`config.changes`) — реальный источник параметров реконнекта не подключён.
* **DLR `raw_status`** берётся как весь текст `short_message` deliver_sm без парсинга полей `id:`/`sub:`/`dlvrd:`/`stat:` — нормализация в `normalized_status` явно вне scope этого сервиса (принадлежит DLR Manager, `platform_contracts.md` §4, владеет Главный агент).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.