# Partner SMPP Gateway

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `services_specifictaion.md` §2.2: SMPP bind/unbind, `submit_sm`, `deliver_sm`, partner-side `query_sm`, `enquire_link`. **Архитектурное решение LLD, соблюдено буквально**: "Не рекомендуется делать критический Gateway полностью зависимым от старой сторонней SMPP-библиотеки" — внутренний `smpp-codec`/`smpp-pdu-model` реализован с нуля на Netty (`src/main/java/uz/mpp/partnersmpp/codec/`), JSMPP/cloudhopper не используются даже как зависимость.

**Статус:** реально компилируется и тестируется — `mvn test`, **38/38** (Java 25). Codec протестирован round-trip. **Полный SMPP-сервер протестирован через реальный TCP localhost socket** (не мок) — `PartnerSmppServerIntegrationTest`: настоящий Netty-сервер на эфемерном порту, настоящий `java.net.Socket`-клиент, настоящая сериализация PDU в обе стороны. Redis-путь протестирован против реального локального Redis (brew).

## Проверено кодревью (CODE_REVIEW.md): все 4 находки по partner-smpp-gateway

1. **CRITICAL — gRPC `PartnerDeliverSmService` без аутентификации, якобы противоречит требованию mTLS.** Расследовано, признано false positive — см. развёрнутый комментарий в `grpcserver/DeliverSmServer.java`: mTLS обеспечен на уровне Istio service mesh (namespace-wide `PeerAuthentication STRICT`, `infra/istio/peer-authentication-strict.yaml`), а `k8s/network_policies.py`'s явный `CALL_GRAPH`-edge `partner-notification-service -> partner-smpp-gateway` ограничивает порт 9000 ТОЛЬКО легитимному вызывающему на сетевом уровне. "Любой сосед по namespace" из находки не подтверждается реальным состоянием репозитория.
2. **HIGH — `submit_sm_resp=ESME_ROK` отправлялся до подтверждения Kafka-публикации.** `kafkaio/IncomingPublisher` получил новый overload `publish(message, callback)` (callback вызывается ПОСЛЕ реального Kafka ack/ошибки, не fire-and-forget); `SmppServerHandler::handleSubmitSm` теперь отвечает партнёру только из этого callback'а — `ESME_ROK` на успех, `ESME_RSYSERR` на ошибку публикации. Callback может сработать на I/O-потоке продюсера, не на Netty event loop — запись в `ChannelHandlerContext` безопасна из любого потока (Netty сам передиспетчеризует на event loop канала).
3. **HIGH — malformed PDU не отвечены, `readCString` без границы.** `codec/SmppStrings::readCString` теперь ограничен `in.readableBytes()` (гарантированно == остаток одного PDU-фрейма) и бросает новый `MalformedPduException` вместо неявного `IndexOutOfBoundsException`; неизвестный `command_id` в `decodeBody` — тот же тип. `SmppServerHandler::channelRead0` оборачивает `decode` в try/catch — malformed PDU теперь отвечается `GENERIC_NACK` с sequence_number, извлечённым напрямую из 16-байтового заголовка (валиден независимо от того, распарсилось ли тело), либо соединение закрывается, если даже заголовок неполон. Новый `exceptionCaught` — соединение закрывается на любое необработанное исключение, не висит молча в Netty default tail.
4. **HIGH — нет idle/read timeout, нет потолка соединений.** Новый `server/ConnectionLimitHandler` (первый в пайплайне, до frame decoder'а — решение принять/отклонить не должно ждать протокольных данных) + `io.netty.handler.timeout.ReadTimeoutHandler` (120с, реагирует новый `exceptionCaught` выше).

**Реальный баг, найденный не по находке кодревью, а при написании regression-теста на находку #3.** `codec/PduCodec::encodeBody` для `BIND_TRANSCEIVER_RESP`/`SUBMIT_SM_RESP`/`DELIVER_SM_RESP` безусловно кастовал `body` и звал `.systemId()`/`.messageId()` — но `SmppServerHandler::respond(..., null)` НАМЕРЕННО передаёт `body=null` на каждый отказ (`ESME_RINVPASWD`, `ESME_RINVBNDSTS`, `ESME_RTHROTTLED`, `ESME_RINVMSGLEN`) — партнёр получал `NullPointerException` при попытке server'а закодировать сам ответ об отказе, т.е. **ни один отклонённый bind/submit никогда не получал ответа вообще**. До добавления `exceptionCaught` (находка #3) это тихо маскировалось — соединение просто зависало без ответа, ничего не падало видимо; после добавления `exceptionCaught` баг стал видимым сразу (`PartnerSmppServerIntegrationTest` — 3 теста, `bindWithWrongPasswordIsRejectedAndConnectionClosed`/`submitSmBeforeBindIsRejected`/`rateLimitThrottlesExcessSubmits` — начали падать на `EOFException` на стороне тестового клиента). Исправлено: `null` body кодируется как пустая C-строка (только NUL-байт), не пропускается — симметрично декодируется той же (неизменённой) веткой `decodeBody`. Регрессия: `PduCodecTest.bindTransceiverRespWithNullBodyEncodesAsEmptySystemIdNotNPE`/`submitSmRespWithNullBodyEncodesAsEmptyMessageIdNotNPE`.

```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@25/25.0.4/libexec/openjdk.jdk/Contents/Home
brew services start redis   # если ещё не запущен, для SessionRedisRegistryTest
cd services/partner-smpp-gateway
mvn test
```

## Что реализовано по service_internal_methods.md §1.2

| Метод | Где | Как проверено |
|---|---|---|
| `handle_bind` | `server/SmppServerHandler::handleBind` + `server/PartnerAuthenticator`/`StaticAuthenticator` | `PartnerSmppServerIntegrationTest` — реальный TCP bind_transceiver, правильный/неправильный пароль, echo sequence_number |
| `register_session` | `server/ChannelRegistry` (in-memory, per-instance) + `registry/SessionRedisRegistry` (Runtime Redis, `smpp:partner_session:{partner_id}:{system_id}`) | `ChannelRegistryTest` (реальные `EmbeddedChannel`, session_epoch меняется при переподключении) + `SessionRedisRegistryTest` (реальный Redis: HSET-поля, TTL, heartbeat не трогает остальные поля) |
| `handle_unbind` | `SmppServerHandler::handleUnbind` | `PartnerSmppServerIntegrationTest` — реальное закрытие TCP-соединения после unbind_resp |
| `validate_submit_pdu` | `core/SubmitValidator` | `SubmitValidatorTest` — пустой/слишком длинный destination_addr, превышение 254 октетов short_message |
| `check_rate_limit` | `core/TokenBucket` (тот же паттерн, что Standard Lane) | `TokenBucketTest` + `PartnerSmppServerIntegrationTest.rateLimitThrottlesExcessSubmits` — реальный throttling через настоящий TCP submit_sm |
| `build_incoming_message` | `core/IncomingMessageBuilder` | `IncomingMessageBuilderTest` — маппинг полей, `segment_count` намеренно не заполняется (считает Pipeline Engine), маппинг `data_coding` |
| `publish_incoming` | `kafkaio/IncomingPublisher` (реальный `kafka-clients` Producer) | `IncomingPublisherTest` — через `MockProducer` (официальный test double kafka-clients, не самодельный мок): топик/ключ/сериализация верны |
| `send_submit_sm_resp` | `SmppServerHandler::handleSubmitSm` | `PartnerSmppServerIntegrationTest` — реальный `submit_sm_resp` с `ESME_ROK`/`ESME_RINVBNDSTS`/`ESME_RTHROTTLED` |
| `handle_deliver_sm_command` / `send_deliver_sm` | `grpcserver/DeliverSmServer` (`platform-contracts/grpc/partner_gateway.proto`, `PartnerDeliverSmService`) | `DeliverSmServerTest` — `DELIVERED`/`NO_ACTIVE_SESSION`/`STALE_EPOCH`, реальная запись PDU-байтов в `EmbeddedChannel` |
| `handle_enquire_link` | `SmppServerHandler` | `PartnerSmppServerIntegrationTest` — реальный `enquire_link_resp` |
| `heartbeat_tick` | `registry/SessionRedisRegistry::heartbeat` | `SessionRedisRegistryTest` — обновляет `heartbeat`, не трогает остальные поля |
| `handle_partner_query_sm` | **не реализовано** | — читает PostgreSQL read model через Partner API-совместимый lookup — принадлежит периметру Partner API, не реализовано в этом срезе (см. ниже) |

## smpp-codec — что реально протестировано

`codec/PduCodec` — round-trip encode/decode для `bind_transceiver(_resp)`, `submit_sm`/`deliver_sm` (общая структура полей, `ShortMessagePdu`), `submit_sm_resp`/`deliver_sm_resp`, `enquire_link(_resp)`, `unbind(_resp)`, `generic_nack` — `PduCodecTest`, включая проверку, что `command_length` реально совпадает с размером фрейма. **Не полный SMPP 3.4**: `schedule_delivery_time`/`validity_period` читаются/пишутся как пустые C-strings (не представлены отдельными полями), TLV-параметры (optional parameters после обязательных полей) пропускаются при декодировании, не парсятся.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `IncomingPublisher` использует настоящий `kafka-clients`, но не проверялся против `kind`+Strimzi.
* **Партнёрская конфигурация — статическая заглушка** (`Main.java` — один `system_id`/`password`/`partner_id` из env). Реальный источник — partner config snapshot из `config.changes` (`partner.schema.json`) — не подключён; `StaticAuthenticator` существует именно для этого разрыва, задокументирован в своём докстринге.
* **`sync_rate_limit_counters`** (периодическая синхронизация локального token bucket в Runtime Redis, ~1с) — не реализовано; `TokenBucket` полностью локален (in-memory per-connection), агрегированный per-partner счётчик через несколько соединений/инстансов не пишется в Redis.
* **`handle_deliver_sm_command`/`send_deliver_sm` — fire-and-forget.** `DeliverSmServer` возвращает `DELIVERED` сразу после успешной записи в канал, не дожидаясь реального `deliver_sm_resp` от партнёра. Корреляция по `sequence_number` с ожидающим gRPC-вызовом (полноценный `smpp-window` для gateway-initiated PDU) не реализована — `SmppServerHandler` принимает `DELIVER_SM_RESP` и молча игнорирует его (см. докстринг в коде).
* **`handle_partner_query_sm`** — не реализовано вообще, требует PostgreSQL read model lookup, совместимый с Partner API (другой сервис Субагента 1, ещё не реализован на момент этого среза).
* **Heartbeat tick не запущен периодически** — `SessionRedisRegistry.heartbeat` реализован и протестирован, но `Main.java` не заводит периодический таймер, вызывающий его (TTL сессии в Redis истечёт при живом TCP-соединении без heartbeat — это баг для прода, зафиксированный здесь честно, не скрытый).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.
