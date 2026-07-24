# Partner SMPP Gateway

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `services_specifictaion.md` §2.2: SMPP bind/unbind, `submit_sm`, `deliver_sm`, partner-side `query_sm`, `enquire_link`. **Архитектурное решение LLD, соблюдено буквально**: "Не рекомендуется делать критический Gateway полностью зависимым от старой сторонней SMPP-библиотеки" — внутренний `smpp-codec`/`smpp-pdu-model` реализован с нуля на Netty (`src/main/java/uz/mpp/partnersmpp/codec/`), JSMPP/cloudhopper не используются даже как зависимость.

**Статус:** реально компилируется и тестируется — `mvn test` (Java 25). Codec протестирован round-trip. **Полный SMPP-сервер протестирован через реальный TCP localhost socket** (не мок) — `PartnerSmppServerIntegrationTest`: настоящий Netty-сервер на эфемерном порту, настоящий `java.net.Socket`-клиент, настоящая сериализация PDU в обе стороны. Redis-путь протестирован против реального локального Redis (brew).

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

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `IncomingPublisher` использует настоящий `kafka-clients`, но не проверялся против `kind`+Strimzi.
* **Партнёрская конфигурация — статическая заглушка** (`Main.java` — один `system_id`/`password`/`partner_id` из env). Реальный источник — partner config snapshot из `config.changes` (`partner.schema.json`) — не подключён; `StaticAuthenticator` существует именно для этого разрыва, задокументирован в своём докстринге.
* **`sync_rate_limit_counters`** (периодическая синхронизация локального token bucket в Runtime Redis, ~1с) — не реализовано; `TokenBucket` полностью локален (in-memory per-connection), агрегированный per-partner счётчик через несколько соединений/инстансов не пишется в Redis.
* **`handle_deliver_sm_command`/`send_deliver_sm` — fire-and-forget.** `DeliverSmServer` возвращает `DELIVERED` сразу после успешной записи в канал, не дожидаясь реального `deliver_sm_resp` от партнёра. Корреляция по `sequence_number` с ожидающим gRPC-вызовом (полноценный `smpp-window` для gateway-initiated PDU) не реализована — `SmppServerHandler` принимает `DELIVER_SM_RESP` и молча игнорирует его (см. докстринг в коде).
* **`handle_partner_query_sm`** — не реализовано вообще, требует PostgreSQL read model lookup, совместимый с Partner API (другой сервис Субагента 1, ещё не реализован на момент этого среза).
* **Heartbeat tick не запущен периодически** — `SessionRedisRegistry.heartbeat` реализован и протестирован, но `Main.java` не заводит периодический таймер, вызывающий его (TTL сессии в Redis истечёт при живом TCP-соединении без heartbeat — это баг для прода, зафиксированный здесь честно, не скрытый).
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.
