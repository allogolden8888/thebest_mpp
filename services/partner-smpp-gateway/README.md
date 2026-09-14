# Partner SMPP Gateway

## Production hardening 2026-09-14: suspended и terminal archive

Live store теперь различает lifecycle status конфиг-версии и status партнёра внутри payload. Валидный `event.status=active` с `payload.status=suspended|archived` больше не отвергается с сохранением старых credentials: карта bind’ов очищается, version fence продвигается, а уже открытые сессии партнёра закрываются. Переход `active(N) -> archived(N)` разрешён один раз и доминирует над повторным `active(N)`; только `active(N+1)` реактивирует партнёра. Redis bootstrap дополнительно сверяет partner identity и допустимый status.

Проверка: `PartnerConfigStoreEventTest` + `PartnerConfigStoreTest` — 9/9, включая реальный локальный Redis. Полный suite — 102/105; три оставшихся падения относятся к `VaultClientTest`, где локальный Vault отклонил seed с `403 invalid token`.

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `services_specifictaion.md` §2.2: SMPP bind/unbind, `submit_sm`, `deliver_sm`, partner-side `query_sm`, `enquire_link`. **Архитектурное решение LLD, соблюдено буквально**: "Не рекомендуется делать критический Gateway полностью зависимым от старой сторонней SMPP-библиотеки" — внутренний `smpp-codec`/`smpp-pdu-model` реализован с нуля на Netty (`src/main/java/uz/mpp/partnersmpp/codec/`), JSMPP/cloudhopper не используются даже как зависимость.

**Статус:** реально компилируется и тестируется — `mvn test`, **71 тест** (Java 25). Codec протестирован round-trip. **Полный SMPP-сервер протестирован через реальный TCP localhost socket** (не мок) — `PartnerSmppServerIntegrationTest`: настоящий Netty-сервер на эфемерном порту, настоящий `java.net.Socket`-клиент, настоящая сериализация PDU в обе стороны. Redis-путь протестирован против реального локального Redis (brew). Последний прогон: 67/71 прошли, оставшиеся 4 `VaultClientTest` требуют запущенный dev Vault на `127.0.0.1:8200`; отдельный heartbeat/registry/TCP-набор — 24/24.

## P0: периодический heartbeat живых SMPP-сессий (2026-09-11)

`Main` теперь запускает `SessionHeartbeatScheduler`: один fixed-delay worker
раз в `SESSION_HEARTBEAT_INTERVAL_SECONDS` (default 30с) берёт snapshot только
текущих bound-каналов и продлевает Redis TTL (3×interval). Fixed delay не
накапливает очередь при медленном Redis; ошибка одной сессии не останавливает
остальные и не отменяет следующие тики. При shutdown worker гарантированно
останавливается до закрытия Redis client.

Heartbeat и unregister теперь fenced по `gateway_instance_id + session_epoch`
атомарными Lua scripts: запоздавший tick/close старого TCP-сеанса не может
продлить или удалить запись нового bind. Если TTL текущей локальной живой
сессии истёк во время краткого отказа Redis, heartbeat восстанавливает запись;
чужую/newer запись он не перезаписывает. Дополнительно закрыта локальная гонка
out-of-order bind callbacks: меньший epoch не перезаписывает уже
зарегистрированный больший.

В том же production path исправлен Redis URI: `REDIS_RUNTIME_PASSWORD` из
ExternalSecret теперь реально включается и URL-экранируется; остаются
`REDIS_RUNTIME_URL` override и passwordless local-dev режим.

Проверка: `SessionHeartbeatSchedulerTest`, `SessionRedisRegistryTest`,
`RedisUrlTest`, `ChannelRegistryTest`, `PartnerSmppServerIntegrationTest` —
**24/24 passed**, включая periodic stop, изоляцию ошибки, восстановление
утраченной Redis-записи текущего bind, stale heartbeat, stale unregister,
out-of-order register и настоящий Redis/TCP.

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
| `handle_bind` | `server/SmppServerHandler::handleBind` + `server/PartnerAuthenticator` (`VaultAuthenticator` по умолчанию, `StaticAuthenticator` — bootstrap/break-glass, см. "Vault-аутентификация" ниже) | `PartnerSmppServerIntegrationTest` — реальный TCP bind_transceiver, правильный/неправильный пароль, echo sequence_number |
| `register_session` | `server/ChannelRegistry` (in-memory, per-instance) + `registry/SessionRedisRegistry` (Runtime Redis, `smpp:partner_session:{partner_id}:{system_id}`) | `ChannelRegistryTest` (реальные `EmbeddedChannel`, session_epoch меняется при переподключении) + `SessionRedisRegistryTest` (реальный Redis: HSET-поля, TTL, heartbeat не трогает остальные поля) |
| `handle_unbind` | `SmppServerHandler::handleUnbind` | `PartnerSmppServerIntegrationTest` — реальное закрытие TCP-соединения после unbind_resp |
| `validate_submit_pdu` | `core/SubmitValidator` | `SubmitValidatorTest` — пустой/слишком длинный destination_addr, превышение 254 октетов short_message |
| `check_rate_limit` | `core/TokenBucket` (тот же паттерн, что Standard Lane) | `TokenBucketTest` + `PartnerSmppServerIntegrationTest.rateLimitThrottlesExcessSubmits` — реальный throttling через настоящий TCP submit_sm |
| `build_incoming_message` | `core/IncomingMessageBuilder` | `IncomingMessageBuilderTest` — маппинг полей, `segment_count` намеренно не заполняется (считает Pipeline Engine), маппинг `data_coding` |
| `publish_incoming` | `kafkaio/IncomingPublisher` (реальный `kafka-clients` Producer) | `IncomingPublisherTest` — через `MockProducer` (официальный test double kafka-clients, не самодельный мок): топик/ключ/сериализация верны |
| `send_submit_sm_resp` | `SmppServerHandler::handleSubmitSm` | `PartnerSmppServerIntegrationTest` — реальный `submit_sm_resp` с `ESME_ROK`/`ESME_RINVBNDSTS`/`ESME_RTHROTTLED` |
| `handle_deliver_sm_command` / `send_deliver_sm` | `grpcserver/DeliverSmServer` (`platform-contracts/grpc/partner_gateway.proto`, `PartnerDeliverSmService`) | `DeliverSmServerTest` — `DELIVERED`/`NO_ACTIVE_SESSION`/`STALE_EPOCH`, реальная запись PDU-байтов в `EmbeddedChannel` |
| `handle_enquire_link` | `SmppServerHandler` | `PartnerSmppServerIntegrationTest` — реальный `enquire_link_resp` |
| `heartbeat_tick` | `registry/SessionHeartbeatScheduler` + fenced `SessionRedisRegistry::heartbeat` | Периодический worker + реальные Redis-тесты TTL/stale epoch/error isolation/shutdown |
| `handle_partner_query_sm` | **не реализовано** | — читает PostgreSQL read model через Partner API-совместимый lookup — принадлежит периметру Partner API, не реализовано в этом срезе (см. ниже) |

## smpp-codec — что реально протестировано

`codec/PduCodec` — round-trip encode/decode для `bind_transceiver(_resp)`, `submit_sm`/`deliver_sm` (общая структура полей, `ShortMessagePdu`), `submit_sm_resp`/`deliver_sm_resp`, `enquire_link(_resp)`, `unbind(_resp)`, `generic_nack` — `PduCodecTest`, включая проверку, что `command_length` реально совпадает с размером фрейма. **Не полный SMPP 3.4**: `schedule_delivery_time`/`validity_period` читаются/пишутся как пустые C-strings (не представлены отдельными полями), TLV-параметры (optional parameters после обязательных полей) пропускаются при декодировании, не парсятся.

## Vault-аутентификация (luminous-hugging-charm.md Ф1) — закрывает "статическая заглушка"

Раньше `Main.java` строил `StaticAuthenticator` из четырёх env vars — ровно один захардкоженный `(system_id, password, partner_id, application_id)`, никакого реального партнёрского конфига. Закрыто в два слоя, оба реальные, не заглушка поверх заглушки:

* **`config/PartnerConfigLoader` + `PartnerConfigStore` + `kafkaio/ConfigChangeConsumer`** — production больше не зависит от `PARTNER_CONFIG_PATH`. Перед открытием SMPP listener store читает точные immutable PARTNER-версии из Configuration Redis, затем отдельная consumer group каждого pod реплеит `config.changes` до captured EOF и только после этого снимает startup barrier. Live-событие применяется прямо из `payload_json` с per-partner version fence: независимый `config-cache-projector` может ещё не успеть переставить Redis pointer, а archived-версия вообще не становится `current`. Архивация оставляет versioned tombstone, удаляет bind credential и немедленно закрывает уже открытые локальные сессии; старый active replay не может оживить партнёра. Ошибка применения делает `seek` на упавший offset и красит readiness, смерть consumer тоже видна в `/readyz`. Kafka key сверяется с `entity_id`, payload — с `partner_id`, коллизия одного глобального SMPP `system_id` между партнёрами отклоняется без изменения последнего корректного snapshot. **`system_id == application_id`** для `SMPP_BIND` — существующее соглашение `partner.schema.json`. `fromFile` оставлен только для fixture/break-glass тестов. Текущий `partner.valid.json` всё ещё не содержит `SMPP_BIND` приложения; пустая карта остаётся fail-closed.
* **`server/VaultAuthenticator`** — реализация `PartnerAuthenticator`, читает актуальный секрет из Vault по `credential_ref` (тот же KV v2 read-контракт, что `credential-issuer-service` пишет и `partner-rest-receiver`'s `VaultAuthVerifier` читает — три независимые реализации одного протокола, см. `services/credential-issuer-service/README.md` за полным разбором HTTP-шейпов). TTL-кеш 45с (bind — не submit_sm, происходит только на (пере)подключение, но ротация credential должна становиться эффективной без передеплоя за разумное время). **Fail-closed на холодном кеше** (Vault недоступен, ни разу не было успешного read для этого `system_id` → bind отклоняется, не default-open), но **stale-кеш переживает временную недоступность Vault** (TTL истёк, но раньше был успешный read → используется протухшее значение, не рвём уже держащийся бинд из-за транзиентной проблемы инфраструктуры) — оба сценария явно покрыты `VaultAuthenticatorTest`, включая cache-hit-не-зовёт-сеть-повторно и TTL-истёк-зовёт-снова.
* **`vault/VaultClient`** — `TokenSource` пара (`StaticTokenSource` для `VAULT_TOKEN`/local-dev/break-glass, `KubernetesAuthTokenSource` для прод — роль `partner-credential-readers`, `infra/terraform/vault-secrets.tf`, общая с `partner-rest-receiver`), прямое зеркало Go-реализации в `credential-issuer-service`.
* **`AUTH_VERIFIER_MODE=vault`** (дефолт) — новый путь; `AUTH_VERIFIER_MODE=env` — старый `StaticAuthenticator`, явный bootstrap/break-glass fallback, не удалён (та же дисциплина, что план требует для старого статического `ExternalSecret`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `IncomingPublisher` использует настоящий `kafka-clients`, но не проверялся против `kind`+Strimzi.
* **Live PARTNER reload проверен unit/component-тестами, но не Kafka E2E.** Целевой набор: 16 passed, 2 Redis-dependent skipped в sandbox. Нужен тест `configuration-service → config-event-publisher → Kafka → два gateway pod`, включая archive/credential rotation, restart/replay и измерение propagation SLA. Общий `config.changes` всё ещё compacted по одному `entity_id`, а не `(entity_type, entity_id)`; коллизия типов может удалить PARTNER-запись из replay до coordinated key migration.
* **Партнёрская конфигурация — закрыто, см. "Vault-аутентификация" выше.** Раньше здесь была статическая заглушка (`Main.java` — один `system_id`/`password`/`partner_id` из env); `StaticAuthenticator`/env-заглушка остаётся только как явный `AUTH_VERIFIER_MODE=env` bootstrap/break-glass путь, не единственный.
* **`sync_rate_limit_counters`** (периодическая синхронизация локального token bucket в Runtime Redis, ~1с) — не реализовано; `TokenBucket` полностью локален (in-memory per-connection), агрегированный per-partner счётчик через несколько соединений/инстансов не пишется в Redis.
* **`handle_deliver_sm_command`/`send_deliver_sm` — fire-and-forget.** `DeliverSmServer` возвращает `DELIVERED` сразу после успешной записи в канал, не дожидаясь реального `deliver_sm_resp` от партнёра. Корреляция по `sequence_number` с ожидающим gRPC-вызовом (полноценный `smpp-window` для gateway-initiated PDU) не реализована — `SmppServerHandler` принимает `DELIVER_SM_RESP` и молча игнорирует его (см. докстринг в коде).
* **`handle_partner_query_sm`** — не реализовано вообще, требует PostgreSQL read model lookup, совместимый с Partner API (другой сервис Субагента 1, ещё не реализован на момент этого среза).
* **Cross-pod duplicate bind policy не завершена.** Redis fencing защищает registry-запись, но потерявший ownership старый TCP-канал на другом gateway ещё не закрывается автоматически; нужен отдельный lease/takeover protocol и integration-тест двух gateway instances.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.
