# Partner Notification Service

**Основание:** `development_plan.md` Фаза 2.1/2.4, одиннадцатый и **последний сервис "ходового скелета"** (Главный агент) — конец пути, начатого `partner-rest-receiver`: превращает `message.lifecycle` в реальную push-доставку партнёру (`service_internal_methods.md` §7.1). Третий Go-сервис Главного агента, первый с реальным gRPC-клиентом на Go в этой сессии.

**Статус (2026-09-14):** `go build ./... && go test ./...`, **50/50 top-level тестов проходят**. В набор входят реальные local round-trip'ы через miniredis, in-process gRPC и `httptest.Server`, а также version-fence/captured-EOF/readiness тесты live-config пути.

```bash
cd services/partner-notification-service
go build ./...
go test ./...
```

## Реальная находка: `partner.schema.json` не имел поля для REST-уведомлений

`resolve_delivery_channel` (service_internal_methods.md §7.1) должен выбрать канал доставки статуса партнёру — но `partner.schema.json` до этого сервиса специфицировал только то, КАК партнёр аутентифицируется на приёме сообщений (`auth.type`), никогда — куда слать статус обратно. Добавлено поле `notification_callback_url` (опциональное, обязательное только для `auth.type != SMPP_BIND` — партнёры с SMPP-биндом получают уведомление через `deliver_sm` на тот же бинд, им URL не нужен) — коммит перед этим сервисом, `config_schemas/validate_all.py` по-прежнему 20/20.

## Реальная находка: `NotificationRetryTask` не несёт `message_id` — тот же класс пробела, что уже закрыт для `dlr-manager`

`scheduler_events.proto`'s `NotificationRetryTask` несёт только `lifecycle_event_id`, не `message_id` — и Sub-агент 1 уже явно задокументировал это как открытый вопрос в `scheduler-background-lane/topology/DispatchBuilder.java`: *"SchedulerBackgroundTask не несёт message_id... Partner Notification Service должен уметь резолвить message_id из lifecycle_event_id самостоятельно"*. Этот сервис — тот самый Partner Notification Service, и решение здесь то же, что уже применено в `dlr-manager` для аналогичного пробела с `operator.dlr.unresolved`: `internal/pending.Store` кэширует исходный `MessageLifecycleEvent` (несущий `message_id`) в Runtime Redis под его собственным `event_id` при первой неудачной попытке доставки; когда позже приходит `NotificationRetryTask` с тем же `lifecycle_event_id`, событие достаётся обратно из кэша.

## Реальная находка: `msgctx` не несёт `application_id`, только `partner_id`

`message.lifecycle` не несёт ни `partner_id`, ни `application_id` вообще — единственный источник `partner_id` для уже попавшего в pipeline сообщения — `msgctx:{message_id}` в Runtime Redis (`data_infrastructure_spec.md` §284), но там нет `application_id`. `resolve_delivery_channel` поэтому в этом срезе работает на уровне партнёра (`Snapshot.FirstApplication`), беря первое сконфигурированное приложение — корректно для тестового партнёра (одинаковый `auth.type`/канал у всех его `application`), **не генерализовано** для партнёра с разнородными каналами по разным приложениям. Правильный фикс — добавить `application_id` в `msgctx` (Pipeline Engine) либо в `MessageLifecycleEvent` (Message State Resolver) — оба уже закоммичены, не тронуты здесь, задокументировано как известное ограничение.

## Проверено кодревью (2026-07-27, PART 2): SSRF на REST callback — исправлено; gRPC insecure credentials — намеренно, не находка

Кодревью нашло два HIGH в этом сервисе.

**SSRF на `notification_callback_url` — реальная находка, исправлено.** `RestClient` раньше отправлял POST на URL из конфигурации партнёра без единой проверки (schema требует только `format: uri`). `internal/notify/ssrf_guard.go`: production-конструктор (`NewRestClient`) теперь (1) отвергает любой scheme кроме `https` до попытки соединения, и (2) использует `net.Dialer.Control`-хук, который проверяет РЕЗОЛВЛЕННЫЙ IP непосредственно перед каждым TCP dial'ом (включая редиректы) и отвергает loopback/RFC1918/link-local (покрывает cloud metadata endpoint 169.254.169.254)/multicast/unspecified — устойчиво к DNS rebinding между валидацией URL и реальным подключением, не только строковая проверка hostname. Обе ошибки — `OutcomePermanentFailure`, не `OutcomeRetryable` (повтор запрещённого адреса никогда не поможет). Тесты используют отдельный `newRestClientWithoutSSRFGuard` для существующих `httptest.Server`-сценариев (обычный `httptest.NewServer` — сам по себе loopback+HTTP, production guard корректно бы его отверг) плюс новые тесты на сам guard, включая `httptest.NewTLSServer` (валидный https-scheme, но loopback IP — доказывает, что dial-level проверка ловит то, что одна только проверка scheme пропустила бы).

**gRPC `insecure.NewCredentials()` в `grpc_client.go` — расследовано, это НЕ находка.** Кодревью процитировало `hld.md`/`service_io_contracts.md` про обязательный mTLS для instance-addressed RPC к Partner SMPP Gateway — верно как требование, но mTLS здесь реально обеспечен, просто не в коде приложения: весь namespace `mpp` помечен `istio-injection: enabled` (`k8s/generate_manifests.py`), и `infra/istio/peer-authentication-strict.yaml` включает `PeerAuthentication` в режиме `STRICT` + `DestinationRule` с `ISTIO_MUTUAL` для `*.mpp.svc.cluster.local` — Istio mesh отклоняет любое plaintext-соединение между подами namespace на уровне Envoy sidecar, независимо от того, что делает код приложения. Это стандартный service-mesh-mTLS паттерн (app → localhost sidecar plaintext → sidecar↔sidecar mTLS → sidecar → destination app plaintext), не пропущенное требование. Добавление TLS-конфигурации в самом gRPC-клиенте поверх этого было бы избыточным double-mTLS без единого документированного источника certs/CA на уровне приложения — что означало бы придумывать инфраструктуру заново вместо использования уже работающей. Пояснение с точными путями файлов оставлено прямо в коде (`grpc_client.go`), чтобы не всплывало как ложная находка повторно.

## Закрыто позже (второй раунд кодревью): SmppClient.conns unbounded growth + retry storm

**`SmppClient.conns` рос неограниченно — исправлено.** LOW/MEDIUM находка кодревью: инстансы Partner SMPP Gateway адресуются по pod/instance endpoint через registry, который меняется на каждый рестарт/rescale — долгоживущий процесс копил бы устаревшие, простаивающие `grpc.ClientConn` вместе с их keepalive-горутинами бесконечно. `internal/notify/grpc_client.go`: каждая запись теперь несёт `lastUsed`, фоновый тикер (раз в минуту) закрывает и убирает соединения, простаивающие дольше `idleTTL` (по умолчанию 10 минут). `TestIdleConnectionsAreEvicted` — регрессия.

**Retry storm — фиксированный 30с интервал без backoff/jitter — исправлено.** MEDIUM находка кодревью: `cmd/partner-notification-service/main.go` раньше планировал каждую следующую попытку через ровно `now + 30s`, независимо от `attempt` — до ~2880 попыток за 24ч TTL, амплифицируя нагрузку на уже деградировавший партнёрский endpoint вместо отступления. `internal/schedule/schedule.go::NextRetryDelay` — full jitter exponential backoff (AWS Architecture Blog): `[0, min(maxBackoff, base*2^(attempt-1)))`, база 30с, потолок 5 минут (настраивается через `Deps.RetryBackoffBase`/`RetryBackoffMax`). `TestNextRetryDelayGrowsWithAttemptAndStaysCapped`/`TestNextRetryDelayNeverNegativeForAnomalousAttempt` — регрессия.

**Не закрыто в этом раунде (см. `development_plan.md`, следующий этап работы):** HMAC/подпись на исходящих REST callback'ах — требует нового поля в `partner.schema.json` (per-partner shared secret), кросс-сервисное изменение схемы конфигурации (config_schemas — общая зона с Субагентом 1), не точечный фикс одного сервиса; `FirstApplication` misdelivery — требует добавления `application_id` в `msgctx`/`MessageLifecycleEvent`, что означает изменения в уже закоммиченных Pipeline Engine и Message State Resolver; удвоенная доставка при сбое `PendingStore.Delete` после успешного retry — известный, самодокументированный race, не решён.

## Production hardening 2026-09-14: live PARTNER config без рестарта

Прежняя реализация была небезопасна для HA: `config.changes` потреблялся той же consumer group, что и lifecycle-события, поэтому обновлялась только одна из реплик. Consumer перечитывал Redis до/после независимого projector, archive видел старый `config:current`, а новый pod мог стать Ready до replay. Dockerfile и K8s дополнительно задавали `PARTNER_CONFIG_PATH`, фактически выключая hot reload в production.

Теперь каждый процесс имеет отдельный group-less full-mirror consumer с earliest replay и точно захваченной границей EOF по всем партициям. До этой границы delivery consumer не обрабатывает трафик. Live update берёт immutable `payload_json` из Kafka, проверяет key/entity/payload identity и версию, а atomic Store держит version fence и archive tombstone. `active(N) -> archived(N)` разрешён один раз, но повторный `active(N)` уже не оживит архив. Partner payload status `suspended` корректно отличается от lifecycle status конфиг-версии и fail-closed блокирует доставку. Периодический Kafka Ping снимает `/readyz` при потере broker; main loop также перестаёт поллить delivery queue, потому что readiness сама по себе Kafka workload не останавливает. При transient processing error consumer явно перематывает in-memory position на упавший offset, а переподключение config mirror делает replay заново поверх последнего валидного snapshot.

Production deployment больше не монтирует статический fixture и получает `redis-configuration-credentials`; Docker Compose подключён к Configuration Redis. Файл остался только как явный local fallback: `PARTNER_CONFIG_MODE=file` + `PARTNER_CONFIG_PATH`.

Upstream terminal path теперь закрыт: `configuration-service.ArchiveVersion` в одной транзакции меняет status той же версии и пишет `config_outbox`; Config Cache Projector удаляет Redis current-pointer через same-version lifecycle fence. До coordinated rollout остаётся compaction-key коллизия: ключ топика равен только `entity_id`, а не `(entity_type, entity_id)`, а также живой двухрепличный Kafka E2E.

## Что реализовано по service_internal_methods.md §7.1

| Метод | Где | Примечание |
|---|---|---|
| `on_lifecycle_event` | `kafkaio.HandleRecord` | Один консьюмер на оба входных топика (`message.lifecycle`, `notification.retry`) |
| `resolve_delivery_channel` | `config.ResolveDeliveryChannel` | SMPP (auth.type=SMPP_BIND) либо REST (`notification_callback_url`) |
| `lookup_gateway_instance` | `registry.Store.Lookup` | `smpp:partner_session:{partner_id}:{system_id}` (`system_id = application_id`, задокументированное допущение) — реальный `go-redis` клиент; записью владеет реализованный Partner SMPP Gateway |
| `send_deliver_sm` | `notify.SmppClient.DeliverSm` | Реальный сгенерированный gRPC-клиент против `platform-contracts/grpc/partner_gateway.proto`, доказанный против in-process gRPC-сервера; production server реализован в `services/partner-smpp-gateway` |
| `send_rest_callback` | `notify.RestClient.SendCallback` | Реальный `net/http`, **доказано против настоящего `httptest.Server`** |
| `send_websocket_push` | — | **Не реализовано в этом срезе** — ни один тестовый партнёр не сигнализирует потребность в WebSocket (нет поля в конфиге, различающего WebSocket от REST callback), см. "Что НЕ реализовано" |
| `handle_delivery_failure` | `kafkaio.HandleRecord` + `schedule.BuildRetryTask` | Публикует `SchedulerBackgroundTask{NOTIFICATION_RETRY}` в `scheduler.background.commands` — **не Kafka-редоставка**: неудачный gRPC/HTTP вызов НЕ считается processing-ошибкой (offset коммитится), единственный retry-механизм — через Scheduler (`services_specifictaion.md` §8.1: "второго, самостоятельного механизма retry внутри сервиса нет") |
| `evaluate_ttl` | `schedule.EvaluateTTL` | Без побочных эффектов на `Expire` — `message.lifecycle` уже терминален, партнёр может получить статус через Partner API pull (HLD §19) |

## Тесты — что доказано

50 top-level тестов: `internal/config` (20), `internal/health` (3), `internal/kafkaio` (7), `internal/notify` (14), `internal/schedule` (6). Новый config-набор проверяет Redis bootstrap, suspended/archive, immutable payload, identity/version mismatch, stale replay, same-version tombstone, concurrent CAS и captured EOF на нескольких партициях. HTTP/gRPC/miniredis тесты остаются реальными local round-trip, а не моками клиентских интерфейсов.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **WebSocket-канал не реализован** — `service_internal_methods.md` §7.1 упоминает `send_websocket_push`, но ни `partner.schema.json`, ни какой-либо другой документ не специфицирует, как партнёр сигнализирует желание получать уведомления через WebSocket вместо REST callback (нет отдельного значения в `auth.type`, нет булева флага) — не додумано заново, оставлено как открытый вопрос, аналогичный найденным в этой сессии для Sub-агента 1.
* **`registry.Store`/`msgctx.Store`/`pending.Store` — реальные Redis-клиенты, ни разу не запущены против живого Runtime Redis** — тот же класс оговорки, что у остальных Redis-клиентов этой сессии.
* **Новый full-mirror ещё не прогнан против production-like Kafka с двумя репликами** — unit/component контракты закрыты, но нужен живой E2E на broadcast, compaction gaps, restart с пустым Redis и broker loss/recovery.
* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступен Docker daemon.
* **`FirstApplication`, не полноценный per-application resolve** — см. находку выше про отсутствие `application_id` в `msgctx`.
