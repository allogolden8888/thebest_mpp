# Partner Notification Service

**Основание:** `development_plan.md` Фаза 2.1/2.4, одиннадцатый и **последний сервис "ходового скелета"** (Главный агент) — конец пути, начатого `partner-rest-receiver`: превращает `message.lifecycle` в реальную push-доставку партнёру (`service_internal_methods.md` §7.1). Третий Go-сервис Главного агента, первый с реальным gRPC-клиентом на Go в этой сессии.

**Статус:** `go build ./... && go test ./...`, **21/21 тестов проходят**, 13 из них — реальные round-trip'ы: 4 против настоящего in-process gRPC-сервера (`google.golang.org/grpc`, не мок клиентского интерфейса), 5 против настоящего `httptest.Server`.

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

## Что реализовано по service_internal_methods.md §7.1

| Метод | Где | Примечание |
|---|---|---|
| `on_lifecycle_event` | `kafkaio.HandleRecord` | Один консьюмер на оба входных топика (`message.lifecycle`, `notification.retry`) |
| `resolve_delivery_channel` | `config.ResolveDeliveryChannel` | SMPP (auth.type=SMPP_BIND) либо REST (`notification_callback_url`) |
| `lookup_gateway_instance` | `registry.Store.Lookup` | `smpp:partner_session:{partner_id}:{system_id}` (`system_id = application_id`, задокументированное допущение) — реальный `go-redis` клиент, live не проверено (владелец записи, Partner SMPP Gateway, не реализован ни в одном репозитории) |
| `send_deliver_sm` | `notify.SmppClient.DeliverSm` | Реальный сгенерированный gRPC-клиент против `platform-contracts/grpc/partner_gateway.proto`, **доказано против настоящего in-process gRPC-сервера в тестах** — сервер (Partner SMPP Gateway) сам не реализован, но контракт клиента полностью проверен |
| `send_rest_callback` | `notify.RestClient.SendCallback` | Реальный `net/http`, **доказано против настоящего `httptest.Server`** |
| `send_websocket_push` | — | **Не реализовано в этом срезе** — ни один тестовый партнёр не сигнализирует потребность в WebSocket (нет поля в конфиге, различающего WebSocket от REST callback), см. "Что НЕ реализовано" |
| `handle_delivery_failure` | `kafkaio.HandleRecord` + `schedule.BuildRetryTask` | Публикует `SchedulerBackgroundTask{NOTIFICATION_RETRY}` в `scheduler.background.commands` — **не Kafka-редоставка**: неудачный gRPC/HTTP вызов НЕ считается processing-ошибкой (offset коммитится), единственный retry-механизм — через Scheduler (`services_specifictaion.md` §8.1: "второго, самостоятельного механизма retry внутри сервиса нет") |
| `evaluate_ttl` | `schedule.EvaluateTTL` | Без побочных эффектов на `Expire` — `message.lifecycle` уже терминален, партнёр может получить статус через Partner API pull (HLD §19) |

## Тесты — что доказано

21 тест:
* `internal/config` (7) — загрузка реального `partner.valid.json` (включая новое поле), `resolve_delivery_channel` для всех веток (SMPP_BIND без URL, API_KEY с URL, API_KEY без URL — ошибка).
* `internal/notify` (9, **реально против живых серверов**) — gRPC: `DELIVERED`/`STALE_EPOCH`/`NO_ACTIVE_SESSION` реакции, переиспользование соединения между вызовами на один endpoint (регрессия на "не открывать новый канал на каждый вызов"); HTTP: 2xx/5xx/429/4xx/сетевая недоступность — каждый со своим `Outcome`, включая тонкое различие **429 retryable, но остальные 4xx — permanent failure** (партнёрский endpoint отверг запрос по причине, которую повтор не исправит).
* `internal/schedule` (4) — TTL-граница (исключительная, `now == deadline` → `Expire`), `attempt` — passthrough, не инкремент (регрессия на находку про `DispatchBuilder`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **WebSocket-канал не реализован** — `service_internal_methods.md` §7.1 упоминает `send_websocket_push`, но ни `partner.schema.json`, ни какой-либо другой документ не специфицирует, как партнёр сигнализирует желание получать уведомления через WebSocket вместо REST callback (нет отдельного значения в `auth.type`, нет булева флага) — не додумано заново, оставлено как открытый вопрос, аналогичный найденным в этой сессии для Sub-агента 1.
* **`registry.Store`/`msgctx.Store`/`pending.Store` — реальные Redis-клиенты, ни разу не запущены против живого Runtime Redis** — тот же класс оговорки, что у остальных Redis-клиентов этой сессии.
* **Ни разу не запущено против реального Kafka-брокера или реального Partner SMPP Gateway** — gRPC-клиент полностью проверен в тестах против фейкового, но настоящего gRPC-сервера; реальный Partner SMPP Gateway не реализован ни в одном репозитории на момент написания.
* **`docker build` не выполнялся** — недоступен Docker daemon.
* **`FirstApplication`, не полноценный per-application resolve** — см. находку выше про отсутствие `application_id` в `msgctx`.
