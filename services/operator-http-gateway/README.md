# Operator HTTP Gateway

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `service_internal_methods.md` §1.3a / `services_specifictaion.md` §2.3a: `register_route`, `enforce_tps`, `submit_sm` через HTTP, приём DLR webhook.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, `-race` чисто. HTTP submit-клиент и webhook протестированы через реальные `httptest.Server`/`httptest.NewRecorder` (не мок транспорта). Redis-путь — против реального протокола Redis (`miniredis`).

**CODE_REVIEW.md — что исправлено после первого прохода ревью:**
* **CRITICAL — неаутентифицированный DoS на публичном DLR webhook.** `io.ReadAll(r.Body)` выполнялся без `http.MaxBytesReader` и без `ReadTimeout`/`MaxHeaderBytes` на сервере — любой вызывающий (эндпоинт обязан быть доступен из интернета) мог прислать произвольно большое или медленно льющееся тело/заголовки. `webhook.Handler` теперь оборачивает `r.Body` в `http.MaxBytesReader` (64 КиБ, 413 при превышении); `webhookSrv` в `main.go` теперь задаёт `ReadTimeout`/`WriteTimeout`/`IdleTimeout`/`MaxHeaderBytes`.
* **HIGH — `webhook_auth` сравнивался через `==` (не constant-time) и имел insecure-дефолт `"demo-webhook-token"`.** `StaticTokenAuthenticator.Authenticate` теперь использует `subtle.ConstantTimeCompare`; `WEBHOOK_AUTH_TOKEN` теперь обязателен — сервис не стартует без него (тот же принцип, что `JWT_PUBLIC_KEY_PEM` в backoffice-api/partner-api), вместо тихой работы с угадываемым секретом. Per-operator секрет из `config.changes` по-прежнему не подключён (см. "Открытые вопросы" #2 ниже) — не то же самое, что HIGH-находка про сам механизм сравнения/дефолт, которая закрыта.
* **HIGH — multi-segment submit терял segment_id/smsc_message_id (finding #3) и мог дублировать реальную доставку при ретрае (finding #4).** `grpcserver.Server.Submit` раньше публиковал ОДНО событие после всех сегментов, с захардкоженным `segment_id=0` и перезаписанным `smsc_message_id` — для сообщения из N сегментов данные N-1 сегментов терялись. Переписано: событие публикуется после КАЖДОГО принятого сегмента с его настоящими `segment_id`/`smsc_message_id`; новый `internal/grpcserver/idempotency.go` — in-process карта уже принятых сегментов по `message_id` (сервис instance-addressed — Delivery Service всегда ретраит на тот же под, см. package doc в `server.go`), так что ретрай того же `message_id` после частичного сбоя не отправляет уже принятые сегменты оператору повторно. `server_test.go` — новые тесты на >1 сегмент (раньше не было ни одного).
* **MEDIUM — TPS проверялся один раз на весь `Submit`, не на каждый реальный HTTP-запрос (finding #7).** Сообщение из N сегментов тратило 1 токен, но генерировало N реальных запросов — до Nx превышало настроенный лимит. `TryAcquire` перенесён внутрь цикла, перед каждым реальным HTTP-вызовом (уже принятые ранее сегменты токен не тратят).
* **MEDIUM — `endpoint_url` без SSRF-валидации (часть finding #5).** Новый `internal/httpio/ssrf_guard.go` — тот же приём, что `partner-notification-service/internal/notify/ssrf_guard.go` в main-ветке (`checkScheme` + `Control`-хук на реальном dial, режет private/loopback/link-local/metadata-класс адреса, устойчив к DNS rebinding). `NewClient` (production) использует guard; `NewClientForTests` — без guard, для тестов на `httptest.Server` (127.0.0.1).
* **MEDIUM — `OperatorDlr.segment_id` никогда не заполнялся (finding #6).** `DlrWebhookPayload`/`RawDlr` теперь несут опциональное `segment_id`, `BuildDlrEvent` пробрасывает его в proto.
* **Low — `/readyz` безусловный, неудачная `register_route` только логировалась.** `internal/health` теперь поддерживает `SetDependencyChecks`, `main.go` пингует Redis.

```bash
cd services/operator-http-gateway
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §1.3a

| Метод | Где | Как проверено |
|---|---|---|
| `register_route` / `heartbeat_tick` | `internal/registry/registry.go` — `operator_route:{operator_id}:{route_id}` (Runtime Redis, общий с Operator SMPP Session Manager) | `registry_test.go` — реальные HSET/EXPIRE на `miniredis`, heartbeat не трогает остальные поля |
| `enforce_tps` | `internal/core/TokenBucket` | `tokenbucket_test.go` |
| `handle_submit_command` / `reply_submit_result` | `internal/grpcserver/server.go` (`OperatorSubmitService.Submit`) | `server_test.go` — ACCEPTED/REJECTED/AMBIGUOUS (route not found)/TPS throttled, событие публикуется только при ACCEPTED |
| `send_http_submit` / `handle_http_submit_response` | `internal/httpio/client.go` | `client_test.go` — реальный HTTP round-trip через `httptest.Server`; 5xx → Ambiguous (не Rejected, HLD §13), 4xx → Rejected |
| `publish_submit_accepted` | `internal/kafkaio/publisher.go` + `builders.go` | `builders_test.go` (чистая сборка) — реальная публикация не тестировалась против брокера, см. ниже |
| `handle_dlr_webhook` | `internal/webhook/webhook.go::Handler` | `webhook_test.go` — реальный `httptest.NewRecorder()`: неаутентифицированный запрос → 401, валидный → 200 + callback, невалидный JSON → 400 |
| `normalize_and_publish_dlr` | `internal/kafkaio/builders.go::BuildDlrEvent` + `Publisher.PublishDlr` | `builders_test.go` |

## Открытые вопросы (задокументированы, не скрыты)

1. **Формат HTTP submit/webhook — рабочее предположение**, не из документов LLD. Ни один документ этой сессии не специфицирует конкретный HTTP API конкретного оператора Узбекистана — `internal/httpio` и `internal/webhook` используют простой JSON-формат собственного изобретения (`SubmitRequestPayload`/`SubmitResponsePayload`/`DlrWebhookPayload`). Реальная интеграция потребует per-operator адаптер поверх этих структур.
2. **`webhook_auth` — статический bearer-токен** (`StaticTokenAuthenticator`, теперь обязательный, без insecure-дефолта — см. CODE_REVIEW.md выше), не per-operator конфигурация из `config.changes` (`operator.schema.json`). То же ограничение, что `StaticAuthenticator` в `partner-smpp-gateway`. `config.changes` (tps_limit/endpoint_url/webhook_auth/retry-политика) в целом не потребляется — все per-operator настройки статичны через env vars, не специфицированный здесь механизм local-snapshot.
3. **Разрыв между "оператор принял" и "Kafka не принял"**: если `PublishSubmitAccepted` возвращает ошибку уже после успешного HTTP submit, `grpcserver.Server.Submit` возвращает `AMBIGUOUS` вызывающей стороне (Delivery), хотя оператор уже реально принял сообщение — этот edge case явно не решён (задокументирован в коде, `grpcserver/server.go`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **Ни разу не запущено против реального оператора** — только `httptest.Server` в тестах.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.