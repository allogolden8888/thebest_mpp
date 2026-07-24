# Operator HTTP Gateway

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `service_internal_methods.md` §1.3a / `services_specifictaion.md` §2.3a: `register_route`, `enforce_tps`, `submit_sm` через HTTP, приём DLR webhook.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`. HTTP submit-клиент и webhook протестированы через реальные `httptest.Server`/`httptest.NewRecorder` (не мок транспорта). Redis-путь — против реального протокола Redis (`miniredis`).

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
2. **`webhook_auth` — статический bearer-токен** (`StaticTokenAuthenticator`), не per-operator конфигурация из `config.changes` (`operator.schema.json`). То же ограничение, что `StaticAuthenticator` в `partner-smpp-gateway`.
3. **Разрыв между "оператор принял" и "Kafka не принял"**: если `PublishSubmitAccepted` возвращает ошибку уже после успешного HTTP submit, `grpcserver.Server.Submit` возвращает `AMBIGUOUS` вызывающей стороне (Delivery), хотя оператор уже реально принял сообщение — этот edge case явно не решён (задокументирован в коде, `grpcserver/server.go`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **Ни разу не запущено против реального оператора** — только `httptest.Server` в тестах.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.