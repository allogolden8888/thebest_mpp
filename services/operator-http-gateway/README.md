# Operator HTTP Gateway

**Основание:** `development_plan.md` — Субагент 1, Operator/partner-facing протоколы. `service_internal_methods.md` §1.3a / `services_specifictaion.md` §2.3a: `register_route`, `enforce_tps`, `submit_sm` через HTTP, приём DLR webhook.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, `-race` чисто. HTTP submit-клиент и webhook протестированы через реальные `httptest.Server`/`httptest.NewRecorder` (не мок транспорта). Redis-путь — против реального протокола Redis (`miniredis`).

**BACKOFFICE_ROADMAP.md P0#1 (2026-09) — per-operator webhook credentials.** `WEBHOOK_AUTH_TOKEN` (один общий bearer-токен на ВСЕХ операторов, читаемый из k8s Secret) удалён целиком. Маршрут теперь `/webhook/dlr/{operator_id}` (`net/http` 1.22+ `PathValue`, не изобретённая заново конвенция — то же `{param}` именование, что chi-роуты в `backoffice-api`), и `webhook.OperatorTokenAuthenticator` резолвит per-operator ожидаемый токен через `internal/webhookauth.Lookup`:
  1. `internal/opconfig.Reader` — Configuration Redis (`config:current:operator:{operator_id}` / `config:version:operator:{operator_id}:{version}`, тот же bootstrap/cache-miss паттерн, что уже использует `billing-service::TariffCache`) — читает `http_profile.webhook_auth.credential_ref` из `operator.schema.json`, поле уже существовало (`vault://operators/{operator_id}/webhook_bearer` — конвенция, зафиксированная в `config_schemas/examples/operator.uzmobile_uz.valid.json` ДО этой фазы), не добавлено заново. Схема теперь дополнительно требует `credential_ref`, когда `webhook_auth.type` — `BEARER_TOKEN`/`HMAC_SIGNATURE` (`if`/`then`, тот же приём, что `smpp_profile.query_sm_enabled`).
  2. `internal/vault.Client` — читает фактическое значение секрета из Vault KV v2 (`ParseCredentialRef`/`ReadKV2Property`, byte-for-byte та же логика, что `credential-issuer-service/internal/vault/client.go` и `partner-rest-receiver/src/vault_auth.rs` — read-only здесь, этот сервис никогда не пишет секреты).
  3. `internal/webhookauth.Lookup` — компонует оба шага, TTL-кеш (30с, тот же порядок величины, что `VaultAuthVerifier` в `partner-rest-receiver`) на итоговом значении секрета — на каждый входящий DLR НЕ делается сетевой round-trip к Redis/Vault при cache hit.

  Timing-safety: `OperatorTokenAuthenticator.Authenticate` всегда выполняет `subtle.ConstantTimeCompare` одной и той же формы (реальный ожидаемый токен ИЛИ `dummyOperatorToken`) независимо от того, известен ли `operator_id`, — тот же принцип, что `iam-service::VerifyStaffCredentials`/`dummyHash`: неизвестный/несконфигурированный `operator_id` не должен отвечать заметно быстрее, чем известный с неверным токеном.

  Инфраструктура (`infra/terraform/vault-secrets.tf`, `k8s/generate_manifests.py`, `infra/secrets/generate_external_secrets.py`) обновлена в паре: удалены `variable "operator_webhook_auth_token"`/`vault_kv_secret_v2.operator_webhook`/секрет `operator-webhook-auth`, добавлены `vault_policy.read_operator_webhook_credentials` (`mpp/data/operators/*`, read-only) + `vault_kubernetes_auth_backend_role.operator_webhook_credential_readers` — тот же паттерн, что `partner_credential_readers` для `partner-rest-receiver`/`partner-smpp-gateway`.

  **Честно, не спрятано:** реальных секретных ЗНАЧЕНИЙ для конкретных операторов Vault по-прежнему не содержит — Terraform больше не сеет один общий bulk-секрет (это и была часть проблемы), а `credential-issuer-service::RotateCredential` умеет выпускать только PARTNER credentials, не OPERATOR — per-operator `vault kv put mpp/operators/<id> webhook_bearer=...` остаётся ручной ops-операцией (тот же caveat, что и у JWKS-работы commit `d3987e0`: этот проход чинит МЕХАНИЗМ, не проводит реальное provisioning). Расширение `credential-issuer-service` на операторов — не сделано, отдельная работа, если понадобится self-service ротация.

**CODE_REVIEW.md — что исправлено после первого прохода ревью:**
* **CRITICAL — неаутентифицированный DoS на публичном DLR webhook.** `io.ReadAll(r.Body)` выполнялся без `http.MaxBytesReader` и без `ReadTimeout`/`MaxHeaderBytes` на сервере — любой вызывающий (эндпоинт обязан быть доступен из интернета) мог прислать произвольно большое или медленно льющееся тело/заголовки. `webhook.Handler` теперь оборачивает `r.Body` в `http.MaxBytesReader` (64 КиБ, 413 при превышении); `webhookSrv` в `main.go` теперь задаёт `ReadTimeout`/`WriteTimeout`/`IdleTimeout`/`MaxHeaderBytes`.
* **HIGH — `webhook_auth` сравнивался через `==` (не constant-time) и имел insecure-дефолт `"demo-webhook-token"`.** Закрыто раньше через `StaticTokenAuthenticator`/`subtle.ConstantTimeCompare`+обязательный `WEBHOOK_AUTH_TOKEN`. Полностью заменено на per-operator механизм выше — `StaticTokenAuthenticator` удалён.
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
| `handle_dlr_webhook` | `internal/webhook/webhook.go::Handler` | `webhook_test.go` — реальный `httptest.NewRecorder()`: неаутентифицированный запрос → 401, валидный → 200 + callback, невалидный JSON → 400, токен оператора A на пути оператора B → 401 |
| `normalize_and_publish_dlr` | `internal/kafkaio/builders.go::BuildDlrEvent` + `Publisher.PublishDlr` | `builders_test.go` |

## Открытые вопросы (задокументированы, не скрыты)

1. **Формат HTTP submit/webhook — рабочее предположение**, не из документов LLD. Ни один документ этой сессии не специфицирует конкретный HTTP API конкретного оператора Узбекистана — `internal/httpio` и `internal/webhook` используют простой JSON-формат собственного изобретения (`SubmitRequestPayload`/`SubmitResponsePayload`/`DlrWebhookPayload`). Реальная интеграция потребует per-operator адаптер поверх этих структур.
2. ~~`webhook_auth` — статический bearer-токен, не per-operator конфигурация~~ — закрыто BACKOFFICE_ROADMAP.md P0#1 (см. раздел выше): `internal/webhook.OperatorTokenAuthenticator` + `internal/opconfig`/`internal/webhookauth`/`internal/vault`. Остальной `http_profile` (`tps_limit`/`endpoint_url`/`retry_policy`/`reconnect_policy`) по-прежнему статичен через env vars, не читается из `config.changes` — тот же класс пробела, что user-память "MPP: dynamic config gap" описывает для Rust-сервисов (arc-swap объявлен, но не подключён); здесь не устранено в этом проходе.
3. **Разрыв между "оператор принял" и "Kafka не принял"**: если `PublishSubmitAccepted` возвращает ошибку уже после успешного HTTP submit, `grpcserver.Server.Submit` возвращает `AMBIGUOUS` вызывающей стороне (Delivery), хотя оператор уже реально принял сообщение — этот edge case явно не решён (задокументирован в коде, `grpcserver/server.go`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **ОБНОВЛЕНО 2026-08-06:** `docker build` реально прогнан и провалидирован для этого сервиса (найдены и исправлены реальные баги по пути, где применимо — см. `development_plan.md` "Координация" п.5 и `infra/docker/README.md`). Формулировка ниже — из более раннего состояния сессии, оставлена для истории.
* **`docker build` не выполнялся** — недоступный Docker daemon.
* **Ни разу не запущено против реального Kafka-брокера** — `internal/kafkaio.Publisher` использует настоящий `franz-go`, компилируется, но не проверялся против `kind`+Strimzi.
* **Ни разу не запущено против реального оператора** — только `httptest.Server` в тестах.
* **`/metrics`** — плейсхолдер (валидный 200), без реальных счётчиков.