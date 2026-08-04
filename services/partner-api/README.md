# Partner API

**Основание:** `development_plan.md` — Субагент 1. Реализует `service_internal_methods.md` §7.2 целиком: `handle_status_query`, `handle_search_query`, `handle_report_query`. HTTP-only (`external_port=8080`, `k8s/generate_manifests.py`), без внутреннего gRPC-сервера — в отличие от Backoffice API, ни один метод §7.2 не проксирует в другой сервис.

**Статус:** реально компилируется и тестируется — `go build ./... && go test ./...`, не псевдокод. `-race` чисто.

**CODE_REVIEW.md — что исправлено после первого прохода ревью:**
* **HIGH — JWT проверял только подпись, не `aud`/`iss`.** Если тот же Keycloak realm выпускает токены и для других клиентов, токен для чужого клиента, но подписанный тем же ключом, раньше принимался здесь (token-confusion риск). `internal/auth.NewValidator` теперь требует `audience`/`issuer` (`PARTNER_API_JWT_AUDIENCE`/`PARTNER_API_JWT_ISSUER`, ни один LLD не фиксирует конкретные значения, поэтому настраиваются деплоем) и передаёт их в `jwt.WithAudience`/`jwt.WithIssuer`. Заодно (Low finding) добавлен `jwt.WithExpirationRequired()` — токен без `exp` раньше принимался как никогда не истекающий. `jwt_test.go` — новые тесты на оба класса.
* **MEDIUM — без rate limiting.** Партнёр или утёкший токен мог забросать Postgres/ClickHouse неограниченным объёмом фильтрованных запросов. Новый `internal/ratelimit` — простой per-partner in-memory token bucket (20 req/s, burst 40; не distributed — см. package doc), применён как middleware после JWT (лимит по `partner_id`, не по IP).
* **MEDIUM — `/readyz` статический.** `internal/health` теперь поддерживает `SetDependencyChecks`, `main.go` пингует Postgres/ClickHouse.
* **Medium — без HTTP-таймаутов.** `httpSrv` в `main.go` — `ReadTimeout`/`WriteTimeout`/`IdleTimeout` (более прямо эксплуатируемо здесь, чем на внутренних сервисах — этот API внешний).
* **Low — утечка внутренних ошибок внешним партнёрам.** Новый `internalError` (`internal/httpapi/errors.go`) — партнёру только generic-сообщение, полная ошибка в лог сервиса; применено в status/search/report.
* **Low — отрицательный `offset`.** `parseNonNegativeInt` отклоняет его в `search.go`.

```bash
brew services start postgresql@17   # если ещё не запущен
xattr -d com.apple.quarantine /opt/homebrew/bin/clickhouse   # если ещё не снят карантин
clickhouse server -- --path=/tmp/clickhouse-data --listen_host=127.0.0.1 &
cd services/partner-api
go build ./...
go test ./...
```

## Что реализовано по service_internal_methods.md §7.2

| Метод | Где | Как проверено |
|---|---|---|
| `handle_status_query` | `internal/httpapi/status.go` — `GET /v1/messages/status?message_id=`\|`?trace_id=` (+`&history=1`), `internal/store/postgres.go` `StatusByMessageID`/`StatusByTraceID`/`LifecycleHistory` (`messaging.message_read_model` + `messaging.message_lifecycle_history`) | `internal/store/postgres_test.go` (реальный PostgreSQL) + `internal/httpapi/router_test.go` (`TestHandleStatusQueryEndToEnd` — полный HTTP round-trip через chi + JWT + PostgreSQL) |
| `handle_search_query` | `internal/httpapi/search.go` — `GET /v1/messages/search?application_id=&status=&from=&to=&limit=&offset=`, `internal/store/postgres.go` `Search` | `postgres_test.go` (`TestSearchFiltersByPartnerAndStatus`) + `router_test.go` (`TestHandleSearchQueryEndToEnd`) |
| `handle_report_query` | `internal/httpapi/report.go` — `GET /v1/reports?from=&to=&stage_name=`, `internal/store/clickhouse.go` `Report` (агрегат по часу/стадии/исходу из `analytics.stage_events`) | `internal/store/clickhouse_test.go` — **реальный INSERT + SELECT на локальном ClickHouse** |

Каждый маршрут защищён `internal/auth.Validator.Middleware` (JWT RS256, Keycloak-совместимый) — `partner_id` всегда берётся из проверенных claims, никогда из query-параметров: партнёр физически не может запросить чужие сообщения, даже подставив чужой `partner_id` в URL.

`cmd/partner-api/main.go` — сборка: health-сервер на `:9090`, HTTP API на `:8080` (chi), OpenTelemetry span на каждый запрос (`internal/telemetry`).

## Аутентификация — что реально протестировано, что нет

`internal/auth/jwt.go` — реальная проверка подписи RS256 через `golang-jwt/jwt/v5` (не заглушка). `jwt_test.go` — 7 тестов на **реально сгенерированной RSA-паре** (`crypto/rsa.GenerateKey`): валидный токен, отсутствие `Bearer`-префикса, истёкший токен, токен с чужой подписью, отсутствующий `partner_id`, 401 без токена, claims доходят до контекста запроса.

**Не протестировано против реального Keycloak** — в проде публичный ключ реалма приходит через `JWT_PUBLIC_KEY_PEM` (PEM, PKIX/SPKI, тот самый ключ, что публикует Keycloak `/realms/{realm}/protocol/openid-connect/certs`); JWKS-эндпоинт с ротацией ключей (`kid`-based lookup) не реализован — сервис проверяет один статический ключ из переменной окружения. Живого Keycloak в этой сессии не поднималось.

## OpenTelemetry — что реально протестировано, что нет

`internal/telemetry/otel.go` — реальный `go.opentelemetry.io/otel/sdk/trace` `TracerProvider`, span на каждый HTTP-запрос (`http.method`/`http.route`/`http.status_code`). `otel_test.go` — проверка через `tracetest.NewSpanRecorder()` (тот же официальный in-memory recorder из SDK, не мок), что span реально создаётся, завершается и несёт правильные атрибуты.

**Экспорт никуда не настроен** — `cmd/partner-api/main.go` использует `noopExporter` (span создаётся и завершается, но не отправляется). Ни один документ этой сессии не специфицирует OTLP-коллектор/эндпоинт для приёма трейсов — то же ограничение, что у Prometheus/Kafka в остальных сервисах: инфраструктура для приёма телеметрии не развёрнута нигде в этой сессии.

## PostgreSQL/ClickHouse — что реально прогнано

`internal/store/postgres_test.go` — реальные вставка/чтение в `messaging.message_read_model` и `messaging.message_lifecycle_history` (`postgres://localhost:5432/mpp`, переопределяется `PARTNER_API_TEST_DSN`). `internal/store/clickhouse_test.go` — реальный `INSERT`/`SELECT` в `analytics.stage_events` (`127.0.0.1:9000`, переопределяется `PARTNER_API_TEST_CLICKHOUSE_ADDR`); схема переиспользует ту, что спроектировал Analytics Writer (см. `services/analytics-writer/README.md` "Открытый вопрос" — в этой сессии ни один документ не специфицирует ClickHouse-схему). Если Postgres/ClickHouse недоступны, тесты пропускаются (`t.Skip`), не падают.

## Открытый вопрос

* **`services_specifictaion.md` §8.2 "Назначение" перечисляет также "partner-side `query_sm` через SMPP Gateway" и "загрузка доступной конфигурации"**, но `service_internal_methods.md` §7.2 (более детальный источник методов) описывает только три метода — `handle_status_query`/`handle_search_query`/`handle_report_query`. `handle_partner_query_sm` (реальный SMPP `query_sm` от партнёра) по `service_internal_methods.md` §1.2 принадлежит **partner-smpp-gateway** ("`QuerySmResp` из PostgreSQL read model через Partner API-совместимый lookup") — это описывает переиспользование того же `message_read_model`-запроса, что и `handle_status_query` здесь, не gRPC-вызов в Partner API. `partner-smpp-gateway/README.md` честно пометил `handle_partner_query_sm` как нереализованный на момент своего среза, ожидая появления Partner API; теперь, когда `internal/store/postgres.go::StatusByMessageID` существует, лучший путь — либо partner-smpp-gateway напрямую подключится к тому же PostgreSQL read model (та же логика запроса, что здесь), либо получит отдельный внутренний gRPC/HTTP эндпоинт от Partner API — ни один документ не специфицирует какой из двух, поэтому не реализовано ни то ни другое в этом срезе. "Загрузка доступной конфигурации" не имеет соответствующего метода ни в одном разделе `service_internal_methods.md` — не реализовано, находка, а не пробел в этой реализации.

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* **`docker build` не выполнялся** — недоступный Docker daemon.
* **JWKS/ротация ключей** — статический RSA public key из `JWT_PUBLIC_KEY_PEM`, не живой Keycloak.
* **OpenTelemetry-экспорт** — span'ы создаются, никуда не отправляются (`noopExporter`).
* **`partner-side query_sm`/"загрузка доступной конфигурации"** — см. "Открытый вопрос" выше.
* **Пагинация `Search` по `total_count`** — возвращается только страница результатов (`limit`/`offset`), без общего количества строк, соответствующих фильтру (потребовался бы отдельный `COUNT(*)` запрос — не специфицирован в `service_internal_methods.md`).
* **`/metrics`** — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков.
