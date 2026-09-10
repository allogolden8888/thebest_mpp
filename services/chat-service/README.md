# Chat Service

**Основание:** `BACKOFFICE_DESIGN_SPEC.md` Экран 27 "Chat" — polling-чат между партнёром и бэкофис-персоналом. До `migrations/V034__chat_messages.sql` ничего похожего не существовало нигде в платформе (ни топика, ни таблицы). Мирроит структуру `services/incident-service`/`services/iam-service` — тот же small Go control-plane service паттерн этой сессии.

```bash
brew services start postgresql@17   # если ещё не запущен
cd migrations && for f in V0*.sql; do psql postgresql://localhost:5432/mpp -v ON_ERROR_STOP=1 -f "$f"; done
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/chat-service
go build ./... && go vet ./... && go test ./... -race
```

## Архитектурное решение: отдельный сервис, не общий доступ к таблице

Задача формулировала это явно как развилку: либо `backoffice-api` и `partner-self-service-api` читают/пишут `support.chat_messages` напрямую (каждый из своего кода, без нового сервиса), либо новый маленький `chat-service` с gRPC API.

Проверено перед выбором:
* `partner-self-service-api` сегодня **не имеет ни одного прямого подключения к Postgres** — его `cmd/partner-self-service-api/main.go` package doc явно перечисляет зависимости как gRPC (`ConfigService`, `CredentialIssuerService`) и HTTP (`template-management-service`), не Postgres. Завести первое прямое DB-подключение этого сервиса ради одной таблицы было бы отходом от его собственной, явно задокументированной архитектуры.
* Единственный найденный в кодовой базе случай, когда два app-сервиса читают одну и ту же Postgres-таблицу напрямую (`compliance-api` и `consent-cache-projector`, оба `SELECT ... FROM policy.subscriber_consent`) — это **read-only с одним писателем через событийную материализацию** (config change event → отдельный проектор), не два независимых писателя в реальном времени. Чат — ровно обратный случай: обе стороны (партнёр и админ) пишут в тот же тред, каждая от своего процесса.
* Ни одного прецедента "два app-сервиса напрямую ПИШУТ в одну таблицу" в этой кодовой базе нет.

Вместо изобретения нового паттерна здесь применён уже устоявшийся: маленький выделенный control-plane сервис с gRPC API (тот же класс, что `iam-service`/`incident-service`/`credential-issuer-service`) — `backoffice-api` и `partner-self-service-api` оба становятся тонкими gRPC-клиентами, никто не получает прямого доступа к `support.chat_messages`.

## Модель

`migrations/V034__chat_messages.sql`, схема `support` (новая — ни одна существующая схема концептуально не подходила):

* `support.chat_messages` — `id, partner_id, sender_type (partner/admin), sender_id, body, created_at, read_at (nullable)`. Один тред на `partner_id` — design-референс `MPP Backoffice.dc.html` (экран 27) показывает ровно один список сообщений на партнёра, не многоканальность.
* `read_at` — заполняется НЕ отдельным mark-as-read эндпоинтом (граница объёма задачи: "no read-receipts UI polish"), а как побочный эффект `ListMessages`, вызванного противоположной стороной (см. ниже) — единственный источник для счётчика непрочитанных.
* Индексы: `(partner_id, created_at)` — обслуживает и polling-курсор `ListMessages`, и агрегацию "последнее сообщение" в `ListThreads`; частичный `(partner_id, sender_type) WHERE read_at IS NULL` — обслуживает подсчёт непрочитанных.

## gRPC-контракт

`platform-contracts/grpc/chat.proto` — `ChatService`:

* **`SendMessage(partner_id, sender_type, sender_id, body) -> ChatMessage`** — простая вставка, `read_at` остаётся `NULL`.
* **`ListMessages(partner_id, since?, viewer_type) -> ChatMessage[]`** — сообщения треда с `created_at > since` (или весь тред, если `since` не задан), `ORDER BY created_at ASC, id ASC`. **Побочный эффект в той же транзакции**: все сообщения этого `partner_id` с `sender_type`, противоположным `viewer_type`, и `read_at IS NULL` помечаются `read_at = now()` ДО select. Админ, читающий тред, помечает партнёрские сообщения прочитанными; партнёр, читающий свой тред, помечает админские.
* **`ListThreads() -> ChatThread[]`** — список партнёров с хотя бы одним сообщением + последнее сообщение + `unread_count` (сообщения ОТ ПАРТНЁРА, ещё не прочитанные админом) — питает сайдбар `backoffice-ui`. Не вызывается `partner-self-service-api` — партнёр всегда читает единственный свой тред через `ListMessages`, ему не нужен список "всех партнёров".

Регенерация protobuf:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
cd services/chat-service
rm -rf internal/proto/gen && mkdir -p internal/proto/gen
protoc --proto_path=../../platform-contracts \
  --go_out=internal/proto/gen --go-grpc_out=internal/proto/gen \
  grpc/chat.proto
```

`chat.proto` self-contained (только `google/protobuf/timestamp.proto`) — та же ситуация, что `incident.proto`/`credentials.proto`.

## HTTP-эндпоинты поверх этого сервиса

Этот сервис сам не несёт HTTP/JWT — тонкие прокси в `backoffice-api` (`internal/httpapi/chat.go`, право `chat:write`, видит все партнёры) и `partner-self-service-api` (`internal/httpapi/chat.go`, `/v1/self-service/chat/messages`, `partner_id`/`sender_id` всегда из JWT claims вызывающего, тот же принцип, что `partnerconfig.go`/`templates.go`).

## Что НЕ реализовано на этом шаге (честно, не спрятано)

* Нет full-text поиска по сообщениям чата — `ListMessages` фильтрует только по `partner_id`+`since`.
* Нет отдельного mark-as-read/typing-indicator/file-attachment функционала — явная граница объёма задачи ("no read-receipts UI polish, no typing indicators, no file attachments — plain text messages only").
* `/metrics` — плейсхолдер Prometheus exposition format (валидный 200), без реальных счётчиков (`chat_messages_sent_total` и т.п.).
* Нет rate-limiting на `SendMessage` — ни один другой control-plane сервис этой сессии его тоже не несёт (граница ответственности — API gateway/ingress уровень, вне скоупа этого сервиса).
