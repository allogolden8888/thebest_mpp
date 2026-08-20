# Backoffice API — каталог для дизайна

Полный список HTTP API `backoffice-api` (админка), сгруппированный по экранам. Источник истины — `services/backoffice-ui/openapi.yaml` (946 строк, вручную поддерживаемая спецификация). Base URL: `/v1`. Auth: `Authorization: Bearer <JWT>`.

Права указаны там, где они реально гейтят маршрут (`RequirePermission`) — без пометки права значит "любой аутентифицированный токен".

---

## 1. Messages (только что построено — план на переработку)

### `GET /v1/messages` — список, пагинация
Права: `support:trace`

Query: `partner_id?`, `current_status?`, `terminal?` (bool), `limit?` (default 50, max 200), `offset?`

```json
{
  "messages": [
    {
      "message_id": "uuid",
      "partner_id": "click_uz",
      "application_id": "click_uz_main",
      "trace_id": "uuid",
      "pipeline_id": "",
      "pipeline_version": "",
      "current_status": "RECEIVED | SUBMITTED | DELIVERED | FAILED | BLOCKED",
      "terminal": false,
      "created_at": "2026-08-19T11:31:36.622Z",
      "updated_at": "2026-08-19T11:31:36.911Z"
    }
  ]
}
```
⚠️ Нет total count (дорого на неограниченно растущей таблице) — "есть следующая страница" определяется по тому, что текущая страница вернулась полной.

### `GET /v1/messages/{message_id}` — деталь + таймлайн
Права: `support:trace`

```json
{
  "message": { /* тот же shape, что выше */ },
  "history": [
    {
      "lifecycle_version": 1,
      "status": "SUBMITTED",
      "event_id": "uuid",
      "occurred_at": "2026-08-19T11:31:36.911Z",
      "source": "message.lifecycle"
    }
  ]
}
```
⚠️ **Известный баг, чиню отдельно:** `current_status`/`history` сейчас застревают на первом статусе для части сообщений — `message-state-resolver`→`message.lifecycle`→`lifecycle-writer` цепочка не всегда докатывает более поздние статусы. Дизайнить экран стоит с расчётом на то, что таймлайн МОЖЕТ содержать только 1 запись или быть пустым ("нет истории") — это легитимное, не только error-состояние.

### `GET /v1/support/messages/search` — точечный поиск
Права: `support:trace`. Query: `message_id?` ИЛИ `trace_id?` (хотя бы один обязателен). Тот же response shape, что browse.

---

## 2. DLQ / Replay

### `GET /v1/dlq` — список
Query: `stage_name?`, `replay_status?`, `limit?`, `offset?`
```json
{
  "records": [
    {
      "stage_execution_id": "string",
      "message_id": "string",
      "stage_name": "string",
      "attempt": 3,
      "reason_code": "string",
      "error_detail": "string?",
      "created_at": "date-time",
      "replay_status": "pending | replayed | expired"
    }
  ]
}
```

### `POST /v1/replay` — реплей одной записи
Права: `replay:request`. Body: `{ "stage_execution_id": "string" }`
Response: `{ "accepted": true, "rejection_reason": "string?" }`

---

## 3. Reconciliation

### `GET /v1/reconciliation` — список
Query-фильтры аналогичны DLQ (не документированы в spec явно, но handler принимает limit/offset).
```json
{
  "cases": [
    {
      "case_id": "string",
      "message_id": "string",
      "stage_execution_id": "string",
      "operator_id": "string",
      "status": "open | resolved | unresolved",
      "opened_at": "date-time",
      "resolved_at": "date-time?",
      "deadline_at": "date-time"
    }
  ]
}
```

---

## 4. Reports (ClickHouse — **сейчас недоступен локально**)

### `GET /v1/reports`
```json
{
  "rows": [
    { "hour": "date-time", "partner_id": "string", "stage_name": "string", "outcome": "string", "event_count": 123 }
  ]
}
```
⚠️ ClickHouse не поднят в локальном стенде — этот экран сейчас не сможет показать реальные данные. Планировать дизайн можно, проверить вживую — нет, пока не подниму ClickHouse отдельно.

---

## 5. Configuration

### `GET /v1/config/versions` — список версий
Query: `entity_type`, `entity_id`. Response: `{ "versions": [ConfigVersion...], "next_page_token": "string" }`
```json
// ConfigVersion
{ "entity_type": "string", "entity_id": "string", "version": 4, "status": "string", "created_at": "date-time" }
```

### `GET /v1/config/versions/active` — активная версия
Query: `entity_type`, `entity_id`. Response: одиночный `ConfigVersion`.

### `POST /v1/config/versions` — создать версию
Права: `config:write`. Body: `{ "entity_type", "entity_id", "payload_json": <любой JSON> }`

### `POST /v1/config/versions/archive` — архивировать
Права: `config:write`. Body: `{ "entity_type", "entity_id", "version" }`

### `POST /v1/config/versions/validate` — валидация без записи
Body: `{ "entity_type", "payload_json" }`. Response: `{ "valid": bool, "errors": ["string"...] }`

### `GET /v1/config/versions/diff` — сравнение двух версий
Query: `entity_type`, `entity_id`, `from`, `to`.
Response: `{ "from_version": int, "from_payload_json": <json>, "to_version": int, "to_payload_json": <json> }`

---

## 6. Execution Control

### `POST /v1/execution-control/override` — применить override
Права: `execution-control:write`
Body: `{ "scope", "scope_id", "state", "admission_rate": number, "reason", "expires_at"? }`
Response: `{ "version": int, "applied_at": "date-time" }`

### `POST /v1/execution-control/override/clear`
Права: `execution-control:write`. Body: `{ "scope", "scope_id" }`

---

## 7. Scheduler

### `POST /v1/scheduler/force-command`
Права: `scheduler:force`
Body: `{ "stage_execution_id", "task_type": "CRITICAL_COMMAND_TYPE_FORCE_TIMEOUT | CRITICAL_COMMAND_TYPE_FORCE_RETRY", "reason"? }`
Response: `{ "accepted": bool }`

---

## 8. Access Control (IAM) — Users & Roles

### `GET /v1/iam/roles`
Права: `iam:manage`. Response: `{ "roles": [{ "id", "name", "description", "permissions": ["string"...] }] }`

### `GET /v1/iam/staff-assignments`
Права: `iam:manage`. Response: `{ "assignments": [{ "id", "external_id", "role", "granted_by", "granted_at" }] }`

### `POST /v1/iam/staff-assignments` — выдать роль
Права: `iam:manage`. Body: `{ "external_id", "role" }`. Response: `{ "assignment": StaffAssignment }`

### `DELETE /v1/iam/staff-assignments/{external_id}/{role}` — отозвать
Права: `iam:manage`. Response: `{ "revoked": bool }`

### `GET /v1/me` — свой эффективный доступ (без гейта прав)
```json
{ "external_id": "local-dev-admin", "roles": ["backoffice-admin"], "permissions": ["config:write", "credentials:issue", ...] }
```

---

## 9. Credentials (партнёрские, через Vault)

### `POST /v1/partners/{partner_id}/applications/{application_id}/credentials/rotate`
Права: `credentials:issue`
Response: `{ "credential_ref", "secret_version": int, "plaintext_secret": "string (show-once!)", "issued_at" }`
⚠️ `plaintext_secret` присутствует ТОЛЬКО в этом ответе, нигде больше не хранится/не показывается повторно — UI должен трактовать его как одноразовый показ (модалка "скопируйте сейчас").

### `GET /v1/partners/{partner_id}/credentials` — история выпуска (без секретов)
Права: `credentials:issue`
```json
{ "secrets": [{ "id", "partner_id", "application_id", "credential_ref", "secret_version", "status", "issued_at", "issued_by" }] }
```

---

## 10. Audit Log

### `GET /v1/audit`
Права: `audit:read`
```json
{
  "entries": [
    { "source": "replay | execution_control | billing_reconciliation | identity", "actor": "string", "action": "string", "target": "string", "created_at": "date-time" }
  ],
  "next_offset": 123
}
```

---

## 11. Incidents

### `POST /v1/incidents` — открыть
Права: `incident:manage`. Body: `{ "title", "severity": "LOW|MEDIUM|HIGH|CRITICAL" }`

### `GET /v1/incidents` — список
Права: `incident:manage`. Query: `status?` (`OPEN`/`RESOLVED`)

### `GET /v1/incidents/{incident_id}` — деталь
Права: `incident:manage`
```json
{
  "incident": { "id", "title", "severity", "status", "opened_by", "opened_at", "resolved_by?", "resolved_at?", "postmortem_notes?" },
  "timeline": [{ "id", "scope", "scope_id", "state", "admission_rate", "reason", "requested_by", "created_at?", "expires_at?" }],
  "notes": [{ "id", "incident_id", "author", "note", "created_at" }]
}
```

### `POST /v1/incidents/{incident_id}/notes`
Права: `incident:manage`. Body: `{ "note" }`

### `POST /v1/incidents/{incident_id}/resolve`
Права: `incident:manage`. Body: `{ "postmortem_notes" }` (обязателен)

---

## 12. Ops Health

### `GET /v1/ops/snapshot`
Права: `ops:read`
```json
{
  "kafka_lag_available": true,
  "kafka_lag": {
    "generated_at": "date-time",
    "bootstrap_servers": ["kafka:9092"],
    "groups": [
      {
        "group": "billing-service",
        "state": "Stable",
        "total_lag": 0,
        "partitions": [{ "topic", "partition", "commit_offset", "end_offset", "lag", "error"? }],
        "error"?: "string"
      }
    ]
  },
  "readyz_available": true,
  "readyz": {
    "generated_at": "date-time",
    "services": [{ "service": "billing-service", "ready": true, "http_status"?: 200, "latency_ms": 13, "error"?: "string" }]
  }
}
```
Сейчас: 22/23 сервиса реально ready, 1 (`backoffice-api`) честно not ready — ClickHouse недоступен локально (`http_status: 503`), не баг мониторинга.

---

## 13. Patterns / Templates — backend уже есть (`template-management-service`), просто не проксирован через backoffice-api

Реальный, рабочий сервис, сегодня не смонтирован в `backoffice-api` (прямой вызов, не через админку) — контракт ниже железно совпадает с кодом, не черновик.

### `GET /v1/templates` — список/поиск (`policy.policy_template`)
Query: `partner_id?`, `sender_id?`, `category?`, `status?`, `limit?` (default 50, max 500), `offset?`
```json
{
  "templates": [
    {
      "template_id": "uuid",
      "partner_id": "click_uz",
      "operator_id": "beeline_uz | null",
      "sender_id": "CLICK | null",
      "channel": "SMS | EMAIL | PUSH",
      "category": "string (не UNTEMPLATED/BLOCKED — зарезервированы)",
      "pattern": "string",
      "version": 1,
      "status": "active | archived",
      "created_at": "date-time",
      "updated_at": "date-time"
    }
  ],
  "limit": 50,
  "offset": 0
}
```

### `POST /v1/templates/preview` — проверить текст против правил партнёра
Body: `{ "partner_id", "sender_id"?, "text" }`
Response: `{ "matched": bool, "template_id"?: "uuid", "category"?: "string" }` (оба опциональных поля отсутствуют, если `matched: false`)

### `POST /v1/templates/validate-pattern` — линт паттерна без сохранения
Body: `{ "pattern": "string" }` → Response: `{ "warnings": ["string"...] }`

### `GET /v1/templates/export` — CSV всех шаблонов
Response: `text/csv`, колонки как в `ImportRow` ниже.

### `POST /v1/templates/import` — bulk CSV импорт
Body: raw CSV, колонки: `template_id, partner_id, operator_id, sender_id, channel, category, pattern, version, status`
```json
{
  "imported": 42,
  "failed": [{ "row": 3, "error": "string" }],
  "warnings": [{ "row": 5, "template_id": "uuid", "warnings": ["string"...] }]
}
```

---

## 14. Планируется — реального backend нет, ниже честная оценка на чём строить и чего нет вообще

### Alphanames / Short Numbers (= "senders" партнёра)
Данные СЕГОДНЯ живут статично в `partner.valid.json` (`senders[]`, поля `sender_id`/`type: ALPHANAME|SHORT_NUMBER`/`status`), не в БД — редактирование потребует либо (а) нового CRUD-стора + миграции, либо (б) переиспользования `configuration-service`'s generic `entity_type/entity_id/payload_json` версионирования (тот же механизм, что уже гоняет `policy_template`/`policy_ruleset` — см. `GET/POST /v1/config/versions` в разделе 5) под `entity_type="partner_senders"`. Второй путь — меньше нового кода, тот же паттерн, что уже есть.

Реалистичный черновик контракта (не реализовано):
```json
// GET /v1/senders?partner_id=click_uz
{ "senders": [{ "sender_id": "CLICK", "type": "ALPHANAME", "status": "active" }] }
// POST /v1/senders  { "partner_id", "sender_id", "type", "status" }
```

### Partners (CRUD)
Тот же статус, что senders — `partner.valid.json` статичен, партнёров сегодня физически нельзя завести/отредактировать через API. Черновик:
```json
// GET /v1/partners
{ "partners": [{ "partner_id", "status": "active|suspended", "applications": [{ "application_id", "rate_limit_tps", "allowed_channels": ["SMS"], "ip_allowlist": ["cidr"...] }] }] }
```

### MO Messages / Deliver SM (P2A)
**Не черновик — этого нет вообще.** Платформа сегодня архитектурно только A2P (исходящие) — нет ни одного топика/таблицы/proto-сообщения под входящие/mobile-originated трафик где-либо в `platform-contracts/`. Это не "backend не готов", это "фичи как класса не существует в текущей архитектуре платформы" — прежде чем это проектировать, нужно решение на уровне HLD, не просто новый эндпоинт.

### Statistics / аналитика с реальными графиками
Backend (`GET /v1/reports`) уже есть и подключаем — упирается только в то, что ClickHouse не поднят в этом локальном стенде (могу поднять отдельно, если нужно вживую проверять). Дизайнить по контракту из раздела 4 можно уже сейчас.


