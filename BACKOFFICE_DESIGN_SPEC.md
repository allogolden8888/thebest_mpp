# MPP Backoffice — спецификация для дизайна

Документ для дизайнера фронтенда. Каждый раздел = экран. Под каждым экраном: поля, состояния, фильтры, действия и API за ними.

**Легенда статуса API:**
- ✅ **есть** — эндпоинт задеплоен и работает, можно дёргать прямо сейчас
- 🔨 **строю** — данные в БД есть, эндпоинта пока нет (оценка в днях)
- ❌ **нет данных** — требует архитектурного решения, не просто эндпоинта

Base URL: `/v1` · Auth: `Authorization: Bearer <JWT>` · Все таймстампы: ISO 8601 UTC (`2026-08-19T11:31:36.622Z`).

---

# ЧАСТЬ 1. СЛОВАРИ (нужны на всех экранах)

## 1.1. Жизненный цикл сообщения (`MessageLifecycleStatus`)

Это статус сообщения **целиком**, то, что видит партнёр. Терминальные помечены 🔴.

| Значение | Что означает | Цвет (предложение) |
|---|---|---|
| `SUBMITTED` | Принято платформой, ушло оператору | синий |
| `DELIVERED` 🔴 | Оператор подтвердил доставку абоненту | зелёный |
| `UNDELIVERABLE` 🔴 | Оператор вернул окончательный отказ | красный |
| `DELIVERY_UNRESOLVED` 🔴 | DLR не пришёл в SLA оператора — судьба неизвестна | оранжевый |
| `LATE_DELIVERY_CONFIRMED` 🔴 | DLR пришёл ПОСЛЕ истечения SLA (уже после `DELIVERY_UNRESOLVED`) | жёлто-зелёный |
| `REJECTED` 🔴 | Отклонено платформой (policy/banword/blacklist/невалидный sender) | красный |
| `FAILED` 🔴 | Внутренняя ошибка платформы | тёмно-красный |
| `SYSTEM_UNAVAILABLE` 🔴 | TTL сообщения истёк, пока платформа была на паузе | серый |

⚠️ Для дизайна: **`RECEIVED`** тоже встречается в данных (начальное состояние, до первого перехода). Это 9-е значение де-факто.

## 1.2. Стадии пайплайна (`StageName`) — по ним считается «где сколько провело»

| # | Стадия | Что делает |
|---|---|---|
| 1 | `DESTINATION_RESOLUTION` | Определяет оператора по номеру (MNP/диапазоны) |
| 2 | `POLICY` | Шаблоны, банворды, антиспам, blacklist, время суток, валидация sender |
| 3 | `BILLING` | Списание по тарифу |
| 4 | `ROUTING` | Выбор маршрута/failover |
| 5 | `DELIVERY` | Реальный SMPP submit оператору |
| 6 | `DELIVERY_RECONCILIATION` | Разбор неподтверждённых (когда DLR не пришёл) |

## 1.3. Исход стадии (`Outcome`) — статус на каждом шаге

`SUCCEEDED` · `REJECTED` · `FAILED` · `TIMED_OUT` · `RETRY_EXHAUSTED` · `SUBMISSION_OUTCOME_UNKNOWN` · `DELIVERY_UNRESOLVED`

## 1.4. Категории сообщений (влияют на тариф)

`SERVICE` · `TRANSACTION` · `ADVERTISING` · `UNTEMPLATED` (не совпало ни с одним шаблоном) · `BLOCKED` (отклонено policy — тарифицируется отдельно)

---

# ЧАСТЬ 2. ЭКРАНЫ

---

## Экран 1. Dashboard (главная)

**Статус: 🔨 собирается из 4 существующих API, отдельный агрегирующий эндпоинт не нужен**

### Тайлы верхнего ряда (KPI за выбранный период)
| Тайл | Значение | Источник |
|---|---|---|
| Всего сообщений | число + дельта к пред. периоду | `GET /v1/reports` (ClickHouse) |
| Доставлено | число + % от всего | `GET /v1/reports` |
| Отклонено | число + % | `GET /v1/reports` |
| Ошибки/недоставлено | число + % | `GET /v1/reports` |
| Списано (сумма) | сумма в UZS | 🔨 `GET /v1/billing/summary` |
| Активных партнёров | число | 🔨 из ledger/messages |

### Блок «Здоровье платформы»
Светофор по 23 сервисам + Kafka lag. Источник: ✅ `GET /v1/ops/snapshot`.
Состояния: все зелёные / N жёлтых (lag > порога) / M красных (not ready).

### Блок «Графики»
- Сообщения по часам (stacked: delivered/rejected/failed) — `GET /v1/reports`
- Топ-10 партнёров по объёму — `GET /v1/reports`
- Распределение по категориям (донат) — `GET /v1/reports`
- Топ-10 alpha-имён — `GET /v1/reports` ⚠️ *см. примечание про sender_id ниже*

### Блок «Требует внимания»
- Открытые инциденты (счётчик + список 3 последних) — ✅ `GET /v1/incidents?status=OPEN`
- DLQ pending (счётчик) — ✅ `GET /v1/dlq?replay_status=pending`
- Reconciliation open (счётчик) — ✅ `GET /v1/reconciliation`

✅ ClickHouse поднят — графики имеют источник. Состояние «нет данных» всё равно закладывайте: свежий стенд стартует с пустой аналитикой.

⚠️ **`sender_id` (alpha-name) в аналитике сегодня НЕТ** — `analytics.stage_events` хранит `message_id, partner_id, stage_name, outcome, reason_code, lifecycle_status, occurred_at`. Топ alpha-имён требует добавления `sender_id` в схему ClickHouse (🔨 ~1 день: поле в analytics-writer + миграция таблицы). Заложите тайл в дизайн, я добавлю поле.

---

## Экран 2. Сообщения — список

**Статус: ✅ есть** — `GET /v1/messages`

### Фильтры
`partner_id` · `current_status` (селект из словаря 1.1) · `terminal` (да/нет) · период (📌 фильтра по дате в API сейчас НЕТ — 🔨 добавлю, полдня) · `sender_id`/`msisdn` (📌 см. ограничение ниже)

### Колонки таблицы
| Колонка | Поле | Примечание |
|---|---|---|
| Message ID | `message_id` | UUID, обрезать с тултипом, копирование в клик |
| Партнёр | `partner_id` | |
| Приложение | `application_id` | |
| Статус | `current_status` | бейдж по словарю 1.1 |
| Терминальный | `terminal` | галочка/прочерк |
| Создано | `created_at` | |
| Обновлено | `updated_at` | |

### Пагинация
`limit` (по умолч. 50, макс 200) + `offset`. ⚠️ **Total count НЕ возвращается** (дорого на растущей таблице) — дизайнить «Пред/След», не «страница 3 из 47».

### Пустые состояния
- Нет сообщений вообще
- Нет по текущим фильтрам
- Источник недоступен (ошибка API)

⚠️ **Поиск по msisdn/номеру абонента невозможен** и это архитектурное ограничение, не баг: номер нигде не индексируется reverse (лежит только в `msgctx:{message_id}` в Redis по ключу message_id). Поиск — только по `message_id` или `trace_id`: ✅ `GET /v1/support/messages/search?message_id=…` или `?trace_id=…`. Если поиск по номеру нужен — это отдельная задача (индекс/проекция), скажите.

---

## Экран 3. Сообщение — детальная карточка ⭐

**Это ключевой экран (ваши скрины). Разбит на 5 блоков.**

### Блок A. Шапка — атрибуты сообщения
**Статус: ✅ есть** — `GET /v1/messages/{message_id}` → `.message`

`message_id` · `partner_id` · `application_id` · `trace_id` · `pipeline_id` · `pipeline_version` · `current_status` (бейдж) · `terminal` · `created_at` · `updated_at`

🔨 **Добавлю в этот же ответ (~1 день, данные есть в `msgctx` Redis + stage-событиях):**
`sender_id` (alpha-name) · `msisdn` (получатель) · `category` · `segment_count` · `operator_id` · `route_id` · `sandbox` (флаг тестовой отправки) · `priority_flag`

### Блок B. Хронология стадий — «где и сколько провело» ⭐
**Статус: ✅ РАБОТАЕТ** — `GET /v1/messages/{message_id}/timeline`. ClickHouse и analytics-writer подняты, пер-стадийные события пишутся по-настоящему.
Реальный ответ (не пример — снят с живого сообщения):
```json
{
  "message_id": "d55f6a96-b4a5-479a-80f4-522b8569188a",
  "total_duration_ms": 385,
  "stages": [
    { "stage_name": "DESTINATION_RESOLUTION", "outcome": "SUCCEEDED", "reason_code": "",
      "occurred_at": "2026-08-21T06:13:32.896Z", "duration_ms": null },
    { "stage_name": "POLICY",   "outcome": "SUCCEEDED", "reason_code": "",
      "occurred_at": "2026-08-21T06:13:32.921Z", "duration_ms": 25 },
    { "stage_name": "BILLING",  "outcome": "SUCCEEDED", "reason_code": "",
      "occurred_at": "2026-08-21T06:13:33.086Z", "duration_ms": 165 },
    { "stage_name": "ROUTING",  "outcome": "SUCCEEDED", "reason_code": "",
      "occurred_at": "2026-08-21T06:13:33.115Z", "duration_ms": 29 },
    { "stage_name": "DELIVERY", "outcome": "SUCCEEDED", "reason_code": "",
      "occurred_at": "2026-08-21T06:13:33.281Z", "duration_ms": 166 }
  ]
}
```
📌 Точные поля, на которые можно верстать: `stage_name`, `outcome`, `reason_code`, `occurred_at`, `duration_ms`, `total_duration_ms`.
📌 `duration_ms` у **первой** стадии всегда `null` (а не 0) — момент старта пайплайна лежит в другом источнике, и 0 означал бы «стадия прошла мгновенно». Рисуйте первый шаг без длительности.
📌 Полей `started_at`/`attempt` в ответе **нет**.
**Дизайн:** горизонтальный степпер (как на вашем скрине «Part #1») с длительностью под каждым шагом; узкое место подсвечивать (самая долгая стадия). Учтите состояния: стадия провалена (красный шаг, дальше обрыв), retry (тот же шаг с `attempt: 2`), сообщение ещё в полёте (последние шаги серые).

### Блок C. SMPP/операторские действия ⭐
**Статус: ✅ РАБОТАЕТ** — `GET /v1/messages/{message_id}/operator-events`. Реальный ответ ниже (поля `dlr` пока нет — см. ограничение в конце блока).
Реальный ответ (снят с живого сообщения):
```json
{
  "message_id": "62c42054-8035-427d-b2c9-4332fb887983",
  "segments": [
    {
      "segment_id": 1,
      "operator_id": "beeline_uz",
      "smsc_message_id": "dktovgt75e8g",
      "stage_execution_id": "aeff6f54-b2b8-40f2-be78-9bed5cf10517",
      "submitted_at": "2026-08-20T10:12:09.727Z",
      "dlr_expires_at": "2026-08-20T14:12:09.727Z"
    }
  ]
}
```
📌 Вложенного объекта `dlr` (время прихода, сырой текст, latency) в ответе **нет**. Факт и время доставки берутся из ленты статусов (Блок D): переход в `DELIVERED`. Сырой текст receipt'а не сохраняется нигде — в proto-контракте `OperatorDlr` нет поля под него, только нормализованный код статуса. Чтобы показать «delivery snippet» как на вашем скрине, нужно добавить поле `raw_receipt` в контракт (отдельная правка с регенерацией protobuf).
**Дизайн:** таблица/аккордеон по сегментам (многосегментные SMS = несколько строк). Показать сырой DLR-текст моноширинным (как на вашем скрине). Состояния: DLR ещё не пришёл (ждём, показать дедлайн `correlation_expires_at`), DLR просрочен, DLR пришёл поздно.

⚠️ **Чего в этом блоке НЕ будет** (данных не существует нигде в платформе): пер-PDU таймстампы вида `submit_sm_to_smsc_at` / `submit_sm_resp_from_smsc_at` / `deliver_sm_to_client_at` — как на вашем 2-м скрине. Есть только: момент submit, момент прихода DLR, сырой текст DLR. Чтобы получить пер-PDU детализацию, нужна новая инструментация в `operator-smpp-session-manager` (публиковать таймстамп на каждый PDU) — это отдельная задача, ~2-3 дня, скажите если нужно.

### Блок D. Лента статусов
**Статус: ✅ есть** — `GET /v1/messages/{message_id}` → `.history`
```json
{ "lifecycle_version": 1, "status": "SUBMITTED", "event_id": "uuid",
  "occurred_at": "…", "source": "message.lifecycle" }
```
**Дизайн:** вертикальный таймлайн. ⚠️ Легитимно бывает **пустым** или из **одной записи** — не рисовать это как ошибку.

### Блок E. Биллинг по сообщению
**Статус: 🔨 строю (~0.5 дня). Данные ЕСТЬ** в `billing.billing_ledger` (связь по `charge_id` = `stage_execution_id` стадии BILLING).
```json
{ "charge_id": "uuid", "amount": 94.0000, "currency": "UZS",
  "entry_type": "charge", "category": "SERVICE", "segments": 1,
  "created_at": "…", "compensated_by": null }
```
**Дизайн:** компактная строка/карточка. Показать компенсацию, если была (`entry_type: compensating`).

---

## Экран 4. Тестовая отправка (Sandbox) ⭐

**Статус: ✅ механизм есть и проверен по коду** — флаг `sandbox: true` в теле `POST /v1/messages` (partner-rest-receiver, порт 8080).

Что реально происходит с sandbox-сообщением:
- ✅ Проходит **все реальные проверки**: resolution (определение оператора), policy (шаблоны, банворды, антиспам, blacklist, валидация sender), routing — результаты настоящие, не подделанные
- 💰 Стадия BILLING проходит, **но списания не происходит вообще** — код не делает *ни одного* обращения к биллинговому Redis (не «списали и вернули», а списания не существует ни на миг). Тариф при этом резолвится по-настоящему, так что **видно, сколько бы стоило**
- 📡 Стадия DELIVERY **не уходит оператору** — реальные SMPP-шлюзы структурно недостижимы для sandbox-трафика; генерируется синтетический DLR, `smsc_message_id` вида `SANDBOX-…`

🔨 **Нужен прокси в backoffice-api** (~0.5 дня), чтобы админ мог отправлять тест из админки, не имея партнёрского ключа: `POST /v1/test/send`

Запрос:
```json
{
  "partner_id": "click_uz",
  "application_id": "click_uz_main",
  "msisdn": "998901234567",
  "sender_id": "CLICK",
  "body": "Тестовое сообщение",
  "priority": 2,
  "sandbox": true
}
```
Ответ: `{ "message_id": "uuid", "trace_id": "uuid" }`

**Дизайн:** форма отправки + переключатель «Sandbox / Реальная отправка» (с явным предупреждением при реальной — уйдёт живому абоненту и **спишутся деньги**). После отправки — прямая ссылка на детальную карточку (Экран 3), чтобы сразу смотреть прохождение по стадиям. Sandbox-сообщения должны быть визуально помечены везде в списках (бейдж «TEST»).

---

## Экран 5. Биллинг

### 5.1. Лента списаний (ledger)
**Статус: ✅ РАБОТАЕТ** — `billing.billing_ledger` теперь наполняется реальными списаниями (transactional outbox достроен).

`GET /v1/billing/ledger?partner_id=&entry_type=&from=&to=&limit=&offset=`
```json
{
  "entries": [
    { "id": 12345, "charge_id": "uuid", "account_id": "click_uz",
      "partner_id": "click_uz", "amount": 94.0000, "currency": "UZS",
      "entry_type": "charge | compensating",
      "source_charge_id": null, "created_at": "…" }
  ]
}
```
**Колонки:** дата · партнёр · charge_id · тип (списание/компенсация) · сумма · валюта.
**Дизайн:** компенсации визуально связать с исходным списанием (`source_charge_id`), отрицательные суммы — другим цветом.

### 5.2. Сводка по партнёрам
**Статус: ✅ РАБОТАЕТ.**

`GET /v1/billing/summary?from=&to=&group_by=partner|category|day`
```json
{
  "currency": "UZS",
  "total": 458200.0000,
  "rows": [
    { "key": "click_uz", "charges": 460000.0000, "compensations": -1800.0000,
      "net": 458200.0000, "message_count": 4870 }
  ]
}
```
**Дизайн:** таблица + график по дням. Нужны: сумма списаний, сумма компенсаций, нетто, кол-во сообщений, средняя цена сообщения.

### 5.3. Тарифы
**Статус: ✅ API есть уже сейчас** через generic config-версии: `GET/POST /v1/config/versions?entity_type=billing_tariff&entity_id={partner_id}`.

Структура тарифа (`payload_json`):
```json
{
  "currency": "UZS",
  "price_per_segment": {
    "SERVICE": 94, "TRANSACTION": 94, "ADVERTISING": 350,
    "UNTEMPLATED": 3500, "BLOCKED": 94
  },
  "default_category": "UNTEMPLATED",
  "recurring_charges": {
    "alphaname_monthly_fee": 4000000,
    "service_sms_package": { "segments": 40000, "price": 2000000 }
  }
}
```
**Дизайн:** форма редактирования цен по категориям + периодические платежи. Обязательно: **версионирование** (тариф — версионируемая сущность: список версий, diff, активная версия, кнопка «архивировать»). Все 4 операции уже есть в API (см. Экран 9).

⚠️ Тариф сегодня **per-partner**. Тариф per-operator потребует изменения схемы + логики резолва в billing-service (~2-3 дня), скажите если нужен.

### 5.4. Сверка (reconciliation)
**Статус: ✅ есть** — `GET /v1/reconciliation`. Кейсы расхождений: `case_id` · `message_id` · `operator_id` · `status` (open/resolved/unresolved) · `opened_at` · `deadline_at`.

---

## Экран 6. MT / MO сообщения

### MT (Mobile Terminated — исходящие, A2P)
**Статус: ✅ это всё, что описано в Экранах 2-3.** Вся платформа сегодня — MT.

### MO (Mobile Originated — входящие от абонента) и P2A
**Статус: ❌ НЕ СУЩЕСТВУЕТ.** Честно: в платформе нет ни одного топика, таблицы или proto-сообщения для входящего трафика. Это не «бэкенд не готов» — это отсутствующая архитектурная возможность.

Чтобы это появилось, нужно (оценка ~1-2 недели):
1. Приём `deliver_sm` от оператора как MO (сейчас `deliver_sm` обрабатывается **только** как DLR)
2. Новый топик `incoming.mo` + схема хранения
3. Маршрутизация MO партнёру (webhook/SMPP-сессия)
4. Экраны

**Для дизайна:** можно заложить структуру экрана MO по аналогии с MT (список + карточка: от кого, кому/на какой короткий номер, текст, оператор, время, статус доставки партнёру), но подтвердите приоритет — это самостоятельный кусок работы, а не эндпоинт.

---

## Экран 7. Шаблоны и паттерны

**Статус: ✅ backend полностью готов** (`template-management-service`), 🔨 нужен прокси через backoffice-api (~0.5 дня).

| Действие | API | Статус |
|---|---|---|
| Список/поиск | `GET /v1/templates?partner_id=&sender_id=&category=&status=&limit=&offset=` | ✅ |
| Preview (проверить текст) | `POST /v1/templates/preview` `{partner_id, sender_id?, text}` → `{matched, template_id?, category?}` | ✅ |
| Валидация паттерна | `POST /v1/templates/validate-pattern` `{pattern}` → `{warnings: []}` | ✅ |
| Экспорт CSV | `GET /v1/templates/export` | ✅ |
| Импорт CSV | `POST /v1/templates/import` → `{imported, failed: [{row, error}], warnings: [{row, template_id, warnings}]}` | ✅ |

Поля шаблона: `template_id` · `partner_id` · `operator_id?` · `sender_id?` · `channel` (SMS/EMAIL/PUSH) · `category` · `pattern` · `version` · `status` (active/archived) · `created_at` · `updated_at`

**Дизайн:** список с фильтрами + редактор паттерна с live-валидацией (`validate-pattern` даёт предупреждения о слишком широких паттернах) + песочница preview (ввести текст → увидеть, какой шаблон сматчился и какая категория/цена). Импорт: drag-n-drop CSV + отчёт об ошибках построчно.

---

## Экран 8. Модерация шаблонов (партнёр → админ)

**Статус: ❌ не существует, самая крупная новая фича (~3-4 дня).**

Что нужно построить:
1. Новый статус `pending_review` в `policy.policy_template` (сейчас только active/archived) — миграция
2. `POST /v1/self-service/templates/propose` (партнёрская сторона) → создаёт заявку
3. `GET /v1/templates?status=pending_review` (✅ уже работает — фильтр есть)
4. `POST /v1/templates/{id}/approve` и `POST /v1/templates/{id}/reject` `{reason}` — 🔨 новые

**Дизайн:** очередь модерации (список заявок с diff «что предлагается»), карточка заявки с preview-проверкой паттерна перед одобрением, кнопки Одобрить/Отклонить с причиной. На партнёрской стороне — «Мои заявки» со статусами.

---

## Экран 9. Конфигурация (версионируемые сущности)

**Статус: ✅ полностью есть.** Один универсальный механизм для всех конфигов.

| Действие | API |
|---|---|
| Список версий | `GET /v1/config/versions?entity_type=&entity_id=` |
| Активная версия | `GET /v1/config/versions/active?entity_type=&entity_id=` |
| Создать версию | `POST /v1/config/versions` `{entity_type, entity_id, payload_json}` |
| Архивировать | `POST /v1/config/versions/archive` `{entity_type, entity_id, version}` |
| Проверить без записи | `POST /v1/config/versions/validate` `{entity_type, payload_json}` → `{valid, errors[]}` |
| Diff двух версий | `GET /v1/config/versions/diff?entity_type=&entity_id=&from=&to=` |

**Через этот же механизм редактируются (никакого нового API не нужно, только UI):**
- `billing_tariff` — тарифы (Экран 5.3)
- `policy_ruleset` — **банворды/антиспам/время суток/разрешённые sender_id** (Экран 10)
- `route_table` — маршруты операторов (Экран 11)
- `partner` — партнёры и приложения (Экран 12)
- `policy_template` — шаблоны
- `subscriber_consent` — согласия/блэклист (Экран 13)

**Дизайн:** один переиспользуемый паттерн «редактор конфига»: селектор сущности → список версий → просмотр/diff → редактирование (форма или JSON) → валидация → публикация. Это самый ценный компонент для переиспользования.

---

## Экран 10. Спам / Банворды / Антиспам

**Статус: ✅ API есть** (через `policy_ruleset`, Экран 9), 🔨 нужен только UI.

Структура `policy_ruleset.payload_json`:
```json
{
  "partner_id": "click_uz",
  "operator_id": null,
  "unmatched_template_behavior": "CATEGORIZE_AS_UNTEMPLATED | REJECT",
  "anti_spam": { "max_messages": 3, "window_seconds": 60,
                 "scope": "PER_MSISDN_PER_CATEGORY" },
  "time_of_day": [
    { "category": "ADVERTISING", "allowed_from": "09:00",
      "allowed_to": "20:00", "timezone": "Asia/Tashkent" }
  ],
  "banwords": {
    "words": ["idiot", "мудак", "терроризм"],
    "normalization": { "nfkc": true, "homoglyph_fold": true,
                       "strip_separators": true }
  },
  "sender_validation": { "allowed_sender_ids": ["Click", "ClickUP"] }
}
```
**Дизайн:** 4 секции — банворды (тег-инпут + переключатели нормализации), антиспам (лимит/окно/скоуп), время суток (по категориям), разрешённые sender_id. Всё версионируется через Экран 9 (показать «активная версия N», историю, diff).

---

## Экран 11. Операторы и маршруты

### 11.1. Живое состояние SMPP-сессий
**Статус: ✅ есть** (`backoffice-ui` экран "Operator Routes", право `ops:read`). Runtime Redis (`operator_route:{operator_id}:{route_id}`), SCAN обязателен (нет источника "список всех operator_id").

`GET /v1/operators/routes`
```json
{
  "routes": [
    { "operator_id": "beeline_uz", "route_id": "beeline_smpp_primary",
      "protocol": "SMPP", "endpoint": "operator-smpp-session-manager:9000",
      "owning_instance_id": "operator-smpp-session-manager",
      "heartbeat": "2026-08-21T08:33:36.243Z", "route_epoch": 82482405302984,
      "ttl_seconds": 81 }
  ]
}
```
`ttl_seconds` вместо отдельного `healthy` — "мёртвая" сессия просто исчезает из ответа, когда TTL истекает (ключ пропадает из Redis), отдельного статус-поля нет.
**Дизайн:** таблица операторов, кто держит сессию сейчас, когда последний heartbeat, сколько секунд до истечения TTL.

### 11.2. Конфигурация маршрутов
**Статус: ✅ через `route_table` (Экран 9).** Primary/reserve эндпоинты, failover, лимиты.

### 11.3. Метрики туннеля оператора
**Статус: ✅ есть, но не проксировано** — `/metrics` на operator-smpp-session-manager (порт 9099) отдаёт: `dispatched_total{tier}`, `rejected_total{tier,reason}`, `queue_depth{tier}`, `achieved_tps{tier}`, `pending_response_count` (реальная глубина SMPP-окна). 🔨 прокси ~0.5 дня.
**Дизайн:** график TPS по приоритетам (high/medium/low), глубина очереди, отказы.

### 11.4. Подключение НОВОГО оператора
**Статус: ❌ нет self-service.** Сегодня — ручное добавление сервиса в деплой + конфиг маршрута. Полноценный «добавить оператора из UI» = provisioning-слой, которого нет (~1 неделя+).

---

## Экран 12. Партнёры

**Статус: ⚠️ смешанный.** Данные партнёров сегодня в статичном `partner.valid.json`, но версионируемый механизм (Экран 9, `entity_type=partner`) позволяет их редактировать без нового кода — 🔨 требуется проверка, что config-cache-projector реально раздаёт эти изменения в рантайм (~1 день на проверку + допил).

Структура партнёра:
```json
{
  "partner_id": "click_uz",
  "status": "active",
  "senders": [
    { "sender_id": "CLICK", "type": "ALPHANAME", "status": "active" },
    { "sender_id": "5252", "type": "SHORT_NUMBER", "status": "active" }
  ],
  "applications": [
    { "application_id": "click_uz_main",
      "display_name": "Click main billing notifications",
      "auth": { "type": "API_KEY", "credential_ref": "vault://…" },
      "ip_allowlist": ["185.65.212.0/24"],
      "rate_limit_tps": 2000,
      "allowed_channels": ["SMS"],
      "notification_callback_url": "https://…" }
  ]
}
```
**Дизайн:** партнёр → приложения (вкладки/аккордеон) → sender'ы (alpha-имена и короткие номера), лимиты TPS, IP-allowlist, webhook. Alpha-имена и короткие номера — это `senders[].type`, отдельных сущностей в платформе нет.

---

## Экран 13. Blacklist / Согласия абонентов

**Статус: ✅ есть**, проксировано через backoffice-api (`backoffice-ui` экран "Blacklist"). Реальный `compliance-api` (порт 8083) остаётся source of truth, `backoffice-api` форвардит `Authorization` и не хранит копию состояния.

| Действие | API |
|---|---|
| Проверить номер | `GET /v1/compliance/consent?msisdn=998901234567` → `{"msisdn": "…", "blocked_categories": ["ADVERTISING"], "blocked_senders": ["SPAMMER"]}` (без gate прав) |
| Добавить запись вручную | `POST /v1/compliance/consent` (право `compliance:write`) |

**Дизайн:** поиск по номеру → карточка с двумя списками (заблокированные категории, заблокированные отправители) + форма добавления (видна только с `compliance:write`).
⚠️ **Обратный поиск («покажи все заблокированные номера») невозможен** — данные лежат в Redis по ключу msisdn, реверс-индекса нет. Дизайнить как «проверка номера», не как «список блэклиста».

---

## Экран 14. DLQ и повторы

**Статус: ✅ есть.**
- `GET /v1/dlq?stage_name=&replay_status=&limit=&offset=` → `{records: [{stage_execution_id, message_id, stage_name, attempt, reason_code, error_detail?, created_at, replay_status: "pending|replayed|expired"}]}`
- `POST /v1/replay` `{stage_execution_id}` → `{accepted, rejection_reason?}` (право `replay:request`)
- `POST /v1/scheduler/force-command` `{stage_execution_id, task_type: "FORCE_TIMEOUT|FORCE_RETRY", reason?}` (право `scheduler:force`)

---

## Экран 15. Управление трафиком (Execution Control)

**Статус: ✅ есть.** Позволяет **притормозить или остановить** платформу целиком или точечно.
- `POST /v1/execution-control/override` `{scope, scope_id, state, admission_rate, reason, expires_at?}`
- `POST /v1/execution-control/override/clear` `{scope, scope_id}`

**Дизайн:** это «рубильник» — нужны явные подтверждения, показ текущих активных override'ов, обязательная причина, срок действия. `admission_rate` (0.0–1.0) = throttling, не только вкл/выкл.

---

## Экран 16. Пользователи и роли (IAM)

**Статус: ✅ полностью есть.**
- `GET /v1/iam/roles` → роли с правами
- `GET /v1/iam/staff-assignments` → назначения
- `POST /v1/iam/staff-assignments` `{external_id, role}`
- `DELETE /v1/iam/staff-assignments/{external_id}/{role}`
- `GET /v1/me` → свои роли и права (без ограничения по правам)

**Роли:** `backoffice-admin` (все права) · `support-agent` (audit:read + support:trace) · `compliance-officer` (compliance:write) · `incident-manager` (incident:manage) · `ops-viewer` (ops:read)

**Права:** `config:write` · `credentials:issue` · `dlq:manage` · `execution-control:write` · `iam:manage` · `replay:request` · `incident:manage` · `ops:read` · `scheduler:force` · `support:trace` · `audit:read` · `compliance:write`

**Дизайн:** матрица «роль × право», список сотрудников с ролями. Каждый экран должен уметь скрываться/дизейблиться по правам — при дизайне закладывайте состояние «нет доступа».

---

## Экран 17. Ключи и секреты партнёров

**Статус: ✅ есть.**
- `POST /v1/partners/{partner_id}/applications/{application_id}/credentials/rotate` → `{credential_ref, secret_version, plaintext_secret, issued_at}`
- `GET /v1/partners/{partner_id}/credentials` → история (без самих секретов)

⚠️ **`plaintext_secret` возвращается ТОЛЬКО в ответе на ротацию, один раз, больше нигде.** Дизайн обязан: модалка «скопируйте сейчас, больше не покажем», кнопка копирования, подтверждение перед закрытием.

---

## Экран 18. Аудит

**Статус: ✅ есть** — `GET /v1/audit` (право `audit:read`).
`{entries: [{source: "replay|execution_control|billing_reconciliation|identity", actor, action, target, created_at}], next_offset}`

**Дизайн:** фильтр по источнику/актору/периоду, бесконечная лента (пагинация через `next_offset`).

---

## Экран 19. Инциденты

**Статус: ✅ полностью есть** (право `incident:manage`).
- `POST /v1/incidents` `{title, severity: LOW|MEDIUM|HIGH|CRITICAL}`
- `GET /v1/incidents?status=OPEN|RESOLVED`
- `GET /v1/incidents/{id}` → `{incident, timeline: [execution-control действия], notes: []}`
- `POST /v1/incidents/{id}/notes` `{note}`
- `POST /v1/incidents/{id}/resolve` `{postmortem_notes}` — постмортем **обязателен**

**Дизайн:** список инцидентов, карточка с таймлайном (какие override'ы применялись) + заметки + закрытие с постмортемом.

---

## Экран 20. Здоровье платформы (Ops)

**Статус: ✅ есть** — `GET /v1/ops/snapshot` (право `ops:read`).
```json
{
  "readyz_available": true,
  "readyz": { "generated_at": "…", "services": [
    { "service": "billing-service", "ready": true, "http_status": 200, "latency_ms": 13, "error": null }
  ]},
  "kafka_lag_available": true,
  "kafka_lag": { "generated_at": "…", "bootstrap_servers": ["kafka:9092"], "groups": [
    { "group": "billing-service", "state": "Stable", "total_lag": 0,
      "partitions": [{ "topic": "stage.billing", "partition": 0,
                       "commit_offset": 94780, "end_offset": 94780, "lag": 0 }] }
  ]}
}
```
**Дизайн:** сетка сервисов (сейчас 23) со светофором + латентностью; секция Kafka lag с раскрытием по партициям. Состояния: сервис не отвечает, lag растёт, снапшот устарел (`generated_at` старее ~3 мин).

---

# ЧАСТЬ 2b. A2P ADMIN — ДОП. СПЕЦИФИКАЦИЯ (экраны 21-41)

Отдельный референс в `MPP Backoffice.dc.html`, стилизованный под реальную SMS-агрегаторскую admin-панель — не 1:1 повтор Экранов 1-20 выше, впервые разобран этим документом. Экраны 42-46 (API Explorer/Debugger/Webhooks/Usage Explorer/API Keys) сам файл маркирует "вне скоупа спеки" — не разбираются здесь.

Легенда та же, что в ЧАСТИ 1. Плюс отдельная категория **⚠️ обсудить** — там, где решение архитектурное (какая это сущность, нужна ли она вообще этой платформе), не просто объём работы.

## Экран 21. Home

**Статус: 🔨 тот же класс, что Экран 1 Dashboard** — обзорные карточки KPI для A2P-раздела, агрегирующего эндпоинта нет и не нужен: фронтенд сам собирает из уже существующих API (тот же выбор, что уже сделан для Dashboard, раздел "8. Dashboards" `BACKOFFICE_ROADMAP.md`). Не отдельная работа — тот же экран, что уже спроектирован.

## Экран 22. Senders

**Статус: ✅ то же, что `senders[]` внутри Экран 12 Партнёры.** Alpha-имена и короткие номера — это `senders[].type` внутри партнёрской конфигурации, отдельной сущности "Sender" в платформе нет. Не отдельная работа.

## Экран 23. CTNs

**Статус: ✅ есть.** Подтверждено пользователем: реальная сущность для офлайн телеком-биллинга — CTN привязан к (партнёр, категория), офлайн-джоб позже генерирует CDR-запись (CTN, название услуги, сумма списания) для сверки с оператором. Реализовано через generic config-version механизм (`entity_type=CONFIG_ENTITY_TYPE_CTN`), `entity_id` — составной ключ `partner_id:category` (тот же паттерн, что `subscriber_consent`). Экран `CTNsView.vue`. Сам CDR-экспорт — см. "Офлайн-биллинг / CDR-экспорт" ниже, план, не реализация.

## Экран 24. Patterns

**Статус: ✅ то же, что Экран 7 Шаблоны и паттерны** (`policy_template`). Не отдельная работа.

## Экран 25. Requests

**Статус: ❌ backend не существует — то же самое, что Экран 8 Модерация шаблонов**, просто другой UI-паттерн (батчи заявок + комментарии + статус "запрошены изменения", не только approve/reject). Тот же backend нужен: `pending_review`-статус в `policy.policy_template`, партнёрский `POST /v1/self-service/templates/propose`, админский `POST /v1/templates/{id}/approve|reject`. Оценка та же — **3-4 дня**, самая содержательная новая фича из всего списка 1-41, делать осознанно отдельным заходом (см. "Приоритет" в `BACKOFFICE_ROADMAP.md`).

## Экран 26. Partners

**Статус: ✅ то же, что Экран 12 Партнёры** — admin read-only список поверх той же сущности. Не отдельная работа.

## Экран 27. Chat

**Статус: ❌ ничего похожего нет нигде в платформе** — ни топика, ни таблицы для сообщений админ↔партнёр. Реальная новая фича: хранилище сообщений + polling/websocket-доставка + экран с обеих сторон (`backoffice-ui` и `partner-portal-ui`). Не оценивать как "1 день" — если нужно, обсуждать масштаб отдельно, это не CRUD-обёртка над существующими данными.

## Экран 28. Blacklist numbers

**Статус: ✅ backend есть (тот же, что Экран 13), но browse-список принципиально не построить.** `GET /v1/compliance/consent` уже проксирован (см. Экран 13) — но он работает только "проверка одного msisdn", не "покажи все заблокированные номера": данные лежат в Runtime Redis по ключу `msisdn`, реверс-индекса нет. Список ВСЕХ заблокированных потребовал бы нового индекса (в Redis или Postgres) — архитектурное решение (что индексировать, кто его поддерживает в актуальном состоянии), не просто эндпоинт. Дизайнить как есть (поиск по номеру), не как browse-таблицу, если явно не решите строить индекс.

## Экран 29. TPS

**Статус: 🔨 частично.** По партнёру/категории/дню — тот же источник, что Reports (ClickHouse, уже поднят локально, см. "Известные баги" история `BACKOFFICE_ROADMAP.md`), другая нарезка временного ряда, не агрегат за период. По IP — ❌ IP не пишется в `message_read_model`/аналитику вообще, нужна новая инструментация раньше, чем экран.

## Экран 30. MT Sessions

**Статус: ❌ пересекается с 11.1, но добавляет write-путь, которого нет.** Таблица живых SMPP-сессий — то же, что 11.1 (Экран 11, уже реализовано как read). Кнопка "Disconnect" — новый функционал: сегодня нет метода принудительно разорвать SMPP-сессию нигде в `operator-smpp-session-manager`. Нужен новый write-эндпоинт + метод в самом сервисе (не просто прокси существующего), не read-only view.

## Экран 31. Statistics

**Статус: 🔨 тот же класс, что Reports/TPS** — тот же ClickHouse-источник, другая нарезка (по sender/category/IP, см. ограничение IP в Экране 29). Не отдельная работа сверх уже запланированных отчётов.

## Экран 32. Categories

**Статус: ✅ есть.** Полная замена фиксированного словаря (SERVICE/TRANSACTION/ADVERTISING/UNTEMPLATED/BLOCKED, раньше жил только как соглашение в документации, не proto/CHECK/ClickHouse-enum — проверено кодом) на управляемую сущность (`entity_type=CONFIG_ENTITY_TYPE_CATEGORY`, name/regex/count_in_cdr), существующие 5 значений засеяны миграцией V030. Экран `CategoriesView.vue`. **Граница объёма**: `policy_template.category`/`billing_tariff.price_per_segment` остаются свободной строкой без изменений — Categories это слой управления/метаданных, не новая валидация в `policy-service`/`billing-service`.

### Офлайн-биллинг / CDR-экспорт (использует Categories + CTN) — план, не реализовано

`billing.billing_ledger` не хранит category сегодня (она долетает только транзитно в Kafka `StageCompletedEvent`) — нужна миграция, добавляющая колонку `category`, и однострочное изменение в сервисе, который пишет строки ledger (уже потребляет это событие). Проблема эффективности (обычный cron re-scan не потянет объём) решается событийным consumer'ом, подписанным напрямую на тот же Kafka-топик биллинговых событий — не периодическим full-scan Postgres: для каждого события с активным CTN-маппингом (`partner_id+category` → `ctn/service_name`, через кеш активных config-версий) сразу дописывается строка в текущий CDR-файл, ротируемый по времени/размеру; если нужен вариант с курсором по Postgres (сверка/дозаполнение) — курсор persist'ится после каждого успешного флеша, `WHERE id > cursor`, тот же at-least-once паттерн, что уже используют Kafka-консьюмеры этой платформы. Формат самого CDR-файла (порядок колонок, разделитель) уточняется при реальной реализации с телеком-партнёром/регламентом.

## Экран 33. Admin users

**Статус: ✅ есть.** Пользователь подтвердил: LDAP не нужен, нужно полноценное локальное управление аккаунтами (логин/пароль прямо из бэкофиса), реальный Keycloak — сильно позже. Новая таблица `iam.staff_accounts` (username/password_hash/display_name/active), новые RPC в `IamService` (`CreateStaffAccount`/`ListStaffAccounts`/`DeactivateStaffAccount`/`VerifyStaffCredentials`, bcrypt внутри `iam-service`, хеш никогда не покидает процесс), новый `POST /v1/auth/login` в `backoffice-api` (единственный маршрут без JWT-проверки, подписывает токен отдельным от общих self-service API RSA-keypair'ом), `LoginView.vue` (форма вместо прежнего "вставь JWT в textarea") + `AdminUsersView.vue`. Роли новым сотрудникам назначаются отдельно, на уже существующем экране "Users & Roles" (Экран 16) — этот экран только про сам аккаунт. Живьём проверено: создать сотрудника → залогиниться → получить рабочий JWT → деактивировать → повторный логин отклонён.

## Экран 34. Roles

**Статус: ✅ то же, что Экран 16** (`GET /v1/iam/roles`), просто другой UI-паттерн (dual-list "доступно / назначено" вместо матрицы прав). 0 backend, чистая фронтенд-полировка.

## Экран 35. Partner Users

**Статус: ✅ есть** — `GET/POST/DELETE /v1/iam/partner-portal-assignments` (право `iam:manage`, тот же паттерн, что `/v1/iam/staff-assignments`, Экран 16), новые RPC в `IamService` (`ListPartnerPortalAssignments`/`AssignPartnerPortalRole`/`RevokePartnerPortalRole`), экран `PartnerUsersView.vue` — отдельный от "Users & Roles" (другая таблица, фиксированный набор ролей `partner-admin`/`partner-viewer`, не открытый каталог `iam.roles`). `external_id` — FK на `iam.partner_portal_users`, назначение неизвестному пользователю → 404. Живьём проверено: assign → list → revoke → идемпотентный повторный revoke, плюс оба негативных пути (неизвестный пользователь → 404, невалидная роль → 400).

## Экран 36. Pattern Placeholders (бывший "Regex Patterns")

**Статус: ✅ есть, включая реальное использование в движке матчинга.** Пользователь подтвердил переименование и нужность (luminous-hugging-charm.md): именованный regex-плейсхолдер (`name`/`regex`/`description?`), используемый внутри `policy_template.pattern` как `%{name}` — не просто справочник "name + pattern + используется в", а реально резолвится и извлекает значение при матчинге сообщения.

Реестр — та же generic config-version сущность, что Categories/CTN/Guides (`CONFIG_ENTITY_TYPE_PATTERN_PLACEHOLDER`, `config_schemas/pattern_placeholder.schema.json`, entity_id=name), 0 нового backend в `configuration-service`/`backoffice-api` — только новый entity_type + экран `PatternPlaceholdersView.vue` (форма name/regex/description + список + архивация, тот же паттерн, что `GuidesView.vue`).

Реальная (не 0-backend) часть — движок матчинга `policy-service`:
- `template_matching.rs`: третий синтаксис плейсхолдера рядом с `%w`/`%d{n,m}` — `%{name}` (имя `^[a-z][a-z0-9_]*$`). `PlaceholderRegistry` (name -> скомпилированный regex, обёрнутый в `^(?:...)$` — регион между литералами должен удовлетворять regex ЦЕЛИКОМ) резолвится при матчинге; в отличие от `%w`/`%d{n,m}` (структурная проверка формы, ничего не извлекают), `%{name}` ЕЩЁ И извлекает подошедшую подстроку в `MatchedTemplate::extracted_values: HashMap<String, String>`. Неизвестное имя (не в реестре) или невалидный regex в конфиге — fail closed: шаблон с этим плейсхолдером просто никогда не матчится, не паника и не ошибка. Специфичность `%{name}` в тай-брейке между конкурирующими шаблонами — строже `%d{n,m}` и `%w` (admin-заданный regex по построению не менее специфичен, чем встроенные хардкод-плейсхолдеры).
- `config_reload.rs`: реестр — живой, hot-reload через тот же `config.changes`-консьюмер, что уже читает `POLICY_RULESET`/`POLICY_TEMPLATE` (третий entity_type, `PATTERN_PLACEHOLDER`, entity_id-keyed overlay, тот же принцип, что overlay шаблонов) — правки в бэкофисе применяются без рестарта `policy-service`.

**Открытый вопрос, сознательно не решённый в рамках этого захода**: извлечённые значения (`MatchedTemplate::extracted_values`) СЧИТАЮТСЯ и логируются (`tracing::debug!` в `policy_engine::evaluate_policy`) для отладочной видимости, но НИКУДА дальше по пайплайну не прокидываются — не в `StageCompletedEvent`/`platform-contracts/common/stage_contract.proto`, не в `analytics-writer`, никуда. Кому и зачем нужно значение ПОСЛЕ матчинга (Billing? партнёрский API? аналитика?) и как это меняет публичный контракт стадии `policy` — отдельное архитектурное решение, требующее подтверждения пользователя, не принято молча здесь.

Проверено: `cargo test` в `policy-service` (30 новых/затронутых тестов в `template_matching.rs` — парсинг `%{name}`, успешный матч+извлечение, полное совпадение региона с regex (не частичное), неизвестное имя, невалидный regex в реестре, специфичность против `%w`/`%d{n,m}`, множественные именованные плейсхолдеры в одном шаблоне, обратная совместимость `find_match` без реестра; плюс новые тесты hot-reload в `config_reload.rs` — регистрация/архивация/перерегистрация плейсхолдера через `config.changes`); `go test ./...` в `configuration-service`/`backoffice-api` не изменился и остаётся зелёным; `npm run build` + `npm test -- --run` в `backoffice-ui` зелёные. Живая docker-проверка сквозного пути (реальный `%{...}` в реальном `policy_template` против реального сообщения через Kafka) не выполнялась в рамках этого захода — см. коммиты `policy-service: named pattern placeholder %{name}...` за деталями того, что именно покрыто тестами.

## Экран 37. Spam Patterns

**Статус: ⚠️ обсудить, вероятно 0 работы.** По названию похоже на уже существующие banwords/antispam внутри `policy_ruleset` (Экран 10, уже полностью функционален через `ConfigView.vue`). Сверить семантику с пользователем перед тем, как строить как отдельное — может оказаться тем же случаем, что banwords/route_table (раздел 2/3 `BACKOFFICE_ROADMAP.md`): уже закрыто, просто нужно подтверждение.

## Экраны 38-40. A2P / P2A / DLRs

**Статус: ❌ то же ограничение, что уже задокументировано в Блоке C выше.** Raw per-PDU SMPP-логи (`submit_sm_resp → OK 68ms`) нигде не собираются — есть только момент submit, момент прихода DLR, сырой текст DLR. Нужна новая инструментация в `operator-smpp-session-manager` (публиковать таймстамп на каждый PDU), **~2-3 дня**, уже оценено выше. P2A (сообщения от абонента к платформе, не наоборот) — архитектурно не существует нигде в текущем пайплайне (весь пайплайн спроектирован под A2P-направление), **~1-2 недели отдельной фазой**, не между делом.

## Экран 41. Guides / Guide management

**Статус: ✅ есть (только админская сторона).** Тот же generic config-version путь, что Categories/CTN (`entity_type=CONFIG_ENTITY_TYPE_GUIDE`, `entity_id`=slug, payload `{title, body_markdown, category}`), экран `GuidesView.vue` — форма (title/slug/category/body_markdown) + список + архивация, slug выводится из title автоматически с возможностью ручного переопределения. Markdown — обычный textarea без live-превью (в зависимостях `backoffice-ui` нет markdown-рендерера, новую библиотеку ради необязательного превью не тянули). **Открытый вопрос, специально не решён**: показ опубликованных гайдов партнёрам где-либо в `partner-portal-ui` — это ТОЛЬКО админский экран управления контентом, партнёрская сторона не построена и не считается закрытой.

---

# ЧАСТЬ 3. СВОДКА РАБОТ

| # | Что | Оценка | Блокирует дизайн? |
|---|---|---|---|
| 1 | Хронология стадий по сообщению (`/timeline`) | 1-2 дня | нет, контракт выше |
| 2 | SMPP/DLR события по сообщению (`/operator-events`) | 1-2 дня | нет |
| 3 | Расширение шапки сообщения (sender/msisdn/category/operator/sandbox) | 1 день | нет |
| 4 | Биллинг: ledger + summary | 2 дня | нет |
| 5 | Биллинг по сообщению | 0.5 дня | нет |
| 6 | Фильтр по дате в списке сообщений | 0.5 дня | нет |
| 7 | Тестовая отправка через админку | 0.5 дня | нет |
| 8 | Прокси: templates, compliance, operator-metrics | 1.5 дня | нет |
| 9 | Живое состояние маршрутов операторов | 1 день | нет |
| 10 | `sender_id` в аналитику (для топ alpha-имён) | 1 день | нет |
| 11 | Модерация шаблонов (workflow) | 3-4 дня | нет |
| 12 | Поднять ClickHouse (нужен для всех графиков) | 0.5 дня | **да, для дашборда** |
| 13 | MO/P2A сообщения | 1-2 недели | **да, если нужны** |
| 14 | Пер-PDU SMPP таймстампы | 2-3 дня | нет, только детализация |
| 15 | Partner Users admin CRUD (экран 35) | 0.5-1 день | нет |
| 16 | Roles dual-list UI (экран 34) | 0.5 дня, 0 backend | нет |
| 17 | Модерация шаблонов, batch/comments-паттерн (экран 25) | тот же backend, что #11 | нет |
| 18 | MT Sessions Disconnect (write-путь, экран 30) | не оценено, новый метод в operator-smpp-session-manager | нет |
| 19 | Chat админ↔партнёр (экран 27) | не оценено, реальная новая фича | **да, если нужен** |
| 20 | ~~Guides/Guide management CMS (экран 41)~~ ✅ готово (только админская сторона) | 0 backend, generic config-version | нет |

**Решено пользователем (luminous-hugging-charm.md):**
- CTN (экран 23) — нужен, реальная сущность для офлайн-биллинга. ✅ реализовано.
- Categories (экран 32) — полная замена enum. ✅ реализовано.
- Spam Patterns (экран 37) = Banwords (Экран 10) — та же сущность, отдельно не строим.
- Regex Patterns (экран 36) → переименован "Pattern Placeholders" — именованные переменные с regex для использования внутри шаблонов. Не 0-backend: требовал изменения движка матчинга `policy-service` (`template_matching.rs`) — ✅ реализовано отдельным заходом (см. полное описание в разделе "Экран 36" выше): `%{name}` синтаксис, `PlaceholderRegistry`, hot-reload через `config.changes`, извлечение значения. Прокидывание извлечённого значения дальше по пайплайну — отдельный, сознательно не решённый вопрос.
- Admin users (экран 33) — LDAP пропускаем, нужно полноценное локальное управление аккаунтами (логин/пароль). Строится (см. `BACKOFFICE_ROADMAP.md`).
- Chat админ↔партнёр (экран 27) — подтверждён нужным. Реальная новая фича с нуля (хранилище + доставка + два UI) — отдельный заход, не между делом.
- TPS-дашборд (экран 29) — подтверждён "сразу полная версия с настраиваемыми виджетами". Почти все данные для него сегодня не существуют — фазовый план в `BACKOFFICE_ROADMAP.md`.
- A2P/P2A/DLRs per-PDU логи (экраны 38-40) — подтверждены нужными именно как в референсе.

**Остаются открытыми:**
- MO/P2A — нужны сейчас или позже?
- Тариф per-operator — нужен?
- Поиск сообщений по номеру абонента — нужен? (потребует индекс/проекцию)
- Blacklist numbers как browse-список (экран 28) — нужен новый реверс-индекс (Redis или Postgres), или оставить только "проверка одного номера" (как Экран 13 сегодня)?
