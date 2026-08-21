# Backoffice — полный план

Дополняет `BACKOFFICE_API_CATALOG.md` (там — контракты для дизайна). Здесь — что строим, в каком порядке, и почему именно так, с реальным статусом данных под каждой темой (не предположения).

## Текущее состояние (уже живое, задеплоено, работает)

| Область | Экран | Статус |
|---|---|---|
| Сообщения | Messages | Работает, но `current_status`/таймлайн застревают — чиню отдельно (см. "Известные баги" ниже) |
| DLQ / Replay | DLQ | Готово |
| Reconciliation | Reconciliation | Готово (billing-специфичный, см. ниже) |
| Config versions/diff | Configuration | Готово |
| Execution Control | Execution Control | Готово |
| Scheduler force-command | — | Готово (backend), нет отдельного пункта меню сейчас |
| Access Control (роли/юзеры) | Users & Roles | Готово |
| Audit Log | Audit Log | Готово |
| Credentials rotation | — | Готово (backend), нет отдельного экрана в меню |
| Incidents | Incidents | Готово |
| Ops Health | Ops Health | Готово (сегодня почини́л DNS-баг) |
| Reports | Reports | Backend готов, упирается в отсутствующий ClickHouse локально |

---

## Новый скоуп — по темам из вашего списка

### 1. Billing (админская видимость)

**Данные, которые реально есть:**
- `billing.billing_ledger` — charge_id, account_id, partner_id, amount, currency, entry_type (charge/compensating), source_charge_id, created_at. Это ПОЛНАЯ история списаний, готова к browse прямо сейчас.
- `billing.reconciliation_audit` — уже проксирован (`GET /v1/reconciliation`).
- Тариф — `billing_tariff.schema.json`: `price_per_segment` по category (SERVICE/TRANSACTION/ADVERTISING/UNTEMPLATED/BLOCKED), `default_category`, `recurring_charges`. **Тариф сегодня per-partner** (TariffCache в billing-service читает per-partner конфиг из configuration-service), **НЕ per-operator** — если нужен тариф per-operator, это архитектурное расширение схемы, не просто новый эндпоинт.

**Что строим:**
1. `GET /v1/billing/ledger` — browse `billing_ledger` (тот же паттерн, что DlqBrowse), фильтры `partner_id`/`entry_type`/дата. **Backend: 1 день**, тот же шаблон, что Messages/DLQ.
2. Tariff management — `GET/POST /v1/config/versions?entity_type=billing_tariff` уже покрывает это ЧЕРЕЗ существующий generic config-version механизм (раздел 5 каталога) — нужен только UI-экран поверх уже существующего API, backend не нужен вообще.
3. Per-operator тариф — если реально нужен, отдельная задача: новое поле в тарифной схеме + изменения в TariffCache resolution logic в billing-service. Не делаю, пока не подтверждено, что нужно.

### 2. Операторы (SMPP routes)

**Данные, которые реально есть:**
- `operator_route:{operator_id}:{route_id}` в Runtime Redis (не Postgres!) — owning_instance_id, endpoint, heartbeat, route_epoch, protocol. Живое состояние, пишет/читает `operator-smpp-session-manager`.
- Статический route table (`route_table.valid.json`) — конфиг per-operator (primary/reserve endpoints, failover) — уже версионируется через тот же generic config-version механизм, что policy_template.

**Что строим:**
1. ✅ `GET /v1/operators/routes` — read-only снимок текущих `operator_route:*` ключей из Redis (кто сейчас держит какой route, когда последний heartbeat), + экран `OperatorRoutesView.vue`. SCAN обязателен (нет источника "список всех operator_id" у backoffice-api), тот же cursor-loop паттерн, что уже в `consent-cache-projector`. Живьём проверено — виден реальный `beeline_uz` SMPP route, который сейчас держит `operator-smpp-session-manager`.
2. Route config (primary/reserve/failover) — ✅ то же самое, что банворды выше: `entity_type=route_table` через уже рабочий `ConfigView.vue`, ничего нового строить не нужно.
3. Новый оператор "с нуля" (новый SMPP-коннекшен, креды, лимиты) — сегодня это ручное добавление сервиса в `docker-compose.yml`/k8s-манифест + route_table конфиг. Полноценный self-service "добавить оператора через UI" — отдельная, немаленькая задача (нужен provisioning-слой, которого нет), не оцениваю как "1 день", честно отдельная фаза.

### 3. Spam / Banwords

**Данные, которые реально есть:**
- `policy_ruleset.banwords.words[]` + normalization-настройки (nfkc/homoglyph_fold/strip_separators) — часть того же `policy_ruleset` config-version объекта, что уже редактируется через `/v1/config/versions?entity_type=policy_ruleset`.

**✅ Уже функционально возможно сегодня, без единой новой строки кода.** `ConfigView.vue` (экран "Configuration", дефолтный маршрут `/`) — это уже полностью универсальный редактор `entity_type`/`entity_id`: список версий, создание, архивирование, `validate`, `diff` — `entity_type` там свободное текстовое поле, не хардкод. Указав `entity_type=policy_ruleset` и нужный `entity_id`, banwords/antispam/time_of_day/sender_validation редактируются прямо сейчас через сырой JSON-textarea. Специализированная форма (тег-инпуты для банвордов, чекбоксы для нормализации, как на референс-дизайне) — это UX-полировка поверх уже рабочей функциональности, не закрытие пробела. Не делаю отдельным заходом, если явно не попросите — приоритетнее закрыть экраны, которых вообще нет.

### 4. Blacklist

**✅ Готово: проксировано через backoffice-api + экран.** `GET/POST /v1/compliance/consent` — плоский HTTP-прокси в `compliance-api` (`internal/httpapi/compliance.go`), форвардит `Authorization`. GET без gate прав (то же решение, что уже приняло само `compliance-api`), POST — `compliance:write`. Экран `BlacklistView.vue`: карточка поиска по msisdn (открыта всем) + карточка ручной блокировки/разблокировки (за `RequirePermission`). Поиск ТОЛЬКО по msisdn — обратный поиск "все заблокированные" не поддержан нигде (msisdn не индексируется reverse в Redis) — честное ограничение, не баг.

По пути нашлись и починены два реальных, ранее не пойманных бага, без которых запись структурно не доходила до Redis (обнаружено живой curl-проверкой, не гипотезой):
1. `services/configuration-service/internal/validate/schemas/subscriber_consent.schema.json` (вручную поддерживаемая копия `config_schemas/`, задокументированный drift risk) не был синхронизирован после того, как канонический файл получил поле `status` — каждый `POST /v1/compliance/consent` падал на валидации.
2. `consent-cache-projector` (Kafka `config.changes` → Runtime Redis) существует и задеплоен в k8s, но полностью отсутствовал в локальном `docker-compose.yml` — тот же класс находки, что уже чинился раньше ("docker-compose: restore 13 local self-service services"). Добавлен.

### 5. Category management per operator

Категории (SERVICE/TRANSACTION/ADVERTISING/etc) — общеплатформенные, не per-operator сегодня (используются в `policy_template.category`, `billing_tariff.price_per_segment`, оба per-partner). "Per-operator категории" как отдельная концепция не существует нигде в текущей архитектуре — прежде чем проектировать экран, нужно решить: это то же самое, что per-partner категории (тогда уже есть, просто UI), или реально новая сущность (тогда — архитектурное решение, не эндпоинт).

### 6. Модерация шаблонов (партнёр → админ)

**Полностью новая фича, ничего не существует.** Партнёр сегодня НЕ МОЖЕТ предложить паттерн — `template-management-service`'s `/v1/templates` пишет напрямую, без approval-стадии. Нужно:
- Новая колонка/статус в `policy.policy_template`: `pending_review` (сейчас только `active`/`archived`).
- Партнёрский эндпоинт: `POST /v1/self-service/templates/propose` (partner-self-service-api) — создаёт запись со статусом `pending_review`.
- Админский эндпоинт: `GET /v1/templates?status=pending_review` (уже работает, статус не гейтится сегодня) + `POST /v1/templates/{id}/approve|reject` (новый, не существует).
- UI с обеих сторон: партнёрская "мои заявки" + админская "очередь модерации".

**Оценка: 3-4 дня** (миграция + 2 новых эндпоинта + 2 экрана). Самая содержательная новая фича из всего списка — реальная новая бизнес-логика, не просто CRUD-обёртка над уже существующими данными.

### 7. Roles & Users

Уже полностью готово (`/v1/iam/*`, экран Access Control). Если нужно — что именно не хватает? (например: bulk-назначение ролей, self-service запрос роли с одобрением — тоже approval workflow, тот же паттерн, что #6).

### 8. Dashboards

Нет единого агрегирующего эндпоинта сегодня — "Home" пришлось бы собирать из уже существующих кусков (Ops Health snapshot + последние N сообщений + открытые инциденты + DLQ count). Два варианта:
- (а) Фронтенд сам делает 3-4 параллельных запроса к уже существующим эндпоинтам, собирает в один экран — **backend не нужен, 1 день фронтенда**.
- (б) Отдельный `GET /v1/dashboard/summary`, агрегирующий на бэкенде — чуть быстрее для клиента, чуть больше кода. Рекомендую (а) — не плодить агрегирующий эндпоинт без реальной нужды.

---

## Известные баги

**✅ Исправлено: `current_status`/лента истории застревали на первом статусе.** Два реальных, независимых бага в `lifecycle-writer`, оба воспроизведены живьём (не гипотеза) в логах реально работающего контейнера:
1. `messaging.create_lifecycle_history_partition` никогда не вызывался ни одним планировщиком — как только текущий час выходил за бутстрап-окно V015, `BatchInsertLifecycleHistory` падал КАЖДЫЙ tick, что блокировало commit офсетов для ВСЕХ топиков (`incoming.messages`/`message.lifecycle`/DLQ), не только history. Фикс — тот же паттерн, что уже был решён для `dlr-correlation-writer` (`EnsurePartition` перед каждым flush).
2. Реальная гонка `incoming.messages`/`message.lifecycle` (разные топики, порядок не гарантирован) — если update приходил раньше insert'а, `UPDATE ... WHERE lifecycle_version < $5` молча не находил строку и терял событие навсегда.

Перезапущен реальный контейнер `docker-lifecycle-writer-1` с фиксом — consumer lag подтверждённо вернулся к 0, сообщения в проде БД реально доходят до DELIVERED/FAILED. Подробности — коммит `856625d`.

---

## Приоритет (моя рекомендация, не финальное решение)

1. ~~Fix lifecycle-writer~~ ✅ готово.
2. ~~Spam/banwords UI~~ / ~~Route config UI~~ — **оказались уже функционально готовы**: `ConfigView.vue` — универсальный редактор любого `entity_type`, уже покрывает `policy_ruleset`/`route_table` сегодня (см. разделы 2/3 выше). Специализированные формы вместо сырого JSON — полировка, не пробел, отложено.
3. ~~Billing ledger browse UI~~ ✅ готово.
4. ~~Blacklist proxy~~ ✅ готово (по пути починены configuration-service schema drift и отсутствующий в compose consent-cache-projector — см. раздел 4 выше).
5. ~~Operator routes read-only view~~ ✅ готово.
6. **Template moderation workflow** — самая большая новая фича, делать осознанно отдельным заходом, не между делом.
7. **Dashboard** — в конце, после того как остальные экраны дадут данные, которые он агрегирует.

Скажите, что переставить местами — список открыт для правок, это не приказ сверху.
