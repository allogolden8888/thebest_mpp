# A2P MPP — Спецификация PostgreSQL, Redis, Kafka

**Основание:** `hld.md`, `service_io_contracts.md`, `service_internal_methods.md`
**Статус:** LLD-уровень, детали (constraint-имена, точные типы partitioning-функций, ACL-биндинги) уточняются при реализации. Партиции/replication factor в разделе Kafka — расчётные значения, вывод см. в `capacity_model.md`; здесь фиксируется итоговая рекомендация.

Точная protobuf-схема сообщения по каждому топику (и валидированный, компилируемый `.proto`-код) — `platform_contracts.md` и каталог `platform-contracts/`. Таблица в §3 ниже описывает топики (ключ/партиции/retention/producer-consumer); привязку топик → конкретное сообщение см. в `platform_contracts.md` §3.

Реальные, применённые и провалидированные на PostgreSQL 17 DDL-миграции по всем таблицам §1 — каталог `migrations/` (см. `migrations/README.md`). Несколько несогласованностей между этим документом и `capacity_model.md`, а также пара пропущенных ограничений целостности, найдены только на этапе реального DDL — отмечены по месту ниже и исправлены в обоих документах.

---

# 1. PostgreSQL

Единый логический кластер (HLD §17.1), схемы разделены по доменам. Домены ниже соответствуют схемам `config`, `messaging`, `billing`, `dlr`, `reconciliation`, `control`.

## 1.1. `config.config_versions`

| Колонка | Тип | Примечание |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `entity_type` | TEXT | `pipeline` \| `policy` \| `billing_tariff` \| `routing_table` \| `partner` \| `operator` |
| `entity_id` | TEXT | |
| `version` | INT | монотонно возрастает в рамках `(entity_type, entity_id)` |
| `payload` | JSONB | immutable-снимок конфигурации |
| `status` | TEXT | `active` \| `archived` (см. HLD §16.1 — hard delete не выполняется) |
| `created_at` | TIMESTAMPTZ | |
| `created_by` | TEXT | |

`UNIQUE (entity_type, entity_id, version)`; `INDEX (entity_type, entity_id, status)`.

## 1.2. `config.config_outbox`

| Колонка | Тип | Примечание |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `config_version_id` | BIGINT FK → `config_versions.id` | |
| `entity_type`, `entity_id` | TEXT | денормализовано для быстрого чтения publisher'ом |
| `payload` | JSONB | |
| `published` | BOOLEAN DEFAULT false | |
| `created_at`, `published_at` | TIMESTAMPTZ | |

`INDEX (created_at) WHERE published = false` — partial index под `poll_outbox`.

## 1.3. `messaging.message_read_model`

| Колонка | Тип | Примечание |
|---|---|---|
| `message_id` | UUID PK | |
| `partner_id`, `application_id` | TEXT | |
| `trace_id` | UUID | |
| `pipeline_id`, `pipeline_version` | TEXT/INT | |
| `current_status` | TEXT | из `message.lifecycle` |
| `terminal` | BOOLEAN | |
| `created_at`, `updated_at` | TIMESTAMPTZ | |

`INDEX (partner_id, created_at)`; `INDEX (trace_id)`. Таблица — UPSERT-heavy (обновляется на каждый `message.lifecycle`), см. `capacity_model.md` §4 про HOT UPDATE и fillfactor.

## 1.4. `messaging.message_lifecycle_history`

Append-only, партиционирована по времени (`RANGE (occurred_at)`, **почасовые** партиции — исправлено при переходе на реальный DDL: капасити-модель (`capacity_model.md` §6.3) явно рекомендует почасовое партиционирование при 27–40к строк/с, суточная партиция при таком объёме слишком велика для индекса/vacuum; здесь раньше по недосмотру было написано «суточные» — несогласованность между документами, замеченная только на уровне `migrations/V005__message_lifecycle_history.sql`). Retention — 72 часа (`message_ttl` 24ч + буфер на расследования), обслуживается функциями `messaging.create_lifecycle_history_partition`/`drop_old_lifecycle_history_partitions` (`migrations/V015__partition_maintenance.sql`), вызываемыми внешним планировщиком раз в час — не привязано к pg_partman ради портируемости.

| Колонка | Тип |
|---|---|
| `message_id` | UUID |
| `lifecycle_version` | BIGINT |
| `status` | TEXT |
| `event_id` | UUID |
| `occurred_at` | TIMESTAMPTZ (partition key) |
| `source` | TEXT |

`PRIMARY KEY (message_id, lifecycle_version, occurred_at)`; `INDEX (message_id)` на каждой партиции.

## 1.5. `messaging.dlq_record`

| Колонка | Тип | Примечание |
|---|---|---|
| `stage_execution_id` | UUID PK | |
| `message_id` | UUID | |
| `stage_name` | TEXT | |
| `attempt` | INT | |
| `original_command` | BYTEA | protobuf исходной stage-команды — то, что реально нужно для republish (HLD §20) |
| `reason_code`, `error_detail` | TEXT | |
| `created_at` | TIMESTAMPTZ | |
| `replay_status` | TEXT | `pending` \| `replayed` \| `expired` |

`INDEX (message_id)`; `INDEX (stage_name, created_at)`.

## 1.6. `messaging.replay_audit`

| Колонка | Тип |
|---|---|
| `id` | BIGSERIAL PK |
| `stage_execution_id` | UUID |
| `requested_by` | TEXT |
| `requested_at` | TIMESTAMPTZ |
| `checks_passed` | JSONB (`ttl`, `idempotency`, `billing_side_effect`, `delivery_ambiguity` → bool) |
| `outcome` | TEXT |
| `target_topic` | TEXT |

`INDEX (stage_execution_id)`.

## 1.6a. Тарифная конфигурация (`billing_tariff`)

Не отдельная таблица — запись в `config.config_versions` (`entity_type='billing_tariff'`, `entity_id=partner_id`), объём заведомо небольшой (партнёров × категорий, не десятки/сотни тысяч строк, как у шаблонов) — выделенная таблица избыточна.

Форма `payload` (JSONB) — тарификация по сегментам сообщения, цена зависит от категории, присвоенной Policy (`PolicyResult.category`, `platform-contracts/common/stage_contract.proto`), **включая служебные категории `UNTEMPLATED` и `BLOCKED`** — отклонённое Policy сообщение всё равно тарифицируется, единой ценой независимо от конкретной причины отклонения (банворды/opt-out/время суток/anti-spam/шаблон/кодировка/отправитель — всё одна категория `BLOCKED`).

```json
{
  "currency": "UZS",
  "price_per_segment": {
    "SERVICE": 100,
    "TRANSACTION": 120,
    "ADVERTISING": 300,
    "UNTEMPLATED": 3000,
    "BLOCKED": 100
  },
  "default_category": "UNTEMPLATED"
}
```

`price_per_segment` — обязателен ключ `BLOCKED` (иначе Billing не может тарифицировать отклонённое Policy сообщение — стадия не должна падать из-за отсутствующей записи в тарифе, но и не должна тихо начислять 0). Остальные ключи — категории, определённые для конкретного партнёра в его шаблонах (`policy.policy_template.category`, §1.9b) плюс `UNTEMPLATED`.

## 1.7. `billing.billing_ledger`

| Колонка | Тип | Примечание |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `charge_id` | UUID UNIQUE | `= stage_execution_id`, единственный источник идемпотентности (HLD §15.1/§15.4) |
| `account_id`, `partner_id` | TEXT | |
| `amount` | NUMERIC(18,4) | |
| `currency` | CHAR(3) | |
| `entry_type` | TEXT | `charge` \| `compensating` |
| `source_charge_id` | UUID NULL | заполнено только для `compensating` |
| `created_at` | TIMESTAMPTZ | |

`INDEX (account_id, created_at)`; `INDEX (partner_id, created_at)`. Вставка — только через `INSERT ... ON CONFLICT (charge_id) DO NOTHING`, апдейтов нет (append-only ledger).

## 1.8. `dlr.dlr_correlation`

Партиционирована по времени (`RANGE (submitted_at)`, часовые партиции — объём выше, чем у lifecycle history, см. капасити-модель §5). Drop партиций по достижении `operator DLR SLA + safety margin` (HLD §14) — реализация: `dlr.create_correlation_partition`/`drop_old_correlation_partitions` (`migrations/V015__partition_maintenance.sql`), по умолчанию 48ч retention, требует уточнения по реальным SLA операторов.

| Колонка | Тип |
|---|---|
| `operator_id` | TEXT |
| `smsc_message_id` | TEXT |
| `segment_id` | INT |
| `message_id` | UUID |
| `stage_execution_id` | UUID |
| `submitted_at` | TIMESTAMPTZ (partition key) |
| `expires_at` | TIMESTAMPTZ |

`PRIMARY KEY (operator_id, smsc_message_id, segment_id, submitted_at)`; `INDEX (expires_at)` для cleanup job (не должен понадобиться при партиционировании — drop партиции дешевле, чем `DELETE`, индекс — на случай ручной чистки).

## 1.9. `reconciliation.reconciliation_cases`

| Колонка | Тип |
|---|---|
| `case_id` | UUID PK |
| `message_id`, `stage_execution_id` | UUID |
| `operator_id` | TEXT |
| `status` | TEXT (`open`/`resolved`/`unresolved`) |
| `opened_at`, `resolved_at`, `deadline_at` | TIMESTAMPTZ |
| `evidence` | JSONB |

`INDEX (message_id)`; `INDEX (status, deadline_at)`.

## 1.9a. `routing.number_range`

Источник данных для Destination Resolution Service (`resolve_operator_by_range`, `service_internal_methods.md` §1.4a).

| Колонка | Тип | Примечание |
|---|---|---|
| `range_start` | BIGINT | начало диапазона MSISDN (числовое представление) |
| `range_end` | BIGINT | конец диапазона |
| `operator_id` | TEXT | |
| `version` | INT | |
| `status` | TEXT | `active` \| `archived` |
| `updated_at` | TIMESTAMPTZ | |

`INDEX (range_start, range_end)`. Объём — сотни-тысячи строк (номерные диапазоны одной страны), полностью загружается в локальный immutable snapshot Destination Resolution Service, не требует range-запросов в реальном времени. Обновления идут тем же механизмом `config_outbox` → `config.changes`, что и остальная конфигурация (`entity_type='number_range'`).

**Пример реальных диапазонов (Узбекистан, неполный список — только префиксы, подтверждённые на сегодня):**

| Префикс (после `998`) | Оператор |
|---|---|
| 90, 91, 92, 20 | Beeline |
| 50, 93, 94 | Ucell |
| 98, 99 | Uzmobile |

Список заведомо неполный (есть и другие префиксы у этих и других операторов Узбекистана — Perfectum, UMS и т.д.) — таблица наполняется реальными данными на этапе реализации, здесь только подтверждённый пример для проверки структуры.

**MNP (перенос номера) — статус: требует решения, см. открытый вопрос в общем чате.** Планируемая структура — отдельная overlay-таблица `routing.number_portability_override (msisdn TEXT PK, operator_id TEXT, ported_at TIMESTAMPTZ, source TEXT)`, которую Destination Resolution Service проверяет **до** попадания в `number_range` (точное совпадение по MSISDN перекрывает диапазон). Наполнение — периодическая синхронизация из внешней базы переносимости номеров, тем же принципом, что и остальная конфигурация (не live-запрос на каждое сообщение — иначе Destination Resolution перестаёт быть чистым вычислением без внешних зависимостей, HLD §8). Периодичность синка и источник (API регулятора/MNP-оператора) — не определены, требуют вашего решения.

## 1.9b. `policy.policy_template`

Выделенная таблица, а не запись в `config.config_versions` — объём потенциально на порядки больше, чем у остальных конфигурационных сущностей (`services_specifictaion.md` §2.5), и Backoffice должен уметь быстро искать/фильтровать по категории/партнёру/оператору.

| Колонка | Тип | Примечание |
|---|---|---|
| `template_id` | UUID PK | |
| `partner_id` | TEXT | |
| `operator_id` | TEXT NULL | `NULL` = общий для всех операторов этого партнёра; иначе операторо-специфичный (правила сравнения различаются по оператору, HLD §5.3) |
| `channel` | TEXT | `SMS` сегодня, `EMAIL`/`PUSH` зарезервированы |
| `category` | TEXT | |
| `pattern` | TEXT | сам шаблон |
| `version` | INT | |
| `status` | TEXT | `active` \| `archived` |
| `created_at`, `updated_at` | TIMESTAMPTZ | |

`INDEX (partner_id, channel, status)` — под путь построения per-partner snapshot; `INDEX (category)`, `INDEX (operator_id)` — под browsing в Backoffice. Изменения идут через тот же `config_outbox` → `config.changes` (`entity_type='policy_template'`, `entity_id=template_id`) — Policy Service обновляет **только запись изменившегося партнёра** в своей карте `partner_id → автомат`, не весь snapshot (`services_specifictaion.md` §2.5).

### Синтаксис `pattern`

Шаблон — литеральный текст с плейсхолдерами:

```text
%w    — один "словоподобный" токен: непрерывная последовательность непробельных
        символов до следующего пробела/литерала шаблона. Без ограничения
        по длине и набору символов — намеренно (решение принято явно,
        не значение по умолчанию): контентные проверки (банворды, п.5)
        применяются к полному нормализованному тексту сообщения независимо
        от того, что попало внутрь плейсхолдера, поэтому пермиссивность
        %w не создаёт обход контентной модерации — только определяет,
        матчится ли структура целиком.
%d{n,m} — от n до m цифровых символов; разделители (пробелы, дефисы и т.п.)
        между цифрами игнорируются при подсчёте — эквивалент "снять все
        не-цифровые символы из региона, затем проверить длину результата
        в [n,m]", а не строгий `\d{n,m}` без пропусков.
```

Пример: шаблон `%w shartnoma bo'yicha %d{1,6} so'm to'lovni bugun amalga oshiring...` матчит текст `Hello1238!@* shartnoma bo'yicha 1 2 3 4 5 6 so'm to'lovni bugun amalga oshiring...` — `%w` захватывает `Hello1238!@*` целиком (непробельный токен), `%d{1,6}` захватывает `1 2 3 4 5 6` → после снятия пробелов `123456`, 6 цифр, попадает в `{1,6}`.

**Алгоритм матчинга — не чистый Aho-Corasick.** Чистый multi-pattern substring matching (Aho-Corasick, изначально выбранный в `services_specifictaion.md` §2.5) находит литеральные подстроки, но не умеет параметризованные плейсхолдеры сам по себе. Практическая схема: шаблон разбивается на чередующиеся литеральные фрагменты и плейсхолдеры; Aho-Corasick (или сортированный поиск подстрок) используется для быстрого отбора **кандидатов** — шаблонов, чьи литеральные фрагменты в правильном порядке присутствуют в тексте; затем для кандидата регионы между литералами проверяются на соответствие типу плейсхолдера (`%w`/`%d{n,m}`) — это уже не multi-pattern поиск, а точечная проверка на коротком фрагменте, дешёвая. Точный алгоритм (в т.ч. как разрешать неоднозначность при нескольких кандидатах) — предмет отдельного LLD Policy Service.

**Банворды — многоязычная нормализация.** Реальный список включает узбекскую латиницу, русскую кириллицу и английский одновременно (`qotaq`, `мудак`, `idiot` в одном списке) — нормализация перед сравнением (п.5 требований Policy) не может ограничиться одним алфавитом/языком: нужна как минимум unicode-нормализация (NFKC), схлопывание похожих по начертанию символов между кириллицей и латиницей (частый приём обхода — писать кириллическую "о" вместо латинской или наоборот), и снятие разделителей внутри слова. Конкретная таблица гомоглифов — LLD Policy Service.

## 1.9c. `policy.subscriber_consent`

Источник истины для consent-блэклистов (требования 4 и 6 Policy Engine). **Не хранится только в Redis** — в отличие от Execution State, opt-out абонента не восстановим из Kafka replay, потеря без резервной копии — тихий compliance-риск (абонент снова начнёт получать то, от чего отказался).

| Колонка | Тип | Примечание |
|---|---|---|
| `msisdn` | TEXT | |
| `scope_type` | TEXT | `CATEGORY` \| `SENDER` |
| `scope_value` | TEXT | имя категории (например `ADVERTISING`) либо `sender_id` |
| `channel` | TEXT | `SMS` сегодня |
| `created_at` | TIMESTAMPTZ | |

`PRIMARY KEY (msisdn, scope_type, scope_value, channel)`; `INDEX (msisdn)`.

Публикуется через тот же `config_outbox` → `config.changes` (`entity_type='subscriber_consent'`), но проецируется не в Configuration Redis (bootstrap-only паттерн), а в Runtime Redis новым воркером **Consent Cache Projector** (Go, симметричен Config Cache Projector) — потому что Policy читает consent-данные на каждое сообщение (hot path), а не при bootstrap. При полной потере этой части Runtime Redis требуется принудительный full resync из PostgreSQL, а не просто ожидание естественного пополнения — это должно быть зафиксировано как явная runbook-процедура на LLD.

## 1.10. `control.execution_control_audit`

| Колонка | Тип |
|---|---|
| `id` | BIGSERIAL PK |
| `scope`, `scope_id` | TEXT |
| `state` | TEXT |
| `admission_rate` | NUMERIC(5,2) |
| `reason` | TEXT |
| `requested_by` | TEXT |
| `created_at`, `expires_at` | TIMESTAMPTZ |

`INDEX (scope, scope_id, created_at)`.

## 1.11. Read-модель для Backoffice/Partner API

Отдельных таблиц не заводим — Backoffice API и Partner API читают из таблиц выше через read replica (HLD §17.1). RBAC/пользователи Backoffice — отдельная небольшая схема `backoffice`, не влияющая на hot-path capacity, детали — LLD Backoffice API.

---

# 2. Redis

Три физически изолированных кластера (HLD §17.3). Ниже — схема ключей.

## 2.1. Runtime Redis

| Ключ | Тип | Поля/значение | TTL | Пишет | Читает |
|---|---|---|---|---|---|
| `exec:{message_id}` | HASH | `pipeline_version, node_id, stage_execution_id, current_state, attempt, deadline, last_applied_event_id` | `message_ttl` + safety margin | Pipeline Engine (CAS) | Pipeline Engine, Critical Sweep (только по найденным просрочкам) |
| `deadlines:{bucket}` | ZSET | score = `deadline` (unix ms), member = `stage_execution_id`; `bucket = hash(stage_execution_id) % N` — шардирование, чтобы не упираться в ограничение Redis Cluster на односегментные ключи | не устанавливается на ключ; запись удаляется `ZREM` при обработке | Pipeline Engine (в той же Lua-транзакции, что CAS) | Critical Sweep (`ZRANGEBYSCORE ... -inf now`, poll ~1с по каждому bucket) |
| `msgctx:{message_id}` | HASH | `body, sender, msisdn, encoding, partner_id, segment_count, channel` | `message_ttl` + safety margin | Pipeline Engine | Policy, Delivery (**не Billing** — получает `segment_count` полем в `BillingExecute`, см. `service_internal_methods.md` §0) |
| `ratelimit:{partner_id}:{application_id}:{window}` | STRING (counter) | INCR + EXPIRE | размер окна (напр. 1s/10s) | Partner REST Receiver, Partner SMPP Gateway — **периодическая синхронизация локального token bucket раз в ~1с, не на каждое сообщение** | то же |
| `smpp:partner_session:{partner_id}:{system_id}` | HASH | `session_id, gateway_instance_id, endpoint, session_epoch, heartbeat` | 3× heartbeat interval | Partner SMPP Gateway | Partner Notification Service |
| `operator_route:{operator_id}:{route_id}` | HASH | `protocol (SMPP\|HTTP), owning_instance_id, endpoint, route_epoch, heartbeat` | 3× heartbeat interval | Operator SMPP Session Manager (`protocol=SMPP`), Operator HTTP Gateway (`protocol=HTTP`) — единый паттерн, HLD §11.4 | Delivery, Delivery Reconciliation |
| `consent:category_blacklist:{msisdn}` | SET | заблокированные категории (например `{ADVERTISING}`) | нет (durable-backed, не эфемерно — см. §1.9c) | Consent Cache Projector (из PostgreSQL `subscriber_consent`) | Policy (`check_category_blacklist`) |
| `consent:sender_blacklist:{msisdn}` | SET | заблокированные `sender_id` | нет (durable-backed) | Consent Cache Projector | Policy (`check_sender_blacklist`) |
| `spam:freq:{msisdn}:{category}:{window}` | STRING (counter) | INCR + EXPIRE | размер окна anti-spam правила | Policy (`increment_spam_counter`) | Policy (`check_spam_frequency`) |

Политика: `noeviction`, без AOF для `exec`/`deadlines`/`msgctx`/`ratelimit`/registry-ключей (эфемерны и восстановимы — из Kafka replay при необходимости, registry — из heartbeat). **Исключение — `consent:*`**: не эфемерно, при полной потере требует принудительного full resync из PostgreSQL (§1.9c), а не пассивного пополнения трафиком, как остальные ключи этого кластера.

## 2.2. Configuration Redis

| Ключ | Тип | Значение | TTL | Пишет | Читает |
|---|---|---|---|---|---|
| `config:current:{entity_type}:{entity_id}` | STRING | номер активной версии | нет (перезаписывается) | Config Cache Projector | bootstrap hot-path сервисов |
| `config:version:{entity_type}:{entity_id}:{version}` | STRING | сериализованный payload | нет (только active + N последних версий, чистка — отдельный worker) | Config Cache Projector | bootstrap hot-path сервисов |

Только bootstrap/cache-miss — не читается на каждое сообщение (HLD §16). Допустима полная потеря — восстанавливается из PostgreSQL.

## 2.3. Billing Redis

| Ключ | Тип | Значение | TTL | Пишет | Читает |
|---|---|---|---|---|---|
| `billing:balance:{account_id}` | HASH | `balance, currency, account_state, account_epoch` | нет (постоянные данные) | Billing Service, Billing Reconciliation (fenced CAS) | Billing Service |
| `billing:charge:{charge_id}` | STRING (dedup marker) | `1` | ограничен (> max retry window, напр. 7 суток) | Billing Service | Billing Service |
| `billing:outbox:{shard}` | STREAM | финансовое событие (charge/compensating) | trim по подтверждённому offset Outbox Publisher'а | Billing Service | Billing Outbox Publisher (consumer group) |

Политика: `noeviction`, AOF `always`/`everysec`, отдельный failover SLA от двух других кластеров (HLD §17.3).

---

# 3. Kafka

Все топики, кроме помеченных `compact`, используют retention **48 часов** (HLD §4 baseline). Replication factor **3**, `min.insync.replicas = 2` для всех — платформа не жертвует durability транспортного слоя. Партиции — расчётная рекомендация под пиковую нагрузку ×3 (HLD §23 burst-требование); вывод чисел — `capacity_model.md` §3.

Число и размер брокеров растут вместе с TPS, а не фиксированы заранее: минимум 3 брокера (ради RF=3) при низкой нагрузке, каждый умеренного размера; при росте TPS сначала растёт размер брокера (vCPU), затем — при приближении к ≈15 000 TPS — число брокеров до 4-5. Точные пороги — `capacity_model.md` §5.

| Топик | Ключ | Партиции | Cleanup policy | Producer(ы) | Consumer(ы) |
|---|---|---|---|---|---|
| `incoming.messages` | `message_id` | 12 | delete | Partner REST Receiver, Partner SMPP Gateway | Pipeline Engine, Lifecycle Writer, Analytics Writer |
| `stage.destination-resolution` | `message_id` | 12 | delete | Pipeline Engine, Standard Lane, Critical Sweep, Replay Service | Destination Resolution Service |
| `stage.policy` | `message_id` | 12 | delete | Pipeline Engine, Standard Lane, Critical Sweep, Replay Service | Policy |
| `stage.billing` | `message_id` | 12 | delete | Pipeline Engine, Standard Lane, Critical Sweep, Replay Service | Billing |
| `stage.routing` | `message_id` | 12 | delete | Pipeline Engine, Standard Lane, Critical Sweep, Replay Service | Routing |
| `stage.delivery` | `message_id` | 12 | delete | Pipeline Engine, Standard Lane, Critical Sweep, Replay Service | Delivery |
| `stage.delivery-reconciliation` | `message_id` | 6 | delete | Pipeline Engine, Standard Lane, Critical Sweep, Replay Service | Delivery Reconciliation |
| `stage.completed` | `message_id` | 32 | delete | Destination Resolution, Policy, Billing, Routing, Delivery, Delivery Reconciliation, Critical Sweep | Pipeline Engine, Message State Resolver, Analytics Writer |
| `delivery.status` | `message_id` | 12 | delete | DLR Manager | Message State Resolver, Delivery Reconciliation |
| `message.lifecycle` | `message_id` | 16 | delete | Message State Resolver | Partner Notification Service, Lifecycle Writer, Analytics Writer |
| `operator.submit.accepted` | `operator_id` | 8 | delete | Operator SMPP Session Manager, Operator HTTP Gateway | DLR Correlation Writer, Delivery Reconciliation |
| `operator.dlr` | `operator_id` | 8 | delete | Operator SMPP Session Manager, Operator HTTP Gateway (нормализованный из webhook) | DLR Manager |
| `operator.dlr.unresolved` | `operator_id` | 4 | delete | Background Lane | DLR Manager |
| `operator.dlr.dlq` | `operator_id` | 2 | delete | DLR Manager | Lifecycle Writer |
| `notification.retry` | `message_id` | 4 | delete | Background Lane | Partner Notification Service |
| `billing.ledger` | `account_id` | 8 | delete | Billing Outbox Publisher | Billing Ledger Writer |
| `config.changes` | `entity_type:entity_id` | 6 | compact | Config Event Publisher | все hot-path сервисы (local snapshot); Consent Cache Projector (фильтр `entity_type='subscriber_consent'`) |
| `execution.control` | `scope:scope_id` | 3 | compact | Execution Control Service | Receiver, Gateway, Pipeline, Critical Sweep, Standard/Background Lane, Billing, Routing, Delivery |
| `scheduler.critical.commands` | `stage_execution_id` | 3 | delete | Backoffice API | Critical Sweep |
| `scheduler.standard.commands` | `message_id` | 8 | delete | Pipeline Engine | Standard Lane |
| `scheduler.background.commands` | `message_id` | 4 | delete | DLR Manager, Partner Notification Service | Background Lane |
| `scheduler.standard.state.changelog` | `message_id` | 16 | compact | Standard Lane (internal) | Standard Lane (internal, recovery) |
| `scheduler.background.state.changelog` | `message_id` | 8 | compact | Background Lane (internal) | Background Lane (internal, recovery) |
| `message-state.changelog` | `message_id` | 40 | compact | Message State Resolver (internal) | Message State Resolver (internal, recovery) |
| `stage.destination-resolution.dlq` | `message_id` | 2 | delete | Critical Sweep | Lifecycle Writer |
| `stage.policy.dlq` | `message_id` | 2 | delete | Critical Sweep | Lifecycle Writer, Replay Service (via PostgreSQL, не напрямую) |
| `stage.billing.dlq` | `message_id` | 2 | delete | Critical Sweep | Lifecycle Writer |
| `stage.routing.dlq` | `message_id` | 2 | delete | Critical Sweep | Lifecycle Writer |
| `stage.delivery.dlq` | `message_id` | 2 | delete | Critical Sweep | Lifecycle Writer |
| `stage.delivery-reconciliation.dlq` | `message_id` | 2 | delete | Critical Sweep | Lifecycle Writer |

**`scheduler.critical.state.changelog` больше не существует** — Critical Sweep не имеет embedded state, дедлайн живёт только в Runtime Redis (`deadlines:{bucket}`, §2.1). Это убирает топик, который был крупнейшим по числу партиций в первой версии (64).

`operator.submit.accepted` и `operator.dlr` намеренно общие для SMPP и HTTP — формат событий одинаковый независимо от протокола, downstream (DLR Manager, DLR Correlation Writer, Delivery Reconciliation) не различает, откуда пришло событие.

**Итого 28 топиков**, суммарно ≈ 278 партиций на пиковую рекомендацию (добавлена `stage.destination-resolution` + её DLQ, 12+2 партиции сверх предыдущих 266) — при кластере из 5-6 брокеров это по-прежнему комфортный запас (см. `capacity_model.md` §3).

Ключевание: везде, где нужна per-message упорядоченность внутри стадии — `message_id`. `operator.*` — по `operator_id`, чтобы DLR Manager мог параллелить корреляцию по оператору без потери упорядоченности внутри одного оператора. `billing.ledger` — по `account_id`, чтобы Ledger Writer видел все операции одного счёта по порядку. Changelog-топики наследуют ключ от состояния, которое реплицируют.
