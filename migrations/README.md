# A2P MPP — PostgreSQL миграции

**Основание:** `data_infrastructure_spec.md` §1.
**Статус:** применены и провалидированы на чистом PostgreSQL 17 (Homebrew, локально). Не привязаны к конкретному migration-инструменту (Flyway/golang-migrate/sqitch/goose) — только последовательная нумерация `V0XX__description.sql`, применяются в порядке имён.

## Порядок применения

```text
V001__schemas.sql
V002__config_versions.sql
V003__config_outbox.sql
V004__message_read_model.sql
V005__message_lifecycle_history.sql
V006__dlq_record.sql
V007__replay_audit.sql
V008__billing_ledger.sql
V009__dlr_correlation.sql
V010__reconciliation_cases.sql
V011__number_range.sql
V012__number_portability_override.sql
V013__policy_template.sql
V014__subscriber_consent.sql
V015__partition_maintenance.sql
V016__execution_control_audit.sql
V017__backoffice_stub.sql
V018__dlq_record_replay_in_progress.sql
V019__message_read_model_lifecycle_version.sql
V020__config_outbox_claim_and_retry.sql
V021__billing_reconciliation_audit.sql
V022__policy_template_demo_seed.sql
V023__number_range_operator_id_uz_suffix.sql
V024__policy_template_sender_id.sql
V025__iam.sql
V026__iam_manage_permission.sql
V027__credentials.sql
V028__incident.sql
V029__message_read_model_sandbox.sql
V030__category_ctn_entity_types.sql
V031__staff_accounts.sql
```

Применить локально:

```bash
for f in V*.sql; do psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"; done
```

**V024 и V029 — присоединены слиянием subagent-2 -> subagent-1 (обе ветки
`luminous-hugging-charm.md`, 12-фазный план закрытия API-пробелов).** V024
(sender_id в policy_template, Фаза 2) и исходно-V028-переименованная-в-V029
(sandbox-колонка message_read_model, Фаза 11) были написаны в ветке
subagent-2 параллельно с V025-V028 в этой ветке (subagent-1) — subagent-2
изначально тоже занял номер V028 под свою миграцию (для другой таблицы),
что при слиянии дало коллизию имён; переименована в V029 (subagent-1's
V028__incident.sql оставлен как есть — оба существовали в исходных ветках
независимо, порядок между ними произволен, коллизия была только в номере
файла, не в содержании).

## Что реально проверено (не только «написано и похоже на правду»)

1. **Все 19 миграций применяются с нуля без ошибок** на чистой PostgreSQL 17.
2. **Все 9 реальных номеров из чата** (`998901331835` и остальные) корректно резолвятся в правильного оператора через `routing.number_range` — `SELECT ... WHERE msisdn BETWEEN range_start AND range_end`, 9/9 совпадений.
3. **CHECK-ограничения реально блокируют некорректные данные**, не только написаны: compensating billing-запись без `source_charge_id` — отклонена; шаблон с зарезервированной категорией `BLOCKED` — отклонён.
4. **Идемпотентность billing_ledger** — повторная вставка с тем же `charge_id` через `ON CONFLICT DO NOTHING` реально не создаёт вторую строку.
5. **Партиционирование и retention-функции** — создание почасовых партиций, автоматическая маршрутизация вставки в нужную партицию, drop партиций старше заданного окна — всё выполнено вручную на реальных данных, не просто прочитано глазами.

## V018 — добавлена после CODE_REVIEW.md (config-event-publisher)

`config.config_outbox` получила `claimed_at`/`attempts`/`last_error` — `poll_outbox` (config-event-publisher) раньше не координировал несколько реплик (не было `SELECT ... FOR UPDATE SKIP LOCKED`) и мог опрашивать одну и ту же не-публикуемую (poison) строку вечно, вытесняя реальные pending-строки из `ORDER BY created_at ASC LIMIT N`. См. `services/config-event-publisher/README.md` за подробности и `internal/outbox/outbox.go`.

## V022 — development_plan.md 5.4, реальные шаблоны

`policy.policy_template` получила 6323 уникальных реальных SMS-паттернов для одного demo-партнёра (`demo_partner`) — раньше был ровно один пример-строка (V013). Источник — файл с примерами реальных шаблонов, предоставленный пользователем сессии (не выдуман, не сгенерирован). Категория (`SERVICE`/`TRANSACTION`/`ADVERTISING`) на исходных данных не размечена — распределена случайно с детерминированным seed, по прямому согласованию: данные для тестов/демо, не production-классификация трафика. Те же 6323 строк продублированы в `config.config_outbox` (`entity_type='policy_template'`, `config_version_id=NULL`) — тем же путём, каким `configuration-service.CreateImmutableVersionAndOutbox` реально пишет туда для этого `entity_type` (см. `internal/store/store.go` `skipsConfigVersionsTable`), так что `config-event-publisher`'s `poll_outbox` видит их как обычные неопубликованные записи. Тарифы (`config_schemas/examples/billing_tariff.valid.json`) обновлены реальными ставками того же партнёра (`SERVICE`/`TRANSACTION`=94, `ADVERTISING`=350, `UNTEMPLATED`=3500, `BLOCKED`=94 сум/сегмент, UZS) — `billing-service`'s `TariffResolverTest` синхронизирован под новые числа.

**Не покрыто этой миграцией (честно, не молчаливый пробел):** ежемесячная плата за alphaname/short number (4 млн сум/мес) и пакет сервисных SMS (40000 частей за 2 млн сум) — это не per-segment тариф, а два новых вида биллинга (periodic recurring charge и prepaid balance package), которых `BillingAccountState`/`TariffResolver` сейчас не поддерживают вообще. Требует отдельного проектирования (новые поля аккаунта, периодический billing job, package-balance tracking), не точечного изменения тарифной таблицы — не начато в этом срезе. (**Закрыто в отдельном шаге** — см. `services/billing-service/README.md` "Recurring charges", `RecurringCharges`/`RecurringBillingJob`.)

## V023 — development_plan.md 5.5, operator_id cross-artifact mismatch

Найдено при работе над "реальными connection-профилями операторов": `routing.number_range` (V011) сеялась с `operator_id` без суффикса (`beeline`/`ucell`/`uzmobile`), но `config_schemas/examples/{operator,routing_table,number_range}.valid.json` и дефолтный `OPERATOR_ID` обоих connector-сервисов (`operator-smpp-session-manager`, `operator-http-gateway`) используют суффикс `_uz` (`beeline_uz` и т.д.) — 5 мест против 2. `routing-service` резолвит route по точному совпадению `operator_id` (`RouteTableSnapshot::for_operator`, `HashMap`-lookup, не fuzzy) — реальное сообщение с `resolved_operator_id='beeline'` от destination-resolution-service не находило бы маршрут в `routing_table.valid.json` (ключ `'beeline_uz'`): `RoutingError::UnknownOperator` на каждом сообщении, для всех трёх операторов, не гипотетический edge case. V023 нормализует на `_uz` (UPDATE, не переписывание V011 задним числом — та же дисциплина, что у остальных миграций этой сессии). `services/destination-resolution-service/data/number_range_snapshot.json` (тот же контент, что и V011, продублированный для локального теста без Postgres) обновлён тем же коммитом.

## V024 — luminous-hugging-charm.md Ф2, sender_id в policy_template

`policy.policy_template` (V013) скоупилась на `partner_id` (+опц. `operator_id`), но не на `sender_id` — хотя `senders[]` уже существует как данные внутри `partner.schema.json` (у партнёра может быть несколько alphaname/short-number отправителей), не было способа завести шаблон, специфичный для одного sender'а — он безусловно применялся ко всем. `sender_id` — nullable, по той же схеме, что уже существующий `operator_id` (`NULL` = применяется ко всем отправителям партнёра). Ссылочная целостность (sender_id реально принадлежит partner_id) — не FK на Postgres-таблицу (реестра отправителей как отдельной таблицы сознательно нет — второй источник истины к `partner.schema.json`), проверяется в `policy-service` через Redis-проекцию `sender_id -> partner_id` от `config-cache-projector`. Разблокирует sender-scoped `template-management-service` (Фаза 4).

## V025/V026 — luminous-hugging-charm.md Ф0, Identity/RBAC/Audit

`iam.*` — прямая замена заглушки `V017__backoffice_stub.sql` (`backoffice.users`, никем не читалась в коде). Роли/права/назначения для backoffice-персонала + пустой заранее заведённый заготовок под партнёрский портал (`iam.partner_portal_users`/`partner_portal_role_assignments`, Фаза 3). Полный разбор — `services/iam-service/README.md`.

## V027 — luminous-hugging-charm.md Ф1, живой выпуск/ротация partner credentials

`credentials.issued_secrets`/`credentials.rotation_audit` — история того, что реально было записано в Vault (сам секрет — только в Vault, никогда в Postgres). До этой фазы `partners/*` Vault-пути, на которые уже ссылались `credential_ref` в `partner.schema.json`, не содержали в реальном Vault вообще никакого значения — Terraform (`infra/terraform/vault-secrets.tf`) сеет только пять платформенных секретов (postgresql/redis×3/clickhouse), не `partners/*`. Полный разбор, включая порядок операций (Vault пишется ДО Postgres) — `services/credential-issuer-service/README.md`.

## V028 — luminous-hugging-charm.md Ф7, инцидент-менеджмент

`incident.incidents`/`incident.incident_notes` — группировка связанных `control.execution_control_audit` override-записей (V016) в именованный, отслеживаемый инцидент с таймлайном и постмортемом; до этой фазы override-и писались как сырые строки без концепции группировки вообще. `control.execution_control_audit` получает nullable `incident_id` (плюс частичный индекс `WHERE incident_id IS NOT NULL` под таймлайн-запрос) — **намеренно без FK через границу схем**: `incident.incidents` принадлежит новому `incident-service`, `control.execution_control_audit` — `execution-control-service`, это две независимо разворачиваемые схемы с разными владельцами, тот же контраст, что уже виден в кодовой базе между `iam.staff_role_assignments.role_id` (FK на `iam.roles.id` — тот же владелец схемы) и `billing.reconciliation_audit`/`messaging.replay_audit` (ни то ни другое не несёт FK за пределы собственной таблицы). Постмортем обязателен при закрытии инцидента — валидируется в `incident-service` (`codes.InvalidArgument` при пустом `postmortem_notes`), не CHECK-ограничением, симметрично тому, как `execution-control-service` валидирует `admission_rate` в коде до того, как запрос доходит до БД. Полный разбор — `services/incident-service/README.md`.

## V029 — luminous-hugging-charm.md Ф11, sandbox mode

`messaging.message_read_model` получает `sandbox BOOLEAN NOT NULL DEFAULT false`. Без неё sandbox-сообщения (dry-run отправка через `X-Sandbox: true`, не тарифицируется, не уходит реальному оператору) в backoffice-ui/partner-api-отчётах и ClickHouse неотличимы от настоящего трафика — "почему за это сообщение никто не списал денег и не было реальной отправки" превращается в загадку при разборе инцидента. `lifecycle-writer` заполняет колонку напрямую из `IncomingMessage.sandbox` (единственный источник этого флага — устанавливается один раз на входе в pipeline, `partner-rest-receiver`) при создании строки read model из `incoming.messages` — не через `message.lifecycle`/`message-state-resolver`: тот флаг уже известен на этом, более раннем шаге, не меняется дальше по ходу пайплайна, и `incoming.messages` в любом случае приходит раньше первого `stage.completed`.

## V030 — luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экраны 32/23

`category`/`ctn` добавлены в `config_versions_entity_type_check`. Категории сообщений (SERVICE/TRANSACTION/ADVERTISING/UNTEMPLATED/BLOCKED) раньше жили только как соглашение в документации (`BACKOFFICE_DESIGN_SPEC.md` §1.4) — не proto-enum, не CHECK-enum, не отдельная таблица; `policy_template.category`/`billing_tariff.price_per_segment` уже были свободной строкой (проверено кодом до миграции, не предположение). `category` становится управляемой config-версионируемой сущностью (name/regex/count_in_cdr) — слой метаданных поверх уже свободного поля, **не** новая валидация в `policy-service`/`billing-service` (они продолжают принимать любую строку). Существующие 5 значений засеяны как первые активные версии, чтобы ничего не сломалось. `ctn` — новая сущность для офлайн телеком-биллинга, привязка `(partner_id, category) -> ctn/service_name`; сам CDR-экспорт (генерация файла для сверки с оператором) спланирован в `BACKOFFICE_ROADMAP.md`, не реализован здесь.

## V031 — luminous-hugging-charm.md, BACKOFFICE_DESIGN_SPEC.md Экран 33

`iam.staff_accounts` — реальный Keycloak не развёрнут нигде в этом репозитории (проверено кодом до миграции: `LoginView.vue` принимает вставленный JWT в textarea, ни одного HTTP-раунд-трипа при логине; `iam.staff_role_assignments.external_id` — свободная строка без FK, ни одной таблицы идентичности для staff, в отличие от `iam.partner_portal_users`, V025). Пользователь явно отклонил LDAP, реальный Keycloak — отдельная, сильно более поздняя задача. До этого — полноценный локальный логин/пароль. `external_id = username` (нет Keycloak `sub`, взять неоткуда). Backfill заводит заглушки для уже существующих `staff_role_assignments.external_id` (`active=false`, заведомо невалидный bcrypt-хеш) перед добавлением FK — не ломает уже выданные роли, администратор должен явно завести пароль через новый экран, чтобы такой аккаунт снова заработал.

## Найдено только на этапе реального DDL (не было видно на уровне концептуальной спеки)

Ради этого и стоило спускаться до миграций, а не оставаться на уровне таблиц-в-markdown:

1. **`message_lifecycle_history`: суточные партиции vs почасовые.** `data_infrastructure_spec.md` §1.4 говорил «суточные», `capacity_model.md` §6.3 явно рекомендовал почасовые (суточная партиция при 27–40к строк/с слишком велика для vacuum/индекса). Разные документы противоречили друг другу, никто не заметил до того, как понадобилось написать `PARTITION BY RANGE` руками. Исправлено — почасовые, в обоих документах.

2. **`config.config_outbox.config_version_id` не может быть `NOT NULL FK`.** `policy.policy_template` и `policy.subscriber_consent` пишут outbox-записи, не имея соответствующей строки в `config_versions` (у них собственные таблицы-источники). Жёсткий FK сломал бы вставку для этих двух entity_type в первой же реальной транзакции. Исправлено — FK nullable, с комментарием почему.

3. **`billing_ledger`: "source_charge_id только для compensating" было текстовым примечанием, не инвариантом.** На DDL это стало двумя CHECK-ограничениями (`compensating` требует `source_charge_id`, `charge` запрещает) — теперь невозможно вставить некорректную запись, а не просто "не рекомендуется".

4. **`policy_template`: ничего не мешало создать шаблон с категорией `BLOCKED` или `UNTEMPLATED`** — зарезервированными значениями, которые Policy присваивает сама во время выполнения, не значениями из реального шаблона. Добавлен CHECK, запрещающий это на уровне БД.

5. **Баг в функции обслуживания партиций**, найденный только прогоном на реальных данных: `WHERE relname LIKE 'message_lifecycle_history_%'` совпадал не только с самими партициями, но и с их индексами (`..._pkey`, `..._message_id_idx`, автоматически создаваемыми PostgreSQL для каждой партиции) — счётчик удалённых партиций был завышен (3 вместо 1 в тесте), хотя видимых ошибок не возникало (индексы каскадно удалялись вместе с таблицей, повторные `DROP TABLE IF EXISTS` по их именам молча no-op'али). Исправлено фильтром `relkind IN ('r','p')`. Без реального прогона это осталось бы незамеченным — на бумаге функция выглядела корректно.

## Чего сознательно нет в этой версии

* `backoffice.users` — минимальная заглушка, полная RBAC-модель не специфицируется (`data_infrastructure_spec.md` §1.11).
* Партиционирование не завязано на `pg_partman` — портируемость важнее, обслуживание через обычные SQL-функции, вызываемые внешним планировщиком (k8s CronJob и т.п.).
* Retention для `dlr_correlation` (48ч по умолчанию) — консервативная оценка, требует уточнения по реальным SLA конкретных операторов Узбекистана.
* `routing.number_range` содержит 9 подтверждённых в чате (проверенных на реальных номерах) префиксов + 4 дополнительных, добавленных по общему знанию нумерации Узбекистана (development_plan.md 5.1) — только для трёх операторов, у которых в платформе реально есть connection-профиль (Beeline/Ucell/Uzmobile); не проверены на реальных номерах, не полный справочник по стране, другие узбекистанские операторы (Perfectum как отдельный бренд, Mobiuz и т.д.) сознательно не добавлены — для них нет connection-профиля, добавление их номерных диапазонов резолвило бы сообщение туда, куда физически некуда доставить.
