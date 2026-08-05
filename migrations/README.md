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
```

Применить локально:

```bash
for f in V*.sql; do psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"; done
```

## Что реально проверено (не только «написано и похоже на правду»)

1. **Все 17 миграций применяются с нуля без ошибок** на чистой PostgreSQL 17.
2. **Все 9 реальных номеров из чата** (`998901331835` и остальные) корректно резолвятся в правильного оператора через `routing.number_range` — `SELECT ... WHERE msisdn BETWEEN range_start AND range_end`, 9/9 совпадений.
3. **CHECK-ограничения реально блокируют некорректные данные**, не только написаны: compensating billing-запись без `source_charge_id` — отклонена; шаблон с зарезервированной категорией `BLOCKED` — отклонён.
4. **Идемпотентность billing_ledger** — повторная вставка с тем же `charge_id` через `ON CONFLICT DO NOTHING` реально не создаёт вторую строку.
5. **Партиционирование и retention-функции** — создание почасовых партиций, автоматическая маршрутизация вставки в нужную партицию, drop партиций старше заданного окна — всё выполнено вручную на реальных данных, не просто прочитано глазами.

## V018 — добавлена после CODE_REVIEW.md (config-event-publisher)

`config.config_outbox` получила `claimed_at`/`attempts`/`last_error` — `poll_outbox` (config-event-publisher) раньше не координировал несколько реплик (не было `SELECT ... FOR UPDATE SKIP LOCKED`) и мог опрашивать одну и ту же не-публикуемую (poison) строку вечно, вытесняя реальные pending-строки из `ORDER BY created_at ASC LIMIT N`. См. `services/config-event-publisher/README.md` за подробности и `internal/outbox/outbox.go`.

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
