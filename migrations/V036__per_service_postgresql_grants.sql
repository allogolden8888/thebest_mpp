-- V036__per_service_postgresql_grants.sql
-- BACKOFFICE_ROADMAP.md Production Readiness Review P1 "Security"
-- ("один Postgres-юзер на все сервисы"). Until this migration every one of
-- the ~17 services with a direct PostgreSQL connection used the same
-- `mpp_app` role with unrestricted DML on the whole `mpp` database — a
-- leaked/compromised credential for ANY one service (or a bug in any one
-- service's SQL) could read or write every other domain's data
-- (billing ledger, IAM roles, credential secrets, ...).
--
-- Ownership mapping below was derived by reading each service's actual
-- SQL (`grep -rn "FROM \|INTO \|UPDATE " services/*/...`), not guessed from
-- service names — see the investigation notes in this session. Grouping is
-- DOMAIN-based, not strictly one-role-per-service: a handful of services are
-- two tightly-coupled halves of the same table's lifecycle (dlr-manager
-- reads/partition-maintains what dlr-correlation-writer writes;
-- replay-service mutates the same messaging.dlq_record row lifecycle-writer
-- creates) and sharing one role for that pair is an honest reflection of
-- their coupling, not a shortcut. Genuine trust-boundary differences (a
-- read-only admin aggregator vs a schema owner; an external partner-facing
-- reader vs an internal writer) always get their OWN role even when the
-- grant set would otherwise be a subset of another role's.
--
-- Roles referenced here (mpp_*) must already exist before this migration
-- runs:
--   - production: created by Terraform (`yandex_mdb_postgresql_user.service`,
--     infra/terraform/postgresql.tf) as part of `terraform apply` — the same
--     pre-existing dependency ordering as "the cluster/database must exist
--     before ANY migration runs".
--   - local docker-compose: created by
--     infra/docker/postgres-init/01-create-service-roles.sql, which the
--     official postgres image runs once from docker-entrypoint-initdb.d on
--     first container start (before migrate.py is ever invoked).
--
-- The old shared `mpp_app` role is NOT dropped here — services keep working
-- unmodified until each one's compose/k8s wiring is switched to its new
-- per-domain credential (infra/docker/docker-compose.yml +
-- k8s/generate_manifests.py SECRET_DEPENDENCIES, done in the same change as
-- this migration). Dropping `mpp_app` is a separate, deliberate follow-up
-- once every consumer is confirmed migrated — see BACKOFFICE_ROADMAP.md.

-- config: configuration-service (config_versions + config_outbox, owner) и
-- config-event-publisher (claims/publishes config_outbox, reads
-- config_versions for status join) — two halves of the same outbox
-- producer/consumer pipeline, same trust boundary.
GRANT USAGE ON SCHEMA config TO mpp_config;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA config TO mpp_config;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA config TO mpp_config;

-- billing write: billing-ledger-writer (billing_ledger, owner) и
-- billing-reconciliation (reads billing_ledger for balance recompute,
-- writes reconciliation_audit) — both mutate billing state.
GRANT USAGE ON SCHEMA billing TO mpp_billing_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA billing TO mpp_billing_write;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA billing TO mpp_billing_write;

-- billing read-only: billing-self-service-api (partner-facing self-service
-- statement view) — read-only на весь billing-домен ей не нужен, только
-- billing_ledger, но домен здесь — одна таблица на сегодня, так что грант на
-- уровне таблицы и на уровне схемы совпадают; таблица явно перечислена ниже,
-- чтобы новая таблица в billing-схеме не стала read-visible этому read-only
-- потребителю неявно.
GRANT USAGE ON SCHEMA billing TO mpp_billing_read;
GRANT SELECT ON billing.billing_ledger TO mpp_billing_read;

-- dlr: dlr-correlation-writer (INSERT на submit) и dlr-manager (SELECT на
-- lookup + вызывает create_correlation_partition/drop_old_correlation_partitions
-- для обслуживания партиций) — общий жизненный цикл одной таблицы.
GRANT USAGE ON SCHEMA dlr TO mpp_dlr;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dlr TO mpp_dlr;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dlr TO mpp_dlr;
GRANT EXECUTE ON FUNCTION dlr.create_correlation_partition(timestamptz) TO mpp_dlr;
GRANT EXECUTE ON FUNCTION dlr.drop_old_correlation_partitions(int) TO mpp_dlr;

-- policy write: template-management-service — владелец policy.policy_template
-- + пишет/читает config.config_outbox напрямую (bulk-операции публикуют
-- config.changes через тот же outbox, что configuration-service, но не
-- проходят через config.config_versions — см. configuration-service/
-- internal/store/store.go skipsConfigVersionsTable). Табличный, не
-- схемный грант на config — этому сервису не нужен доступ к
-- config.config_versions (владеет mpp_config/mpp_credentials по отдельности).
GRANT USAGE ON SCHEMA policy TO mpp_policy_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA policy TO mpp_policy_write;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA policy TO mpp_policy_write;
GRANT USAGE ON SCHEMA config TO mpp_policy_write;
GRANT SELECT, INSERT, UPDATE, DELETE ON config.config_outbox TO mpp_policy_write;
GRANT USAGE, SELECT ON SEQUENCE config.config_outbox_id_seq TO mpp_policy_write;

-- policy read-only: consent-cache-projector — startup resync читает ВСЕ
-- строки policy.subscriber_consent (RESYNC_ON_START), никогда не пишет её
-- (см. investigation: ни один сервис в кодовой базе сегодня не пишет эту
-- таблицу вне тестовых фикстур — честно задокументированный, отдельный от
-- этой миграции пробел, не что-то, что эта миграция должна выдумывать
-- владельца для).
GRANT USAGE ON SCHEMA policy TO mpp_policy_read;
GRANT SELECT ON policy.subscriber_consent TO mpp_policy_read;

-- reconciliation: delivery-reconciliation-service — единственный писатель,
-- касается только этой схемы (reconciliation_cases, early_evidence).
GRANT USAGE ON SCHEMA reconciliation TO mpp_reconciliation;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA reconciliation TO mpp_reconciliation;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA reconciliation TO mpp_reconciliation;

-- control: execution-control-service — единственный писатель
-- control.execution_control_audit, касается только этой схемы (incident_id
-- FK-подобное поле передаётся сквозным образом, без валидации в
-- incident.incidents — см. ApplyOverride комментарий в grpcserver/server.go).
GRANT USAGE ON SCHEMA control TO mpp_control;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control TO mpp_control;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control TO mpp_control;

-- iam: iam-service — единственный писатель, единственная схема (roles,
-- permissions, staff/partner-portal accounts, identity_audit).
GRANT USAGE ON SCHEMA iam TO mpp_iam;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA iam TO mpp_iam;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA iam TO mpp_iam;

-- incident: incident-service — владелец incident.* + настоящее cross-schema
-- чтение control.execution_control_audit (TimelineForIncident,
-- internal/store/store.go) для объединённого таймлайна инцидента.
GRANT USAGE ON SCHEMA incident TO mpp_incident;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA incident TO mpp_incident;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA incident TO mpp_incident;
GRANT USAGE ON SCHEMA control TO mpp_incident;
GRANT SELECT ON control.execution_control_audit TO mpp_incident;

-- credentials: credential-issuer-service — владелец credentials.* + реальное
-- cross-schema чтение config.config_versions (LookupCredentialRef,
-- internal/store/store.go) для резолва активной версии партнёрского
-- credential_ref.
GRANT USAGE ON SCHEMA credentials TO mpp_credentials;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA credentials TO mpp_credentials;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA credentials TO mpp_credentials;
GRANT USAGE ON SCHEMA config TO mpp_credentials;
GRANT SELECT ON config.config_versions TO mpp_credentials;

-- support: chat-service — единственный писатель, единственная схема
-- (support.chat_messages, "принадлежит целиком chat-service" — package doc).
GRANT USAGE ON SCHEMA support TO mpp_support;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA support TO mpp_support;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA support TO mpp_support;

-- messaging write: lifecycle-writer (owner: message_lifecycle_history,
-- message_read_model, создаёт строки dlq_record) и replay-service
-- (UPDATE dlq_record.replay_status на replay-командах, INSERT
-- messaging.replay_audit) — общий жизненный цикл одной и той же
-- dlq_record-строки от создания до replay, тот же класс группировки, что
-- dlr выше.
GRANT USAGE ON SCHEMA messaging TO mpp_messaging;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA messaging TO mpp_messaging;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA messaging TO mpp_messaging;
GRANT EXECUTE ON FUNCTION messaging.create_lifecycle_history_partition(timestamptz) TO mpp_messaging;
GRANT EXECUTE ON FUNCTION messaging.drop_old_lifecycle_history_partitions(int) TO mpp_messaging;

-- messaging read-only: partner-api — партнёрский (внешний, за OIDC)
-- read-only просмотр СВОИХ сообщений (per-partner scoping в SQL, см.
-- internal/store/postgres.go). Разный доверительный периметр от
-- backoffice-api (внутренний, кросс-партнёрский) и от mpp_messaging
-- (внутренний писатель) — отдельная роль даже притом, что грант — строгое
-- подмножество обеих.
GRANT USAGE ON SCHEMA messaging TO mpp_messaging_read;
GRANT SELECT ON messaging.message_read_model, messaging.message_lifecycle_history TO mpp_messaging_read;

-- backoffice read-only aggregator: backoffice-api — административный BFF,
-- сознательно читает чужие схемы напрямую вместо gRPC ("тот же принцип
-- прямого чтения чужих схем", package doc postgres.go/billing.go), но
-- ТОЛЬКО читает — ни одного INSERT/UPDATE/DELETE не найдено ни в одном из
-- store-файлов этого сервиса. Один read-only грант через все шесть схем,
-- по перечисленным таблицам, а не GRANT ALL TABLES IN SCHEMA — новая таблица
-- в любой из этих схем не должна становиться read-visible админке неявно.
GRANT USAGE ON SCHEMA messaging, reconciliation, control, billing, iam, dlr TO mpp_backoffice_read;
GRANT SELECT ON
    messaging.dlq_record,
    messaging.replay_audit,
    messaging.message_read_model,
    messaging.message_lifecycle_history,
    reconciliation.reconciliation_cases,
    control.execution_control_audit,
    billing.billing_ledger,
    billing.reconciliation_audit,
    iam.identity_audit,
    dlr.dlr_correlation
TO mpp_backoffice_read;
