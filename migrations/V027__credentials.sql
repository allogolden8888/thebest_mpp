-- V027__credentials.sql
-- luminous-hugging-charm.md Фаза 1 — живой выпуск/ротация partner
-- credentials. partner.schema.json's applications[].auth.credential_ref
-- уже существует как a Vault path REFERENCE (data_infrastructure_spec.md,
-- "конфиг не хранит credential в открытом виде") — до этой фазы ни один
-- реальный процесс не писал значение ПО этому пути (Terraform сеет только
-- postgresql/redis/clickhouse в Vault, не partners/*, см.
-- infra/terraform/vault-secrets.tf). Эта схема — история "что было
-- реально записано", не сам секрет (тот живёт только в Vault).

CREATE SCHEMA IF NOT EXISTS credentials;

CREATE TABLE credentials.issued_secrets (
    id              BIGSERIAL PRIMARY KEY,
    partner_id      TEXT NOT NULL,
    application_id  TEXT NOT NULL,
    credential_ref  TEXT NOT NULL,
    secret_version  INT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at      TIMESTAMPTZ NULL,
    issued_by       TEXT NOT NULL
);

-- Ровно одна активная версия на (partner_id, application_id) одновременно —
-- ротация атомарно отзывает предыдущую и вставляет новую в одной
-- транзакции (тот же принцип, что iam.staff_role_assignments_active_unique,
-- V025).
CREATE UNIQUE INDEX issued_secrets_active_unique
    ON credentials.issued_secrets (partner_id, application_id) WHERE status = 'active';

CREATE INDEX issued_secrets_partner_app_idx
    ON credentials.issued_secrets (partner_id, application_id, secret_version);

-- Append-only, тот же формат, что iam.identity_audit (V025).
CREATE TABLE credentials.rotation_audit (
    id         BIGSERIAL PRIMARY KEY,
    actor      TEXT NOT NULL,
    action     TEXT NOT NULL CHECK (action IN ('ISSUED', 'ROTATED')),
    target     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX rotation_audit_created_at_idx ON credentials.rotation_audit (created_at);
