-- production_grants.sql — real per-table ClickHouse GRANTs for the
-- per-service users created by infra/terraform/clickhouse.tf
-- (yandex_mdb_clickhouse_user.service). Companion to
-- migrations/V036__per_service_postgresql_grants.sql, but NOT run through
-- infra/migrations/migrate.py — that runner is psql/PostgreSQL-specific
-- (advisory locks, mpp_migrations.schema_history, checksum verification)
-- and this repository has no equivalent versioned/checksummed runner for
-- ClickHouse. This file is intentionally a lighter-weight, manually-applied
-- bootstrap step — it does not claim migrate.py's guarantees (no history
-- table, no lock, no drift detection). If ClickHouse schema changes grow
-- beyond "a handful of GRANTs plus two self-provisioning CREATE TABLE IF NOT
-- EXISTS calls", building a real versioned runner is the honest next step,
-- not pretending this file already is one.
--
-- WHY GRANTs live here instead of the Terraform `permission{}` block: see
-- clickhouse.tf comment above `yandex_mdb_clickhouse_user.service` — the
-- Yandex provider's ClickHouse user resource only exposes database-level
-- `permission{database_name}` (checked via `terraform providers schema
-- -json` on the installed 0.218.0 provider), and analytics-writer /
-- pdu-log-writer deliberately share ONE database ("analytics") for two
-- different tables — database-level grants can't separate them, so the
-- actual isolation has to be expressed as native ClickHouse per-table GRANT
-- statements, run here.
--
-- Apply after `terraform apply` has created the users (they must already
-- exist — this file only grants, never creates users):
--
--   clickhouse-client --host <CLICKHOUSE_HOST> --port 9440 --secure \
--     --user admin --password "$CLICKHOUSE_ADMIN_PASSWORD" \
--     --multiquery < infra/clickhouse/production_grants.sql
--
-- Idempotent: GRANT in ClickHouse does not require the target table to
-- exist yet (verified live against ClickHouse 24.8 — granting SELECT on a
-- not-yet-created table succeeds, unlike PostgreSQL) and re-running an
-- identical GRANT is a no-op, so this is safe to re-apply after every
-- `terraform apply` that touches these users.

-- analytics-writer: owns analytics.stage_events + the materialized view
-- that aggregates it (stage_events_hourly_mv). CREATE TABLE is granted at
-- the DATABASE level — see clickhouse.tf comment for why this is an
-- honestly-disclosed, not-fully-closed gap (EnsureSchema self-provisions
-- DDL at startup; ClickHouse has no "CREATE this one not-yet-existing table
-- name only" grant).
GRANT SELECT, INSERT, ALTER ON analytics.stage_events TO analytics_writer;
GRANT SELECT, INSERT, ALTER ON analytics.stage_events_hourly_mv TO analytics_writer;
GRANT CREATE TABLE ON analytics.* TO analytics_writer;

-- pdu-log-writer: owns analytics.operator_pdu_log only. No SELECT/INSERT on
-- stage_events — verified live: pdu_log_writer gets ACCESS_DENIED reading
-- analytics.stage_events.
GRANT SELECT, INSERT, ALTER ON analytics.operator_pdu_log TO pdu_log_writer;
GRANT CREATE TABLE ON analytics.* TO pdu_log_writer;

-- backoffice-api: internal admin BFF, PDU-log read endpoint (BACKOFFICE_DESIGN_SPEC.md
-- Экраны 38-40) + stage_events-based reporting/timeline. Read-only on both
-- tables it actually queries (internal/store/clickhouse.go) — the readonly=1
-- session setting on this Terraform-managed user is defense-in-depth on top
-- of these table-level grants, not a substitute for them.
GRANT SELECT ON analytics.stage_events TO analytics_reader;
GRANT SELECT ON analytics.operator_pdu_log TO analytics_reader;

-- partner-api: external (OIDC-authenticated) per-partner reporting. Reads
-- ONLY stage_events (internal/store/clickhouse.go) — operator_pdu_log is
-- internal-admin-only data (raw per-PDU operator protocol detail), never
-- exposed to partners. Verified live: partner_api_reader gets ACCESS_DENIED
-- reading analytics.operator_pdu_log.
GRANT SELECT ON analytics.stage_events TO partner_api_reader;
