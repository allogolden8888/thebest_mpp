-- 01-create-service-users-and-grants.sql — local docker-compose analogue of
-- infra/terraform/clickhouse.tf's yandex_mdb_clickhouse_user.service +
-- infra/clickhouse/production_grants.sql, combined into one file because
-- there's no separate "migration runner" step in the local dev flow the way
-- migrate.py exists for Postgres.
--
-- The official clickhouse-server image runs every *.sql under
-- /docker-entrypoint-initdb.d/ once, after the server is up, on first
-- container start against an empty data directory — see
-- infra/docker/docker-compose.yml clickhouse service (mounts this
-- directory) and CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 (lets the bootstrap
-- "mpp" user run CREATE USER/GRANT here).
--
-- Passwords are fixed local-dev values (same "mpp_local_dev" convention
-- used everywhere else in this compose file) — never used outside this
-- disposable local stack. Grants mirror production_grants.sql exactly
-- (verified live against ClickHouse 24.8 in this session — see this
-- session's report for the negative-permission test transcript).
CREATE USER IF NOT EXISTS analytics_writer IDENTIFIED WITH plaintext_password BY 'mpp_local_dev';
CREATE USER IF NOT EXISTS pdu_log_writer IDENTIFIED WITH plaintext_password BY 'mpp_local_dev';
CREATE USER IF NOT EXISTS analytics_reader IDENTIFIED WITH plaintext_password BY 'mpp_local_dev' SETTINGS readonly=1;
CREATE USER IF NOT EXISTS partner_api_reader IDENTIFIED WITH plaintext_password BY 'mpp_local_dev' SETTINGS readonly=1;

GRANT SELECT, INSERT, ALTER ON analytics.stage_events TO analytics_writer;
GRANT SELECT, INSERT, ALTER ON analytics.stage_events_hourly_mv TO analytics_writer;
GRANT CREATE TABLE ON analytics.* TO analytics_writer;
-- EnsureSchema (services/analytics-writer/internal/store/store.go) always
-- runs `CREATE DATABASE IF NOT EXISTS analytics` unconditionally at startup
-- — ClickHouse requires CREATE DATABASE to even attempt this statement, it
-- doesn't skip the privilege check just because the database already
-- exists (created by CLICKHOUSE_DB env var at container init). Without
-- this grant analytics-writer fails to start on every restart, not just
-- the first one.
GRANT CREATE DATABASE ON analytics.* TO analytics_writer;

GRANT SELECT, INSERT, ALTER ON analytics.operator_pdu_log TO pdu_log_writer;
GRANT CREATE TABLE ON analytics.* TO pdu_log_writer;
-- Same unconditional CREATE DATABASE IF NOT EXISTS at startup, see comment above.
GRANT CREATE DATABASE ON analytics.* TO pdu_log_writer;

GRANT SELECT ON analytics.stage_events TO analytics_reader;
GRANT SELECT ON analytics.operator_pdu_log TO analytics_reader;

GRANT SELECT ON analytics.stage_events TO partner_api_reader;
