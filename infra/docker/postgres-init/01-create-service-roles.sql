-- 01-create-service-roles.sql — local docker-compose analogue of what
-- Terraform (`yandex_mdb_postgresql_user.service`,
-- infra/terraform/postgresql.tf) provisions in production: one login role
-- per domain, so migrations/V036__per_service_postgresql_grants.sql has
-- something to GRANT to.
--
-- The official postgres image runs every *.sql/*.sh under
-- /docker-entrypoint-initdb.d/ exactly once, only when the data directory is
-- empty (first container start on a fresh volume) — see
-- infra/docker/docker-compose.yml postgres service (mounts this directory).
-- Passwords here are fixed local-dev values (same "mpp_local_dev" convention
-- already used for postgres/redis/clickhouse elsewhere in this compose
-- file) — never used outside this disposable local stack. Production
-- passwords come from TF_VAR_postgresql_service_passwords (Vault-managed,
-- see vault-secrets.tf), never hardcoded.
--
-- Role list mirrors local.postgresql_service_users in
-- infra/terraform/postgresql.tf exactly — keep both in sync by hand (no
-- codegen link between Terraform and this file today, same as the rest of
-- this docker-compose stack being hand-maintained relative to k8s/generate_manifests.py).
DO $$
DECLARE
    role_name TEXT;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'mpp_config',
        'mpp_billing_write',
        'mpp_billing_read',
        'mpp_dlr',
        'mpp_policy_write',
        'mpp_policy_read',
        'mpp_reconciliation',
        'mpp_control',
        'mpp_iam',
        'mpp_incident',
        'mpp_credentials',
        'mpp_support',
        'mpp_messaging',
        'mpp_messaging_read',
        'mpp_backoffice_read'
    ]
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', role_name, 'mpp_local_dev');
        END IF;
    END LOOP;
END
$$;
