-- V037__partition_maintenance_security_definer.sql
--
-- V036__per_service_postgresql_grants.sql gave mpp_messaging/mpp_dlr only
-- USAGE + EXECUTE on the four partition-maintenance functions below
-- (messaging.create_lifecycle_history_partition,
-- messaging.drop_old_lifecycle_history_partitions,
-- dlr.create_correlation_partition, dlr.drop_old_correlation_partitions),
-- not CREATE/DROP on the schema itself. Those functions are plain
-- LANGUAGE plpgsql with no SECURITY clause, which defaults to SECURITY
-- INVOKER — the CREATE TABLE/DROP TABLE inside them runs with the
-- CALLING role's privileges, not the function owner's. Found live:
-- lifecycle-writer (mpp_messaging) failed every single flush with
-- "permission denied for schema messaging" the moment it needed a new
-- hourly partition, since EXECUTE on the function was never going to be
-- enough on its own.
--
-- Fix: SECURITY DEFINER, so these four functions run with the owning
-- role's (whoever applies migrations, same role that owns the schemas)
-- privileges regardless of caller — the standard Postgres idiom for
-- "let a low-privilege role perform one specific elevated action safely"
-- without widening mpp_messaging/mpp_dlr's own grants to blanket
-- CREATE/DROP on the whole schema (which would partially defeat the point
-- of V036). search_path pinned explicitly per Postgres's own
-- SECURITY DEFINER advice (defends against a search_path set by the
-- caller redirecting unqualified references) — moot here since every
-- table/schema reference in these bodies is already fully qualified, but
-- cheap insurance for anyone editing them later without re-reading this.

ALTER FUNCTION messaging.create_lifecycle_history_partition(timestamptz) SECURITY DEFINER SET search_path = messaging, pg_temp;
ALTER FUNCTION messaging.drop_old_lifecycle_history_partitions(int) SECURITY DEFINER SET search_path = messaging, pg_temp;
ALTER FUNCTION dlr.create_correlation_partition(timestamptz) SECURITY DEFINER SET search_path = dlr, pg_temp;
ALTER FUNCTION dlr.drop_old_correlation_partitions(int) SECURITY DEFINER SET search_path = dlr, pg_temp;
