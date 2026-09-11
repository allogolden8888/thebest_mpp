#!/usr/bin/env python3
"""Versioned PostgreSQL migration runner for MPP.

The runner deliberately has only two runtime dependencies: Python 3 and the
PostgreSQL ``psql`` client.  Migration SQL is executed by one persistent psql
session so a session-level advisory lock protects the complete run, including
the commits between individual migrations.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence


RUNNER_VERSION = "1"
LOCK_KEY = 6_044_496_743_993_734_994
HISTORY_SCHEMA = "mpp_migrations"
HISTORY_TABLE = "schema_history"
MIGRATION_NAME = re.compile(
    r"^V(?P<version>[0-9]{3,})__(?P<description>[a-z0-9][a-z0-9_]*)\.sql$"
)
MANAGED_SCHEMAS = (
    "backoffice",
    "billing",
    "config",
    "control",
    "credentials",
    "dlr",
    "iam",
    "incident",
    "messaging",
    "policy",
    "reconciliation",
    "routing",
    "support",
)


class MigrationError(RuntimeError):
    """A deterministic migration catalog or execution error."""


@dataclasses.dataclass(frozen=True)
class Migration:
    version: int
    description: str
    script_name: str
    checksum: str
    path: Path


def discover_migrations(directory: Path) -> list[Migration]:
    """Load and validate an immutable, contiguous migration catalog."""

    directory = directory.resolve()
    if not directory.is_dir():
        raise MigrationError(f"migration directory does not exist: {directory}")

    migrations: list[Migration] = []
    invalid_names: list[str] = []
    for path in directory.iterdir():
        if not path.is_file() or path.suffix.lower() != ".sql":
            continue
        match = MIGRATION_NAME.fullmatch(path.name)
        if match is None:
            invalid_names.append(path.name)
            continue
        version = int(match.group("version"))
        if version <= 0:
            invalid_names.append(path.name)
            continue
        payload = path.read_bytes()
        if not payload.strip():
            raise MigrationError(f"empty migration is not allowed: {path.name}")
        migrations.append(
            Migration(
                version=version,
                description=match.group("description"),
                script_name=path.name,
                checksum=hashlib.sha256(payload).hexdigest(),
                path=path.resolve(),
            )
        )

    if invalid_names:
        names = ", ".join(sorted(invalid_names))
        raise MigrationError(f"invalid migration filename(s): {names}")
    if not migrations:
        raise MigrationError(f"no migrations found in {directory}")

    migrations.sort(key=lambda item: item.version)
    versions = [item.version for item in migrations]
    duplicate_versions = sorted(
        version for version in set(versions) if versions.count(version) > 1
    )
    if duplicate_versions:
        raise MigrationError(
            "duplicate migration version(s): "
            + ", ".join(f"V{version:03d}" for version in duplicate_versions)
        )

    expected = list(range(1, versions[-1] + 1))
    if versions != expected:
        missing = sorted(set(expected) - set(versions))
        raise MigrationError(
            "migration versions must be contiguous from V001; missing: "
            + ", ".join(f"V{version:03d}" for version in missing)
        )
    return migrations


def sql_literal(value: str) -> str:
    """Quote a trusted catalog value as a PostgreSQL string literal."""

    if "\x00" in value:
        raise MigrationError("NUL is not allowed in a SQL literal")
    return "'" + value.replace("'", "''") + "'"


def psql_include_path(path: Path) -> str:
    r"""Quote a path for psql's \ir meta-command."""

    value = str(path)
    if any(character in value for character in ("\n", "\r", "\x00")):
        raise MigrationError(f"unsupported character in migration path: {path}")
    return "'" + value.replace("'", "''") + "'"


def expected_values(migrations: Sequence[Migration]) -> str:
    return ",\n".join(
        "    ("
        f"{item.version}, {sql_literal(item.description)}, "
        f"{sql_literal(item.script_name)}, {sql_literal(item.checksum)}"
        ")"
        for item in migrations
    )


def bootstrap_sql(lock_timeout_seconds: int, *, allow_untracked: bool = False) -> str:
    managed_schemas = ", ".join(sql_literal(name) for name in MANAGED_SCHEMAS)
    untracked_guard = ""
    if not allow_untracked:
        untracked_guard = f"""
-- A pre-existing MPP schema with no history is unsafe to replay. Existing
-- installations must be baselined explicitly after a schema audit.
DO $mpp$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM {HISTORY_SCHEMA}.{HISTORY_TABLE}
    ) AND EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname IN ({managed_schemas})
    ) THEN
        RAISE EXCEPTION USING
            MESSAGE = 'untracked MPP schema detected',
            HINT = 'verify the existing schema on a restored copy, then use explicit baseline mode';
    END IF;
END
$mpp$;
"""

    return f"""\\set ON_ERROR_STOP on
\\set ECHO errors
SET application_name = 'mpp-schema-migrator';
SET lock_timeout = '{lock_timeout_seconds}s';
SELECT pg_advisory_lock({LOCK_KEY});
SET lock_timeout = '0';

CREATE SCHEMA IF NOT EXISTS {HISTORY_SCHEMA};
CREATE TABLE IF NOT EXISTS {HISTORY_SCHEMA}.{HISTORY_TABLE} (
    version       BIGINT PRIMARY KEY CHECK (version > 0),
    description   TEXT NOT NULL,
    script_name   TEXT NOT NULL UNIQUE,
    checksum      TEXT NOT NULL CHECK (checksum ~ '^[0-9a-f]{{64}}$'),
    installed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    installed_by  TEXT NOT NULL,
    execution_ms  BIGINT NOT NULL CHECK (execution_ms >= 0),
    runner_version TEXT NOT NULL
);

CREATE TEMP TABLE mpp_expected_migrations (
    version      BIGINT PRIMARY KEY,
    description  TEXT NOT NULL,
    script_name  TEXT NOT NULL UNIQUE,
    checksum     TEXT NOT NULL
) ON COMMIT PRESERVE ROWS;
{untracked_guard}
"""


def catalog_validation_sql(migrations: Sequence[Migration]) -> str:
    return f"""
INSERT INTO mpp_expected_migrations (version, description, script_name, checksum)
VALUES
{expected_values(migrations)};

DO $mpp$
DECLARE
    problem TEXT;
BEGIN
    SELECT format(
        'V%s database=(%s,%s) repository=(%s,%s)',
        h.version,
        h.script_name,
        h.checksum,
        coalesce(e.script_name, '<missing>'),
        coalesce(e.checksum, '<missing>')
    )
    INTO problem
    FROM {HISTORY_SCHEMA}.{HISTORY_TABLE} h
    LEFT JOIN mpp_expected_migrations e USING (version)
    WHERE e.version IS NULL
       OR h.script_name <> e.script_name
       OR h.description <> e.description
       OR h.checksum <> e.checksum
    ORDER BY h.version
    LIMIT 1;

    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'migration history drift: %', problem;
    END IF;

    SELECT format('V%s was applied before missing V%s', max_applied, missing_version)
    INTO problem
    FROM (
        SELECT max(version) AS max_applied
        FROM {HISTORY_SCHEMA}.{HISTORY_TABLE}
    ) applied
    CROSS JOIN LATERAL (
        SELECT min(e.version) AS missing_version
        FROM mpp_expected_migrations e
        LEFT JOIN {HISTORY_SCHEMA}.{HISTORY_TABLE} h USING (version)
        WHERE e.version <= applied.max_applied AND h.version IS NULL
    ) gap
    WHERE missing_version IS NOT NULL;

    IF problem IS NOT NULL THEN
        RAISE EXCEPTION 'non-prefix migration history: %', problem;
    END IF;
END
$mpp$;
"""


def apply_sql(migrations: Sequence[Migration]) -> str:
    sections: list[str] = []
    for item in migrations:
        sections.append(
            f"""
SELECT EXISTS (
    SELECT 1 FROM {HISTORY_SCHEMA}.{HISTORY_TABLE} WHERE version = {item.version}
) AS mpp_already_applied \\gset
\\if :mpp_already_applied
\\echo 'skip {item.script_name} (already applied and checksum verified)'
\\else
\\echo 'apply {item.script_name}'
BEGIN;
SELECT clock_timestamp()::text AS mpp_started_at \\gset
\\ir {psql_include_path(item.path)}
INSERT INTO {HISTORY_SCHEMA}.{HISTORY_TABLE} (
    version, description, script_name, checksum,
    installed_by, execution_ms, runner_version
) VALUES (
    {item.version},
    {sql_literal(item.description)},
    {sql_literal(item.script_name)},
    {sql_literal(item.checksum)},
    current_user,
    round(extract(epoch FROM (clock_timestamp() - :'mpp_started_at'::timestamptz)) * 1000),
    {sql_literal(RUNNER_VERSION)}
);
COMMIT;
\\endif
"""
        )
    return "".join(sections)


def verify_complete_sql() -> str:
    return f"""
DO $mpp$
DECLARE
    pending TEXT;
BEGIN
    SELECT string_agg(e.script_name, ', ' ORDER BY e.version)
    INTO pending
    FROM mpp_expected_migrations e
    LEFT JOIN {HISTORY_SCHEMA}.{HISTORY_TABLE} h USING (version)
    WHERE h.version IS NULL;

    IF pending IS NOT NULL THEN
        RAISE EXCEPTION 'pending migrations: %', pending;
    END IF;
END
$mpp$;
\\echo 'migration verification passed: database exactly matches repository catalog'
SELECT pg_advisory_unlock({LOCK_KEY});
"""


def baseline_sql(migrations: Sequence[Migration], baseline_version: int) -> str:
    if baseline_version not in {item.version for item in migrations}:
        raise MigrationError(
            f"baseline V{baseline_version:03d} is not present in the migration catalog"
        )
    managed_schemas = ", ".join(sql_literal(name) for name in MANAGED_SCHEMAS)
    return f"""
DO $mpp$
BEGIN
    IF EXISTS (SELECT 1 FROM {HISTORY_SCHEMA}.{HISTORY_TABLE}) THEN
        RAISE EXCEPTION 'baseline requires an empty migration history';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_namespace WHERE nspname IN ({managed_schemas})
    ) THEN
        RAISE EXCEPTION 'baseline requires an existing MPP schema';
    END IF;
END
$mpp$;

BEGIN;
INSERT INTO {HISTORY_SCHEMA}.{HISTORY_TABLE} (
    version, description, script_name, checksum,
    installed_by, execution_ms, runner_version
)
SELECT
    version, description, script_name, checksum,
    current_user || ' (verified baseline)', 0, {sql_literal(RUNNER_VERSION)}
FROM mpp_expected_migrations
WHERE version <= {baseline_version}
ORDER BY version;
COMMIT;
\\echo 'recorded verified baseline through V{baseline_version:03d}; no migration SQL was executed'
SELECT pg_advisory_unlock({LOCK_KEY});
"""


def build_driver_sql(
    migrations: Sequence[Migration],
    mode: str,
    lock_timeout_seconds: int,
    baseline_version: int | None = None,
) -> str:
    if lock_timeout_seconds <= 0:
        raise MigrationError("lock timeout must be positive")

    if mode == "baseline":
        if baseline_version is None:
            raise MigrationError("baseline mode requires --baseline-version")
        # Baseline has its own existing-schema guard. The normal bootstrap
        # guard intentionally blocks untracked installations.
        bootstrap = bootstrap_sql(lock_timeout_seconds, allow_untracked=True)
    else:
        bootstrap = bootstrap_sql(lock_timeout_seconds)

    driver = bootstrap + catalog_validation_sql(migrations)
    if mode == "apply":
        return driver + apply_sql(migrations) + verify_complete_sql()
    if mode == "verify":
        return driver + verify_complete_sql()
    if mode == "baseline":
        assert baseline_version is not None
        return driver + baseline_sql(migrations, baseline_version)
    raise MigrationError(f"unsupported database mode: {mode}")


def run_psql(database_url: str, driver_sql: str, psql_binary: str) -> None:
    executable = shutil.which(psql_binary)
    if executable is None:
        raise MigrationError(f"psql executable not found: {psql_binary}")

    driver_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", prefix="mpp-migrations-", suffix=".sql", delete=False
        ) as driver:
            driver.write(driver_sql)
            driver_path = Path(driver.name)
        os.chmod(driver_path, 0o600)
        completed = subprocess.run(
            [
                executable,
                "--no-psqlrc",
                "--dbname",
                database_url,
                "--file",
                str(driver_path),
            ],
            check=False,
        )
        if completed.returncode != 0:
            raise MigrationError(
                f"psql failed with exit code {completed.returncode}; database may not be deployed"
            )
    finally:
        if driver_path is not None:
            driver_path.unlink(missing_ok=True)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    default_directory = Path(__file__).resolve().parents[2] / "migrations"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        choices=("validate", "apply", "verify", "baseline"),
        help="validate files, apply pending versions, verify zero pending, or baseline an audited DB",
    )
    parser.add_argument("--migrations-dir", type=Path, default=default_directory)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--psql", default="psql", help="psql executable name or path")
    parser.add_argument("--lock-timeout-seconds", type=int, default=60)
    parser.add_argument("--baseline-version", type=int)
    parser.add_argument(
        "--acknowledge-baseline-risk",
        action="store_true",
        help="confirm that the existing DB was backed up and schema-verified before baseline",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        migrations = discover_migrations(args.migrations_dir)
        print(
            f"validated {len(migrations)} immutable migrations "
            f"(V001..V{migrations[-1].version:03d})"
        )
        if args.mode == "validate":
            return 0
        if not args.database_url:
            raise MigrationError("DATABASE_URL or --database-url is required")
        if args.mode == "baseline" and not args.acknowledge_baseline_risk:
            raise MigrationError(
                "baseline is intentionally blocked without --acknowledge-baseline-risk"
            )
        if args.mode != "baseline" and args.baseline_version is not None:
            raise MigrationError("--baseline-version is only valid in baseline mode")

        driver_sql = build_driver_sql(
            migrations,
            mode=args.mode,
            lock_timeout_seconds=args.lock_timeout_seconds,
            baseline_version=args.baseline_version,
        )
        run_psql(args.database_url, driver_sql, args.psql)
        return 0
    except MigrationError as error:
        print(f"migration error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
