from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import migrate


class MigrationCatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def write(self, name: str, contents: str = "SELECT 1;\n") -> None:
        (self.directory / name).write_text(contents, encoding="utf-8")

    def test_discovers_sorted_catalog_and_hashes_raw_bytes(self) -> None:
        self.write("V002__second.sql", "SELECT 2;\n")
        self.write("V001__first.sql", "SELECT 1;\n")
        (self.directory / "README.md").write_text("ignored", encoding="utf-8")

        catalog = migrate.discover_migrations(self.directory)

        self.assertEqual([item.version for item in catalog], [1, 2])
        self.assertEqual(catalog[0].description, "first")
        self.assertEqual(
            catalog[0].checksum,
            hashlib.sha256(b"SELECT 1;\n").hexdigest(),
        )

    def test_rejects_duplicate_version(self) -> None:
        self.write("V001__first.sql")
        self.write("V001__also_first.sql")

        with self.assertRaisesRegex(migrate.MigrationError, "duplicate.*V001"):
            migrate.discover_migrations(self.directory)

    def test_rejects_gap(self) -> None:
        self.write("V001__first.sql")
        self.write("V003__third.sql")

        with self.assertRaisesRegex(migrate.MigrationError, "missing: V002"):
            migrate.discover_migrations(self.directory)

    def test_rejects_sql_file_outside_naming_contract(self) -> None:
        self.write("V001__first.sql")
        self.write("manual_patch.sql")

        with self.assertRaisesRegex(migrate.MigrationError, "manual_patch.sql"):
            migrate.discover_migrations(self.directory)

    def test_rejects_empty_migration(self) -> None:
        self.write("V001__first.sql", " \n")

        with self.assertRaisesRegex(migrate.MigrationError, "empty migration"):
            migrate.discover_migrations(self.directory)


class DriverSqlTest(unittest.TestCase):
    def migration(self, version: int = 1) -> migrate.Migration:
        path = Path(f"/tmp/V{version:03d}__example.sql")
        return migrate.Migration(
            version=version,
            description="example",
            script_name=path.name,
            checksum="a" * 64,
            path=path,
        )

    def test_apply_holds_session_lock_and_records_atomic_checksum(self) -> None:
        sql = migrate.build_driver_sql(
            [self.migration()], mode="apply", lock_timeout_seconds=23
        )

        lock_position = sql.index("pg_advisory_lock")
        table_position = sql.index("CREATE TABLE")
        include_position = sql.index("\\ir '/tmp/V001__example.sql'")
        history_insert_position = sql.index(
            "INSERT INTO mpp_migrations.schema_history", include_position
        )
        commit_position = sql.index("COMMIT;", history_insert_position)
        unlock_position = sql.index("pg_advisory_unlock")

        self.assertLess(lock_position, table_position)
        self.assertLess(include_position, history_insert_position)
        self.assertLess(history_insert_position, commit_position)
        self.assertLess(commit_position, unlock_position)
        self.assertIn("SET lock_timeout = '23s'", sql)
        self.assertIn("migration history drift", sql)
        self.assertIn("non-prefix migration history", sql)
        self.assertIn("'a" + "a" * 63 + "'", sql)

    def test_verify_fails_when_any_repository_version_is_pending(self) -> None:
        sql = migrate.build_driver_sql(
            [self.migration()], mode="verify", lock_timeout_seconds=60
        )

        self.assertIn("RAISE EXCEPTION 'pending migrations: %'", sql)
        self.assertNotIn("\\ir '/tmp/V001__example.sql'", sql)

    def test_baseline_requires_known_version_and_does_not_execute_sql(self) -> None:
        sql = migrate.build_driver_sql(
            [self.migration()],
            mode="baseline",
            lock_timeout_seconds=60,
            baseline_version=1,
        )

        self.assertIn("verified baseline", sql)
        self.assertIn("version <= 1", sql)
        self.assertNotIn("\\ir '/tmp/V001__example.sql'", sql)

    def test_rejects_invalid_lock_timeout(self) -> None:
        with self.assertRaisesRegex(migrate.MigrationError, "lock timeout"):
            migrate.build_driver_sql(
                [self.migration()], mode="verify", lock_timeout_seconds=0
            )


if __name__ == "__main__":
    unittest.main()
