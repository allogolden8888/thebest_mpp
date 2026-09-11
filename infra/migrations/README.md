# Production PostgreSQL migration gate

`migrate.py` replaces the unsafe production pattern `for f in V*.sql; psql
-f ...`.  It keeps an immutable history in
`mpp_migrations.schema_history`, compares SHA-256 checksums on every run and
uses one PostgreSQL session-level advisory lock for the whole operation.

Each pending migration and its history row run in one transaction.  An SQL
error therefore rolls the migration back and prevents application rollout.
Applied migrations must never be edited or renamed; add a new `VNNN__*.sql`
instead. Versions must be unique and contiguous from `V001`.

## Deployment contract

Run from the repository root with a direct PostgreSQL connection (do not send
the session through a transaction-pooling PgBouncer endpoint):

```bash
python3 infra/migrations/migrate.py validate
DATABASE_URL='postgresql://...' python3 infra/migrations/migrate.py apply
DATABASE_URL='postgresql://...' python3 infra/migrations/migrate.py verify
```

`apply` is the pre-deploy migration job. It waits at most 60 seconds for the
global DB lock, validates the complete applied prefix, applies pending files
and verifies that none remain. `verify` is the application deployment gate:
it executes no migration and fails on a changed/renamed/removed migration, a
history gap, or any unapplied version.

The deploy identity needs `CREATE` on the database for first installation and
the DDL/DML privileges required by the migration files. Application identities
should only receive permissions on their own schemas and must not be able to
modify `mpp_migrations.schema_history`.

## Adopting an existing database

The normal modes intentionally fail when MPP schemas exist without history.
For each existing environment:

1. take a restorable backup and stop schema-changing deployments;
2. restore the backup into an isolated database;
3. apply `V001..VNNN` to a fresh empty database and compare schema, constraints,
   indexes, functions and required seed data with the restored database;
4. only after discrepancies are repaired, record the audited prefix once:

```bash
DATABASE_URL='postgresql://...' python3 infra/migrations/migrate.py baseline \
  --baseline-version 35 --acknowledge-baseline-risk
DATABASE_URL='postgresql://...' python3 infra/migrations/migrate.py verify
```

Baseline mode never executes migration SQL. The acknowledgement is deliberately
explicit because checksums prove repository immutability from that moment
forward; they cannot prove that an untracked historical database was built by
those files.

## Tests

```bash
python3 -m unittest discover -s infra/migrations -p 'test_*.py'
```

An integration smoke test can use any disposable PostgreSQL 17 database:
run `apply`, run `apply` again (idempotency), run `verify`, then change one SQL
file in a copied migration directory and confirm `verify` fails with
`migration history drift`.
