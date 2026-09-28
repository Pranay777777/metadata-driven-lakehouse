# ADR-020: Migrations, Postgres integration tests, and a CI matrix

- **Status:** accepted
- **Date:** 2026-09-28

## Context

Coverage has been above 90% since before this step, so "≥70%" was never
the gap. Three others were:

1. **Every test ran on SQLite.** The control plane targets Postgres (and
   Azure SQL), and nothing had ever exercised it there.
2. **Schema changes had no migration path.** `create_all` only creates
   missing tables; it never adds a column to an existing one. Step 36
   added two `column_metadata` columns and broke every Silver task on an
   existing stack until the table was dropped by hand. `alembic` was a
   declared dependency with nothing using it.
3. **CI tested one configuration nobody develops on.** Ubuntu, Python
   3.11 — while the developer machine is Windows, Python 3.12. A NumPy
   release that dropped 3.11 broke mypy on 3.12 and CI never saw it.

## Decision

### Alembic, with an exact baseline

Revision `0001` is the schema `create_all` built at step 38, verified
table by table — columns, types, nullability, CHECK, UNIQUE, indexes,
foreign keys — on SQLite and Postgres (`tests/schema_shape.py`).

`lakehouse.migrate.ensure_schema` replaces the CLIs' `create_all` calls
and handles three starting points: empty (run everything), versioned
(run what is pending), and **legacy** — tables but no `alembic_version`,
because `create_all` built them. A legacy database is stamped at the
baseline and upgraded, keeping its history. That is the state of every
stack that followed the steps, so nobody has to do anything by hand.

`python -m lakehouse.migrate [--check]` does the same from the command
line. The config needs no `alembic.ini` on disk, so it works from an
installed wheel (the Docker image); `alembic.ini` exists for authoring
new revisions.

Two tests keep models and migrations honest: one compares the migrated
schema with `create_all`'s; the other runs Alembic's autogenerate diff
and requires it to be empty. Adding a model column without a migration
fails the second — verified by doing exactly that.

### The first real migration fixed a bug the new tests found

Running the platform against Postgres showed the dashboard's compute
figure reading zero. Every loader stored `int(elapsed)` in an integer
column, so any sub-second task recorded 0. Revision `0002` widens
`task_run.duration_seconds` to a float and loaders store
`round(elapsed, 3)`. Widening loses nothing; downgrade narrows back.

### Integration tests against real Postgres

`tests/test_integration_postgres.py`, marked `integration`, runs the
demo end to end, every dashboard view, migrations (including legacy
adoption with history and a downgrade round trip) and the `day()`
construct against Postgres. Each test creates and drops its own database
from an admin URL (`LAKEHOUSE_TEST_POSTGRES_URL`), so pointing it at the
compose stack never touches the `app` control plane. Without the
variable they skip. `make test-integration` runs them locally.

### A CI matrix that includes the developer's machine

- Tests on Ubuntu 3.11, Ubuntu 3.12 and **Windows 3.12**.
- mypy on 3.11 and 3.12.
- An integration job with a `postgres:14-alpine` service — the compose
  stack's version.

The Windows job skips Spark (`LAKEHOUSE_SKIP_SPARK`): the runner has a
JVM, but Spark writes on Windows also need Hadoop's `winutils`, and the
Linux jobs cover Spark. That mirrors the developer machine, which has
no JVM at all.

The Python floor stays at 3.11. Testing both versions costs one matrix
cell; dropping 3.11 would be a support-policy change with no benefit to
anyone using the project today.

## Consequences

**Migrations run automatically** whenever `pipeline` or `privacy`
starts. That suits a single-user tool; in a shared deployment, schema
changes should be a deploy step, and `migrate --check` can gate one.

**Autogenerate is a draft, not a migration.** It misses CHECK
constraint changes and renames. Every revision is reviewed by hand, and
the shape test catches what review misses.

**Azure SQL is still untested.** The reference DDL and the `day()`
construct target it, but no CI job runs against SQL Server. A
`mcr.microsoft.com/mssql/server` service job is the next step if that
target becomes real.

**Time zones.** `day()` on Postgres buckets by the session time zone,
which is UTC on the compose stack and in CI. A server configured
otherwise would shift late-evening runs to the next day.
