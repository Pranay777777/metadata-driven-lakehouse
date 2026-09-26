# ADR-002: Metadata control plane as SQLAlchemy models

- **Status:** accepted
- **Date:** 2026-09-26

## Context

The platform's premise is that onboarding a source is configuration, not
code. That only holds if the configuration schema is rich enough to
describe every load without a special case, and rigid enough that an
invalid configuration is rejected before a pipeline runs.

An earlier version of this project used two tables: `SourceTableConfig`
(table name, load type, target folder, incremental column, load order) and
`ExecutionLog`. The shape was right, but three things were missing:

1. **No watermark storage.** `IncrementalColumn` recorded *which* column to
   filter on, but nothing recorded *where the last load stopped*. A run
   that failed halfway had no way to resume, which makes "incremental"
   decorative.
2. **No constraints.** Nothing stopped a row declaring `LoadType =
   'Incremental'` with a null incremental column. That configuration fails
   at runtime, in production, on the source that matters.
3. **One row count.** `RowsCopied` cannot distinguish a clean load from one
   that silently dropped half its input.

Two dialects are also in play: PostgreSQL in the local Docker stack, Azure
SQL in the cloud. Hand-maintaining two DDL files guarantees they drift.

## Options considered

1. **Raw SQL per dialect.** Simple and readable; two files to keep in sync
   by hand, and no way to enforce that they match.
2. **YAML config files in the repo.** No database needed, diffable in git.
   But configuration becomes a deployment rather than an INSERT, there is
   no referential integrity, and run audit needs a database regardless.
3. **SQLAlchemy models as the source of truth**, with DDL generated per
   dialect and migrations via Alembic.

## Decision

Option 3. `src/lakehouse/metadata/models.py` defines ten tables across
three groups:

**Configuration** — `source_system`, `source_object`, `column_metadata`,
`dq_rule`, `object_dependency`
**State** — `load_watermark`, `schema_version`
**Audit** — `pipeline_run`, `task_run`, `dq_result`

Specific choices worth recording:

- **Watermarks live in their own table**, keyed by object, with the
  `committed_run_id` that advanced them. The watermark moves only after a
  successful write, which is what makes a re-run idempotent rather than
  lossy.
- **Watermark values are stored as text plus a type tag**, so one column
  serves timestamp, integer and string keys. The alternative — three
  nullable typed columns — is uglier and invites nulls.
- **Enums are strings with CHECK constraints**, not native enum types.
  PostgreSQL and Azure SQL disagree on enum support, and altering a native
  enum is painful.
- **Conditional CHECK constraints encode the rules that used to be
  runtime failures**: incremental strategy requires an incremental column;
  CDC requires primary key columns. An invalid config cannot be inserted.
- **Row counts split three ways** — read, written, rejected.
- **Credentials are never stored**, only the name of the secret to resolve.
  The control plane must be safe for anyone who can query it to read.
- **`ON DELETE CASCADE` at the database level** with `passive_deletes=True`
  on the ORM side, so the database performs cascades rather than the ORM
  nulling foreign keys first.

## Consequences

**Gained:** invalid configuration is rejected at insert time rather than at
3am; incremental loads can actually resume; both dialects are generated
from one definition and cannot drift; Alembic gives versioned schema
changes; the models are typed, so pipeline code gets autocomplete and
mypy checks against the schema.

**Given up:** SQLAlchemy is now a dependency of the control plane, and
anyone editing the schema needs to understand declarative models rather
than plain SQL. Generated DDL is slightly less idiomatic than
hand-written. Ten tables is more surface area than two, and most small
projects would not need `object_dependency` or `schema_version` on day
one.

**Revisit when:** a source appears that cannot be described by this schema
without adding a nullable column that only applies to one case — that is
the signal the model is being stretched rather than extended.
