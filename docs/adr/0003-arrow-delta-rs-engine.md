# ADR-003: Arrow + delta-rs as the default engine, Spark as an option

- **Status:** accepted
- **Date:** 2026-09-26

## Context

The platform needs a table format with ACID writes, versioning and time
travel — Delta Lake, to match the Azure Data Factory and Fabric target.
The question is what executes the writes.

The obvious answer is PySpark, which is what the cloud deployment uses.
But Spark locally means a JVM, a Docker Compose stack of several
gigabytes, and a README whose first instruction is "install Docker
Desktop and enable WSL2". Most people who clone a repo do not do that;
they read the README and leave. A platform nobody can run is a platform
nobody can evaluate.

`delta-rs` (the `deltalake` package) implements the Delta protocol in
Rust with an Arrow interface. It is a pip install with no JVM.

## Options considered

1. **PySpark + Docker Compose.** Matches production exactly. Costs every
   reader a 3 GB download and a working Docker install before they see
   anything run.
2. **Plain Parquet, no table format.** Trivially portable, but gives up
   ACID, versioning and time travel — which are the properties the
   idempotency and replay work depends on.
3. **Arrow + delta-rs by default, Spark as an optional engine.**

## Decision

Option 3. Ingestion is written against Arrow tables and `deltalake`, so
the default path is `pip install && make seed && make run` with no other
prerequisites. The engine is kept behind a narrow interface so a Spark
backend can be added (step 33) without touching the metadata layer or
the load strategies.

Verified before committing: `delta-rs` gives real versions and time
travel — reading version 0 after an overwrite returns the pre-overwrite
data — with no Java present.

## Consequences

**Gained:** the whole pipeline runs on a laptop in seconds; CI needs no
services, so tests are fast and hermetic; Delta semantics (ACID,
versioning, time travel) are preserved, which the idempotency and replay
steps depend on; the eventual two-engine setup is a stronger
demonstration than Spark alone.

**Given up:** the default path no longer resembles the production
runtime, and a reviewer scanning for PySpark will not find it in the
ingestion layer. Very large workloads would need the Spark backend.
delta-rs does not implement every Delta feature Spark does — notably
some `MERGE` and Z-ordering behaviour — which may constrain steps 20 and
34.

**Revisit when:** a required Delta feature turns out to be missing from
delta-rs, or the dataset outgrows single-node memory.

## Update, 2026-09-26 (step 20)

The `MERGE` concern above did not materialise. delta-rs implements
`when_matched_update`, `when_matched_delete` and `when_not_matched_insert`
with predicates, executes them in one atomic commit, and returns
row-level metrics (`num_target_rows_inserted`, `updated`, `deleted`),
which the CDC loader records straight into `task_run`. Insert, update
and delete applied in a single batch are covered by tests.

One operational note: the delta-rs process emits `terminate called
without an active exception` on interpreter exit in some scripts. It is
teardown noise after the commit has been written, not a failed merge,
and it has not appeared under pytest.
