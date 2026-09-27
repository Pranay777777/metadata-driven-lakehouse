# ADR-013: Dagster assets generated from the catalog

- **Status:** accepted
- **Date:** 2026-09-27

## Context

`python -m lakehouse.pipeline` runs the layers in a fixed order and
stops at the first thing it cannot do. Fine for a demo, wrong for
operations, which needs three things it cannot provide: a dependency
graph something other than a human understands, reruns of only what
failed, and a schedule that does not require someone at a terminal.

## Decision — Dagster, not Airflow

Airflow is the safer CV line. Dagster is the better fit here, for one
concrete reason: this platform's unit of work is a **dataset**, not a
task. Bronze, Silver and Gold tables are assets with lineage between
them, and Dagster models exactly that, which means its graph and the
OpenLineage graph from ADR-012 describe the same thing rather than two
parallel views.

Airflow would model the same work as tasks with edges bolted on, and
the interesting structure — that `fact_orders` depends on
`dim_customers` — would live only in a DAG file.

**Consequence:** anyone reading this repo who expects Airflow will need
the above explained. That is what this ADR is for.

## Decision — assets are generated from the catalog

Fifteen assets are produced from `CATALOG`, the same list that
registers the control plane. Adding a `CatalogEntry` produces its
Bronze, Silver and Gold assets with dependencies already correct.

Hand-written assets would be fifteen places to forget an edge, and the
edge most easily forgotten is the one that matters most:

- Silver depends on its own Bronze.
- A Gold **dimension** depends on its own Silver.
- A Gold **fact** depends on its own Silver *and every dimension it
  references*.

Miss that last one and a fact resolves surrogates against a dimension
that has not been published yet. Every key lands on the unknown member,
the run reports success, and the result looks like a data quality
problem rather than an ordering bug. It is generated from
`gold_reference` precisely so it cannot be forgotten.

## Decision — the schedule ships stopped

`default_status=STOPPED`, 02:00 daily. A schedule that starts itself
the moment someone opens the UI is a surprise, not a feature, and a
portfolio repo that begins running jobs on a reviewer's laptop is a
bad first impression.

## Implementation notes

- **No `from __future__ import annotations` in the definitions module.**
  Dagster inspects real annotation objects to decide which parameters
  are resources; string annotations defeat it, and the error it
  produces names the wrong cause.
- `LakehouseResource` is a plain class, not a `ConfigurableResource`.
  Configuration already lives in `Settings`, and a second configuration
  system layered on the first is how the two end up disagreeing. Assets
  declare `required_resource_keys` and read it off the context.

## Consequences

- **Dagster is a heavy dependency** — it and its webserver dominate the
  install. It is an optional extra for that reason; the platform runs
  without it via `python -m lakehouse.pipeline`.
- Assets currently return `None` rather than emitting Dagster
  materialization metadata (row counts, Delta versions). That metadata
  is already in `GoldResult` and `SilverResult` and would show up in
  the Dagster UI for free. Worth doing; not done.
- Nothing is partitioned. Daily partitions on the incremental objects
  are the natural next move and would make backfills first-class.
- The lineage parent-run gap from ADR-012 is **not** closed here. A
  Dagster run id now exists and could be passed as an OpenLineage
  parent run facet, but the assets each open their own pipeline run, so
  there is still no single parent to point at.
