# ADR-012: Column-level lineage via OpenLineage

- **Status:** accepted
- **Date:** 2026-09-27

## Context

The control plane records what ran and how many rows moved. It cannot
answer the question people actually ask during an incident: *this
number is wrong, where did it come from?* That needs edges between
datasets, and it needs them in a form a tool can draw.

OpenLineage is the standard for those edges; Marquez, added at step 17,
is the server that draws them.

## Decision — lineage never breaks the pipeline

Every emission is wrapped; failures are logged, never raised. A client
that cannot be constructed degrades to disabled rather than throwing at
import time.

Observability that can take down the thing it observes is a liability.
A Marquez container down at 3am must not stop the load, and a five
second transport timeout means a hung server does not become a hung
pipeline.

## Decision — disabled means silent, not buffered

With `OPENLINEAGE_ENABLED` false there is no client, no socket, no
queue. The default is false, so the test suite and CI never touch the
network and never need Marquez running.

## Decision — column-level, not just table-level

A table-level graph says a number came from somewhere in `orders`. A
column-level graph says which field, which is the difference between
narrowing an incident to a table and narrowing it to a line of code.

Each layer declares the mapping it knows:

- **Bronze** copies faithfully — identity mapping. The provenance
  columns it *adds* have no upstream field and are deliberately left
  out of the mapping rather than attributed to something invented.
- **Silver** renames — each conformed column points back at its
  original source name, which is what makes `customer_id` traceable to
  `CustomerID`.
- **Gold** derives — the surrogate key is attributed to the natural key
  columns it was hashed from.

## Decision — events wrap the existing task boundaries

START on entry, COMPLETE or FAIL on exit, sharing one run UUID, emitted
from the same `try`/`except` that already records the task audit. A FAIL
carries the error message as a run facet, so the graph shows a failed
edge rather than simply missing one.

START carries no datasets on purpose: a job does not know its output
schema until it has produced one.

## Consequences

- **A new runtime dependency: `openlineage-python`.**
- Lineage run IDs are fresh UUIDs per task, unrelated to
  `pipeline_run.run_id`. Correlating a Marquez run back to the control
  plane is therefore manual today — passing the pipeline run id as a
  parent run facet would fix it and belongs with step 32's
  orchestration, where a real parent run exists.
- Emission is synchronous and inline. At this volume that is invisible;
  under a per-partition loop it would not be, and the client's async
  transport is the answer then.
- Nothing emits lineage for the quarantine table yet, so rows leaving
  the pipeline are absent from the graph. That is a real gap and worth
  closing when the dashboard at step 38 starts reporting on quarantine.
