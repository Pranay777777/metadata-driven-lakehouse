# ADR-010: Quarantine as a table, and an audit trail worth reading

- **Status:** accepted
- **Date:** 2026-09-27

## Context

ADR-009 left a hole it admitted to: quarantined rows were removed from
the batch and written nowhere. The pipeline reported success while
losing data, which is strictly worse than failing, because it is quiet.

Separately, `pipeline_run`, `task_run` and `dq_result` had been written
since step 18 and read by nothing. An audit table nobody queries is a
table nobody notices is wrong.

## Decision — quarantined rows get their own Delta table

Rejected rows land at `quarantine/<same relative path>`, carrying the
rule they breached, the run and task that rejected them, and when. Three
things become possible that a dropped row makes impossible: counting
what is being lost, showing a data owner the actual offending rows, and
replaying them once the source is fixed.

**Append-only, never rewritten.** A quarantine table with automatic
cleanup is a log nobody can trust.

**Written before the layer itself.** A crash between the two loses the
load, which reruns, rather than the evidence, which does not.

**Schema is merged, not overwritten.** Step 26 permits additive drift,
so quarantined rows from different weeks legitimately have different
shapes, and dropping the older ones to make room defeats the point.

A row breaching two rules records both. Recording only the first turns
"why was this rejected" into a guessing game.

## Decision — a run's status is derived from its tasks

Previously the caller passed a status, so a run whose every task failed
could still report success. It is now computed: any failed task fails
the run, any quarantined task marks it quarantined.

## Decision — `track_run` guarantees the run is closed

`start_pipeline_run` deliberately persists a `running` row up front, so
a killed process leaves evidence rather than nothing. Nothing ever
closed it, so "in flight" and "died three weeks ago" looked identical.

`track_run` is a context manager that always closes the run, as `failed`
if an exception escaped — and re-raises, because swallowing it would
turn a crash into a silent no-op, the exact failure this exists to
catch. `stale_runs` surfaces whatever is still open.

## Consequences

- Quarantine tables grow without bound. There is no retention policy and
  should not be one until someone decides what it is; `VACUUM` at step
  34 must be configured not to touch them.
- `object_health` and `rule_performance` read every matching row rather
  than aggregating in SQL. Honest at this scale and the wrong shape for
  a dashboard refreshing every minute — step 38 will need them pushed
  down.
- Ordering by `started_at` alone is ambiguous at one-second resolution,
  so `run_id` breaks the tie. That makes the order stable rather than
  strictly chronological within a second.
- `track_run` is available but not yet used by the runner; step 32 wires
  it in with Dagster. Until then a caller can still forget it.
