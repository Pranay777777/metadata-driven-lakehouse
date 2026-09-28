# ADR-019: An ops dashboard that reads only through the audit module

- **Status:** accepted
- **Date:** 2026-09-28

## Context

Since step 18 every loader has written `pipeline_run`, `task_run` and
`dq_result` rows. Step 36 added `column_metadata` classifications; the
Gold builder records unknown-member fallbacks. An operator had no way to
see any of it short of writing SQL.

The plan asked for freshness, volume trend, DQ pass rate and cost, in
Streamlit.

## Decision

### The page is a renderer; `audit.snapshot()` is the product

Every number on the dashboard comes from `lakehouse.audit.snapshot()`,
which reads all views against one clock and returns plain dataclasses.
The app lays them out and holds no logic. So the numbers are tested
without Streamlit (`test_audit_views.py`), and the page is tested
headlessly with Streamlit's own `AppTest` against a control plane the
real pipeline populated (`test_dashboard.py`).

### Aggregation happens in SQL, one statement per view

`object_health` and `rule_performance` used to issue one query per
object and per rule, then count in Python. Both are now single
statements. "The last N tasks per object" is a `ROW_NUMBER()` window
rather than N queries. A test counts the statements executed, so the
N+1 cannot return quietly.

Grouping by day needed care: `CAST(x AS DATE)` is right on Postgres and
Azure SQL but silently yields the *year* on SQLite, and `date(x)` does
not exist on Azure SQL. A small `day()` construct compiles per dialect,
with a test for each.

### Each metric reads the source that actually means it

- **Freshness** is time since the last *completed* Silver build (a
  quarantined build still wrote its passing rows), against
  `freshness_sla_minutes`. It measures when the platform last loaded an
  object, not how new the newest source row is — the page says so.
- **Quarantine** is `dq_result.failed_row_count` on quarantine-severity
  rules. Silver's `rows_rejected` also counts removed duplicates and
  late SCD2 rows; labelling those "quarantined" would be wrong in the
  direction that causes panic.
- **Unknown members** come from each fact's *latest* Gold build only.
  Gold is rebuilt in full, so older counts describe tables that no
  longer exist.
- **Privacy posture** lists every classified column, its masking
  strategy, and whether it reaches Gold (ADR-017).

### No cost figure — compute time instead

There is no billing data behind this platform. A dollar figure would
mean inventing a rate and presenting the invention as a measurement. The
page shows compute-seconds per layer per day and labels it the cost
proxy. On Databricks or Synapse, the real figure comes from the
platform's billing tables joined on the same `run_id`.

### Streamlit is optional, and does not phone home

`streamlit` is the `[dashboard]` extra (and in `dev`, so CI tests the
page). `.streamlit/config.toml` turns off usage statistics: an
operations page over a control plane has no business sending telemetry.
The control-plane URL is shown with its password redacted.

## Consequences

**The dashboard found a real bug on its first render.** `run_all` and
both Dagster asset factories opened Silver and Gold runs with
`start_pipeline_run` and never closed them. Every invocation since those
stages were written left two runs marked `running` forever —
indistinguishable from a crashed process. `audit.track_run`, written for
exactly this, had never been adopted by either. All four call sites
now use it, and a regression test fails against the old code with
`['silver', 'gold']` still open.

**Existing control planes carry that bug's debris.**
`python -m lakehouse.audit --close-abandoned` finds runs left `running`
for more than 24 hours and shows what it would close, deriving each
status from the run's tasks. A run with no tasks, or one still holding a
`running` task, closes as failed. The end time is the last task's, so
durations stay truthful. `--apply` writes it. Dry-run by default,
because the control plane is the audit record.

**No alerting.** The page shows a breached SLA; nothing pages anyone.
Alerts need a scheduler that runs whether or not someone has the tab
open — Dagster sensors, at a later step.

**No authentication.** Streamlit serves to anyone who can reach the
port. It is a local tool; exposing it needs a reverse proxy with auth in
front, which is a deployment concern, not an application one.
