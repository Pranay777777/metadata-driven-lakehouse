# ADR-005: SCD Type 2 history in Silver

- **Status:** accepted
- **Date:** 2026-09-27

## Context

Silver as built in step 23 is a snapshot: one row per key, newest wins.
That is correct for most objects and wrong for any object whose past is
part of the answer. Which city was this customer in when they placed
that order? What was this product's category on the day it sold? A
snapshot dimension answers both questions with today's value, quietly,
and every historical report built on it is wrong in a way nobody
notices.

`source_object.scd2_enabled` has existed since step 15 and has been
unused. This ADR records what turning it on actually means.

Four decisions were open. Each turned out to be forced by something
already in the repo rather than a matter of taste.

## Decision 1 — history accumulates, it is never rebuilt

**Rebuilding Silver history from Bronze on every run is not possible.**
`load_full` writes Bronze with `mode="overwrite"`; a full-reload source
keeps no past in Bronze at all. Rebuilding would produce exactly one
version per key, forever, and it would look like it was working.

History therefore lives only in Silver and is extended by a Delta
`MERGE` — one atomic commit per run, so a crash applies the whole change
or none of it.

The MERGE uses the two-copy pattern: each new version appears twice in
the source table, once carrying real key values (which matches the open
row and closes it) and once carrying nulls (which matches nothing and is
inserted). A single MERGE statement cannot otherwise both update and
insert for the same key.

**Consequence:** Silver is now stateful. It can no longer be deleted and
recomputed, which makes the Delta table itself a thing to back up. Delta
time travel mitigates this and step 35's replay CLI will lean on it.

## Decision 2 — effective dating prefers business time, falls back to processing time

`valid_from` comes from the object's `incremental_column` when that
column holds a timestamp. When it holds an integer surrogate key — a
perfectly valid incremental column, and one the schema explicitly allows
— it orders rows correctly but says nothing about *when* anything was
true, so those versions are dated at processing time.

Rejected: a new `scd2_effective_column` config field. It would be
another column to populate correctly for every source, to express
something already derivable from the type of a column we have.

**Consequence:** two objects can date history on different clocks. That
is honest — it reflects what the sources actually provide — but it means
`valid_from` is not comparable across objects without checking which
kind it is.

## Decision 3 — change detection by hash of the business columns

Each row carries `_row_hash`, a SHA-256 over every column except the
keys, the provenance stamps (`ingested_at`, `run_id`, `source`) and the
SCD2 columns themselves. Columns are read in sorted order, and nulls get
an explicit marker so a null and an empty string cannot collide.

Rejected: a configured list of tracked columns. It is one more thing to
get wrong when a source gains a column, and getting it wrong means
silently missing changes.

Excluding provenance is what makes this idempotent: re-running the same
batch produces identical hashes, so nothing is written. That is the same
property step 21 established for Bronze, and it is tested the same way.

**Consequence:** every column change opens a version, including ones
nobody cares about. If a chatty column proves noisy, a tracked-column
list can be added later without changing the stored format.

## Decision 4 — a delete closes the row and leaves a tombstone open

When a key disappears from the source, its open row is closed and a new
version is opened with `is_deleted = true`. Consumers filter
`is_current AND NOT is_deleted`.

Rejected: closing the row and opening nothing. A key with no open row is
indistinguishable from a key that has not loaded yet, and fact rows
referencing the deleted entity lose their dimension join entirely.

The invariant this preserves: **every key ever seen has exactly one open
row.**

Deletion is only *inferred* for sources whose Bronze table represents
current state — full reload and CDC. For an incremental source Bronze
only ever grows, so a key's absence from a batch means nothing, and
inferring deletes there would tombstone the entire dimension on the
first run.

**Consequence:** a deleted key keeps a queryable row forever. For PII
this is the wrong default and step 36 will need to mask tombstones
rather than assume deletion removed anything.

## Out of scope

- **Retroactive correction.** A row whose effective time is older than
  the currently open version is refused and counted in `late_skipped`,
  not spliced into the middle of the chain. Rewriting history needs a
  policy, not a code path.
- **Schema evolution.** A batch whose columns no longer match the
  history table raises rather than widening the table. That is step 26.
- **Reading the whole history to find open rows.** Fine at portfolio
  scale, not at production scale. Partitioning on `is_current` is step
  34's problem.

## Implementation note: no timezone database is assumed

Timestamp values are compared as epoch microseconds, never as Python
`datetime` objects. Converting a timezone-aware Arrow timestamp to
Python requires an IANA timezone database, which Linux provides at
`/usr/share/zoneinfo` and Windows does not ship at all. Reading values
the obvious way therefore passes in CI and fails on a Windows
workstation.

Integers order identically, need no such lookup, and avoid constructing
a `datetime` per row. The test suite follows the same rule, and both it
and the library are verified with the system timezone database removed.

## Consequences

`build_silver` now dispatches on `scd2_enabled`, so the choice stays
metadata-driven and the runner still knows nothing about any particular
object. Objects with the flag off behave exactly as they did in step 23.
