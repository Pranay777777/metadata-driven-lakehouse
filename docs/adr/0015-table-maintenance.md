# ADR-015: Compaction and Z-ordering, without partitioning

- **Status:** accepted
- **Date:** 2026-09-27

## Context

Every incremental load appends files and nothing ever merges them. A
table that has loaded daily for a year is thousands of small Parquet
files, and every read pays to open each one. This is the most common way
a Delta lake gets slow without anything visibly failing.

## Measured

`scripts/benchmark_maintenance.py`: 200 appends of 5,000 rows each —
1,000,000 rows, keys shuffled across every batch so each file spans the
full key range. The query reads a 0.1% key range. Median of 7 runs,
Linux x86_64.

| state | files | avg MB | read ms | vs start |
|---|---:|---:|---:|---:|
| fragmented | 200 | 0.08 | 215.0 | 1.0x |
| compacted | 8 | 1.74 | 51.7 | 4.2x |
| z-ordered | 9 | 1.36 | 11.6 | **18.5x** |

Compaction removes the per-file overhead. Z-ordering then clusters keys
so each file's min/max statistics cover a narrow range, and the reader
skips files that cannot contain the answer — another 4.4x on top.

### A benchmark that almost lied

The first version compacted to a single file. Z-order still showed a
gain, but with one file there is nothing to skip *between*; the gain
was row-group ordering inside that file. Real, but not what Z-ordering
is for, and quoting it would have misattributed the speedup. The
benchmark now compacts to a small target so several files remain, which
is the honest test and produces the larger, correctly attributed number.

## Decision — no partitioning

The plan listed partitioning. It is deliberately not implemented.

Partitioning pays when a partition holds enough data that skipping it
matters — conventionally a gigabyte or more. Below that, it multiplies
small files: a daily partition on a table taking a few thousand rows a
day produces exactly the fragmentation this ADR exists to remove, now
spread across hundreds of directories that compaction cannot merge
across.

Z-ordering gets most of the pruning benefit without fixing the layout
to a single column forever. Over-partitioning is one of the most common
ways a lakehouse gets slower as it grows, and declining to do it on
tables this size is the correct call rather than a gap.

**Revisit** when any one table's daily volume passes roughly a gigabyte.

## Decision — vacuum is the dangerous one

Compaction and Z-ordering change layout, never data, so they are always
safe. Vacuum deletes files, so it is guarded three ways:

- **dry run by default** — `--apply` is required to delete anything;
- **a one-week retention floor** that cannot be undercut, because the
  replay CLI (ADR-016) needs history to travel to;
- **a hard refusal to touch quarantine tables**, as ADR-010 required.

## Consequences

- Maintenance runs on demand, not on a schedule. Wiring it as a weekly
  Dagster job is a small follow-up.
- Z-order columns are chosen per invocation. The right columns are the
  ones analysts filter on, which is knowledge about queries, not about
  tables — so it is not guessed automatically.
