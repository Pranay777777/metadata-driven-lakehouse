# ADR-016: Replay, and the one time a watermark may move backwards

- **Status:** accepted
- **Date:** 2026-09-27

## Context

Two failures need recovery, and they need different tools.

**Bad source data, since corrected upstream.** Everything loaded after
some date is wrong and the fixed rows now exist at the source. The
lake's history is fine; what it loaded is not.

**A bad transform.** The source was right, a Silver or Gold table is
wrong. The source is irrelevant; the table needs to go back.

## Decision — rewind for the first, restore for the second

`replay rewind --source orders --from 2026-03-01` moves the source's
watermark back, so the next run re-reads everything since. Silver
deduplicates on key and sequence (ADR-004), so re-reading an overlap is
safe rather than doubling rows. That property is what makes rewinding
cheap.

`replay restore --path ... --to 2026-03-01` uses Delta time travel to
restore a table to its state at that moment. Delta records the restore
as a new commit, so it is itself reversible.

A full-load source is refused for rewind: it has no watermark, and its
next run already reloads everything.

## Decision — the watermark rule has exactly one exception

Design rule two: the watermark never moves backwards. The rule exists to
stop *automatic* regression — a grace window, clock skew, a bug — from
silently re-reading or skipping data.

Replay moves it backwards deliberately, and is the only sanctioned way
for that to happen. It is therefore:

- **operator-initiated only**, never called by the pipeline;
- **audited** — a task run records the old and new values, so "why did
  orders reload four months of data on Tuesday" has a recorded answer
  rather than looking identical to the bug the rule prevents;
- **dry run by default**, printing what it would change.

## Consequences

- A string watermark cannot be rewound to a date, because string keys
  have no ordering relationship with time. Refused with that reason.
- Replay depends on history surviving. The maintenance module's
  one-week vacuum floor (ADR-015) is partly here to guarantee it.
- Rewind and restore are separate commands on purpose. A single
  "replay" that did both would restore Silver *and* reload Bronze, and
  the two interact: restoring Silver to March and then reloading from
  March is right, doing it in the other order is not.
