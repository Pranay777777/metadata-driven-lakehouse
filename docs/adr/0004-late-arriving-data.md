# ADR-004: Late-arriving data via a bounded grace window

- **Status:** accepted
- **Date:** 2026-09-26

## Context

An incremental load reads rows whose watermark column is strictly above
the stored watermark. That definition is silently wrong for any source
where a row can be written with a timestamp earlier than the moment it
becomes visible — which is most of them. Long-running transactions,
clock skew between application servers, batch back-fills and replayed
message queues all produce rows that appear *after* the watermark has
passed them.

Those rows are never read again. No error, no alert, no gap in the audit
table — the load reports success and the data is simply absent. This is
the most common silent data-loss bug in watermark-based pipelines.

The synthetic generator produces these deliberately: 2% of rows have an
`updated_at` backdated behind their event time.

## Options considered

1. **Ignore it.** Correct only for sources that write in strict
   timestamp order — rare, and unverifiable from the outside.
2. **Full reload.** Always correct, and unaffordable for a fact table.
3. **Change-tracking on the source.** Best answer where it exists (CDC
   is exactly this), but many sources cannot offer it.
4. **A bounded grace window:** read from `watermark - grace` rather than
   `watermark`.

## Decision

Option 4, configured per object as `source_object.watermark_grace`,
defaulting to 0. Grace is expressed in the watermark's own units —
seconds for a timestamp, raw units for an integer key. String watermarks
have no arithmetic, so grace is ignored and a warning is logged rather
than a wrong answer produced.

Two supporting rules:

- **The watermark never moves backwards.** With grace enabled, a batch
  can consist entirely of old rows whose maximum sits below the stored
  watermark. Advancing to that maximum would ratchet the watermark down
  every run and widen the window without limit.
- **The cost is reported, not hidden.** `rows_reprocessed` on the result
  counts rows re-read because of grace, so the overhead of a wide window
  is visible in the audit trail rather than discovered as a bill.

## Consequences

**Gained:** late rows within the window are ingested; the size of the
window is a per-source decision made by whoever knows the source, not a
global guess; the trade-off is measurable.

**Given up:** rows already loaded are re-read and appended again. Bronze
is append-only, so duplicates land there and Silver's deduplication
(step 23) removes them — which means correctness now depends on Silver
doing its job. A wider window costs proportionally more re-reading. And
the window is a bound, not a guarantee: a row later than `grace` is
still missed, which is a choice to accept a known limit rather than
pretend the problem is solved.

**Revisit when:** a source needs a window wide enough that re-reading
dominates the load — at that point change-tracking at the source, or an
anti-join against the target, becomes the cheaper answer.
