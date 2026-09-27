# ADR-006: Gold as a config-driven star schema

- **Status:** accepted
- **Date:** 2026-09-27

## Context

Silver is correct but shaped like the source system. Gold has to be
shaped like the questions: a fact table of measurements surrounded by
dimensions that describe them.

The constraint that matters is the same one that governs every other
layer here — nothing in the pipeline may know what `orders` is. A Gold
builder that hardcodes "resolve `customer_id` against `dim_customer`"
would break that premise at exactly the layer analysts look at.

## Decision 1 — the star is described in the control plane

Two additions:

- `source_object.gold_role` — `fact`, `dimension`, or NULL. NULL is the
  default and means the object is not published. Most objects are
  staging or reference data that no analyst should query.
- `gold_reference` — one row per edge of the star: a fact column, and
  the dimension object it points at.

Rejected: YAML. Step 27 introduces YAML contracts, but those describe
expectations about data; this describes the shape of the platform, and
ADR-002 already argued that belongs in the control plane where it can be
joined and queried alongside everything else.

**Consequence:** building a star for a new fact is two INSERTs, and the
builder never changes.

## Decision 2 — surrogate keys are derived, not sequenced

A surrogate is a 63-bit BLAKE2b hash of the natural key — plus
`valid_from` for an SCD2 dimension, so each version of a member is
individually addressable.

Rejected: a sequence or identity column. A sequence needs a generator, a
persisted counter, and an answer to "what happens when Gold is rebuilt".
Gold is derived data and *is* rebuilt in full every run, so its keys
must be reproducible from the input alone. A hash is.

**Consequence:** keys are not small, not ordered, and not human-readable,
so they are useless for debugging and must never be shown to a user.
Collision probability at 63 bits is negligible at any volume this
platform will see, but it is not zero, and that is a real difference
from a sequence.

## Decision 3 — every dimension carries an unknown member at key 0

Key `0` is reserved and is never produced by the hash (the hash is
offset away from zero). Each published dimension gets one all-null row
at that key.

A fact row whose dimension member has not loaded yet has to join to
*something*. Dropping it removes the measure from every total silently;
a null foreign key does the same thing on an inner join. The unknown
member keeps the row, keeps the money in the sum, and makes the gap
countable — `GoldResult.unresolved` reports it per column and the task
audit records it as `rows_rejected`.

**Consequence:** a report can quietly attribute revenue to "unknown"
forever if nobody watches that count. Step 38's dashboard should surface
it.

## Decision 4 — facts join SCD2 dimensions at event time

When a dimension has history and the fact has a timestamped
`incremental_column`, the lookup selects the version whose validity
interval contains the fact's event time, by binary search over that
member's versions.

Joining a two-year-old order to today's customer record discards the
entire point of step 24. A fact that predates every version of its
member resolves to the unknown member rather than to the earliest
version, because attributing it to a version that did not yet exist is a
guess dressed as data.

**Consequence:** a fact with no usable event-time column falls back to
the current version, which is correct for real-time loads and wrong for
back-fills. That fallback is silent and should probably become a
warning once step 28's DQ engine exists.

## Out of scope

- **Conformed dimensions shared across facts.** Two facts referencing
  the same dimension object already share its surrogates, which is
  conformance in the useful sense, but there is no cross-source identity
  resolution and no mastering.
- **Build ordering.** Dimensions must be built before the facts that
  reference them, and the caller owns that today. `object_dependency`
  exists for this and is wired up at step 32 with Dagster.
- **Incremental Gold.** Full rebuild every run. Affordable because Gold
  is derived; revisit at step 34 if it stops being affordable.

## Implementation note

As in ADR-005, timestamp values are read as epoch microseconds rather
than Python `datetime` objects, so no IANA timezone database is
required. Verified with the system timezone database removed.
