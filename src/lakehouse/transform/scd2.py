"""Slowly Changing Dimensions, Type 2.

Silver's default is a snapshot: one row per key, the newest one wins,
and the previous value is gone. That is correct for most objects and
wrong for any object whose *past* matters — what city was this customer
in when they placed that order, what price was this product at on the
day it sold.

SCD2 keeps every version. A change closes the current row and opens a
new one, so a key has exactly one open row and an unbroken chain of
closed ones behind it.

Four decisions are load-bearing here, and each is forced by something in
the rest of the repo rather than chosen for elegance:

**History accumulates; it is never rebuilt.** Bronze cannot be replayed
into SCD2 history, because `load_full` overwrites Bronze on every run —
a full-reload source keeps no past. History therefore lives only in
Silver and is extended by MERGE, one atomic Delta commit per run. A
crash leaves either the whole change applied or none of it.

**Effective dating prefers business time.** `valid_from` comes from the
object's `incremental_column` when that column actually carries a moment
in time. Sources also use integer surrogate keys as their incremental
column, and dating history by a row ID is meaningless, so a
non-timestamp column falls back to processing time.

**Change detection is a hash of the business columns.** Comparing
column by column needs a configured list of which columns count, which
is one more thing to get wrong when a source gains a column. Hashing
everything except the keys and the provenance stamps means an unchanged
row is byte-identical and re-running a batch is a no-op — which is what
makes this idempotent, the same property step 21 established for Bronze.

**A delete closes the row and leaves a tombstone open.** The obvious
alternative — close the row and open nothing — makes a deleted key
indistinguishable from a key that simply has not loaded yet, and it
breaks fact joins for facts that reference the deleted entity. A
tombstone keeps the invariant that every key ever seen has exactly one
open row; consumers filter `is_current AND NOT is_deleted`.

Deletion is only inferred for sources whose Bronze table represents
current state (full reload, CDC). For an incremental source Bronze is an
append-only accumulation, so a key's absence from this batch means
nothing at all.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

from lakehouse.ingest.bronze import INGESTED_AT, RUN_ID, SOURCE_FILE
from lakehouse.metadata.enums import LoadStrategy
from lakehouse.tables import to_snake_case

VALID_FROM = "valid_from"
VALID_TO = "valid_to"
IS_CURRENT = "is_current"
IS_DELETED = "is_deleted"
ROW_HASH = "_row_hash"

HISTORY_COLUMNS = (VALID_FROM, VALID_TO, IS_CURRENT, IS_DELETED, ROW_HASH)

MERGE_PREFIX = "_mk_"
"""Prefix for the merge-key columns that exist only inside the MERGE source."""

TIMESTAMP = pa.timestamp("us", tz="UTC")

_MICROSECOND = 1
"""Smallest step in the timestamp unit, used to nudge past a tombstone."""

_PROVENANCE = frozenset(to_snake_case(c) for c in (INGESTED_AT, RUN_ID, SOURCE_FILE))
"""Provenance columns as they appear *after* conformance.

`conform` strips the leading underscore, so Bronze's `_ingested_at`
reaches Silver as `ingested_at`. Hashing it would make every row look
changed on every run.
"""

_NULL_MARKER = "\x00"
_FIELD_SEPARATOR = "\x1f"


@dataclass(frozen=True)
class Scd2Outcome:
    """What one SCD2 apply did to the history table."""

    opened: int
    """New versions written — new keys, changes and resurrections."""

    closed: int
    """Previously open rows given a `valid_to`."""

    tombstoned: int
    """Keys that disappeared from the source and were marked deleted."""

    unchanged: int
    """Keys present in the batch whose hash already matched. No write."""

    late_skipped: int
    """Rows older than the version already recorded. Refused, not applied."""

    @property
    def rows_written(self) -> int:
        return self.opened


def represents_current_state(load_strategy: str) -> bool:
    """Whether a key's absence from Bronze means it was deleted.

    True for full reload and CDC, where Bronze mirrors the source as it
    stands. False for incremental, where Bronze only ever grows.
    """
    return load_strategy in (LoadStrategy.FULL, LoadStrategy.CDC)


def _as_python(column: pa.ChunkedArray) -> list[object]:
    """Read a column into Python without needing a timezone database.

    Converting a timezone-aware Arrow timestamp to a `datetime` requires
    an IANA timezone database, which Windows does not ship — so any
    column read this way is reduced to epoch microseconds first. Hashing
    and key comparison only need values that are stable and comparable,
    and integers are both.
    """
    if pa.types.is_timestamp(column.type):
        column = column.cast(pa.int64())
    return column.to_pylist()  # type: ignore[no-any-return]


def hashable_columns(table: pa.Table, keys: list[str]) -> list[str]:
    """Business columns that participate in change detection.

    Keys are excluded because they identify the row rather than describe
    it, provenance because it changes every run by design, and the
    history columns because they are this module's own bookkeeping.
    """
    excluded = set(keys) | _PROVENANCE | set(HISTORY_COLUMNS)
    return sorted(c for c in table.column_names if c not in excluded)


def row_hashes(table: pa.Table, columns: list[str]) -> pa.Array:
    """One stable hash per row over the given columns.

    Columns are read in sorted order so the hash does not depend on the
    source's column ordering, and nulls get an explicit marker so that a
    null and an empty string do not collide.
    """
    if not columns:
        return pa.array([""] * table.num_rows, type=pa.string())

    values = [_as_python(table.column(c)) for c in columns]
    digests: list[str] = []
    for i in range(table.num_rows):
        joined = _FIELD_SEPARATOR.join(_NULL_MARKER if v[i] is None else str(v[i]) for v in values)
        digests.append(hashlib.sha256(joined.encode("utf-8")).hexdigest())
    return pa.array(digests, type=pa.string())


def effective_times(table: pa.Table, column: str, fallback: datetime) -> pa.Array:
    """When each incoming row became true.

    Uses the configured column when it holds a timestamp. An integer or
    string incremental column orders rows perfectly well but says
    nothing about *when*, so those date from the run instead.
    """
    if column in table.column_names and pa.types.is_timestamp(table.schema.field(column).type):
        return table.column(column).cast(TIMESTAMP).combine_chunks()
    return pa.array([fallback] * table.num_rows, type=TIMESTAMP)


def with_history(
    table: pa.Table,
    hashes: pa.Array,
    valid_from: pa.Array,
    *,
    is_deleted: bool,
) -> pa.Table:
    """Attach the five SCD2 columns to a batch of business rows."""
    n = table.num_rows
    return (
        table.append_column(VALID_FROM, valid_from)
        .append_column(VALID_TO, pa.nulls(n, TIMESTAMP))
        .append_column(IS_CURRENT, pa.array([True] * n, type=pa.bool_()))
        .append_column(IS_DELETED, pa.array([is_deleted] * n, type=pa.bool_()))
        .append_column(ROW_HASH, hashes)
    )


def _key_tuples(table: pa.Table, keys: list[str]) -> list[tuple[object, ...]]:
    columns = [_as_python(table.column(k)) for k in keys]
    return [tuple(col[i] for col in columns) for i in range(table.num_rows)]


def _check_schema(incoming: pa.Table, history: pa.Table) -> None:
    """Refuse a batch whose columns no longer match the history table.

    Evolving the history table is schema drift, which is step 26's job
    and needs a policy. Failing loudly here is better than a MERGE that
    half-applies or a silently widened table.
    """
    existing = {c for c in history.column_names if c not in HISTORY_COLUMNS}
    arriving = set(incoming.column_names)
    added = sorted(arriving - existing)
    removed = sorted(existing - arriving)
    if added or removed:
        detail = []
        if added:
            detail.append(f"added: {', '.join(added)}")
        if removed:
            detail.append(f"removed: {', '.join(removed)}")
        raise ValueError(
            "incoming schema does not match the SCD2 history table "
            f"({'; '.join(detail)}). Schema evolution is not yet supported."
        )


def _merge_predicate(keys: list[str]) -> str:
    conditions = [f"t.{k} = s.{MERGE_PREFIX}{k}" for k in keys]
    conditions.append(f"t.{IS_CURRENT} = true")
    return " AND ".join(conditions)


def _with_merge_keys(table: pa.Table, keys: list[str], *, matching: bool) -> pa.Table:
    """Add the merge-key columns that decide insert-versus-close.

    This is the two-copy MERGE: the same new version appears twice in the
    source. The copy carrying real key values matches the open row and
    closes it; the copy carrying nulls matches nothing and is inserted.
    One statement, one commit, both halves or neither.
    """
    for key in keys:
        column = (
            table.column(key)
            if matching
            else pa.nulls(table.num_rows, table.schema.field(key).type)
        )
        table = table.append_column(f"{MERGE_PREFIX}{key}", column)
    return table


def apply_scd2(
    target: Path,
    incoming: pa.Table,
    keys: list[str],
    effective_column: str,
    run_time: datetime,
    *,
    allow_deletes: bool,
) -> Scd2Outcome:
    """Fold a deduplicated, conformed batch into the SCD2 history table.

    `incoming` must already carry one row per key — Silver's dedupe does
    that before this is called.
    """
    hash_cols = hashable_columns(incoming, keys)
    hashes = row_hashes(incoming, hash_cols).to_pylist()
    effective = effective_times(incoming, effective_column, run_time)

    if not (target / "_delta_log").exists():
        versions = with_history(
            incoming, pa.array(hashes, type=pa.string()), effective, is_deleted=False
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(str(target), versions, mode="overwrite", schema_mode="overwrite")
        return Scd2Outcome(
            opened=incoming.num_rows, closed=0, tombstoned=0, unchanged=0, late_skipped=0
        )

    history = DeltaTable(str(target)).to_pyarrow_table()
    _check_schema(incoming, history)
    open_rows = history.filter(pc.equal(history.column(IS_CURRENT), True))

    open_index = {key: i for i, key in enumerate(_key_tuples(open_rows, keys))}
    open_hash = open_rows.column(ROW_HASH).to_pylist()
    open_deleted = open_rows.column(IS_DELETED).to_pylist()
    # Compared as epoch microseconds, never as Python datetimes.
    # Converting a timezone-aware Arrow timestamp to Python needs an IANA
    # timezone database, which Windows does not ship. Integers order
    # identically, need no such lookup, and skip building a datetime per
    # row into the bargain.
    open_from = open_rows.column(VALID_FROM).cast(pa.int64()).to_pylist()

    incoming_keys = _key_tuples(incoming, keys)
    effective_values = effective.cast(pa.int64()).to_pylist()

    to_open: list[int] = []
    chosen: list[int] = []
    unchanged = 0
    late_skipped = 0

    for i, key in enumerate(incoming_keys):
        existing = open_index.get(key)
        if existing is None:
            to_open.append(i)
            chosen.append(effective_values[i])
            continue
        if hashes[i] == open_hash[existing] and not open_deleted[existing]:
            unchanged += 1
            continue
        if open_deleted[existing]:
            # A resurrection is later news than the deletion by
            # definition, whatever the source's business clock says. The
            # tombstone was dated at processing time, so a business
            # timestamp behind it would otherwise run the interval
            # backwards; nudge past it instead of refusing.
            to_open.append(i)
            chosen.append(max(effective_values[i], open_from[existing] + _MICROSECOND))
            continue
        if effective_values[i] <= open_from[existing]:
            # The open version is already newer. Rewriting history
            # retroactively is out of scope; refuse rather than create
            # an interval that runs backwards.
            late_skipped += 1
            continue
        to_open.append(i)
        chosen.append(effective_values[i])

    business = [c for c in history.column_names if c not in HISTORY_COLUMNS]
    changes = with_history(
        incoming.take(pa.array(to_open, type=pa.int64())).select(business),
        pa.array([hashes[i] for i in to_open], type=pa.string()),
        pa.array(chosen, type=pa.int64()).cast(TIMESTAMP),
        is_deleted=False,
    )
    # A new key has no open row to close; a change or a resurrection does.
    supersedes = [position for position, i in enumerate(to_open) if incoming_keys[i] in open_index]

    tombstones = _tombstones(
        open_rows, open_index, set(incoming_keys), business, run_time, allow_deletes=allow_deletes
    )

    new_versions = pa.concat_tables([changes, tombstones]).combine_chunks()
    if new_versions.num_rows == 0:
        return Scd2Outcome(
            opened=0, closed=0, tombstoned=0, unchanged=unchanged, late_skipped=late_skipped
        )

    # Every tombstone closes a row by definition; only some changes do.
    closing_rows = pa.concat_tables(
        [changes.take(pa.array(supersedes, type=pa.int64())), tombstones]
    ).combine_chunks()

    source = pa.concat_tables(
        [
            _with_merge_keys(closing_rows, keys, matching=True),
            _with_merge_keys(new_versions, keys, matching=False),
        ]
    ).combine_chunks()

    DeltaTable(str(target)).merge(
        source=source,
        predicate=_merge_predicate(keys),
        source_alias="s",
        target_alias="t",
    ).when_matched_update(
        updates={VALID_TO: f"s.{VALID_FROM}", IS_CURRENT: "false"}
    ).when_not_matched_insert(updates={c: f"s.{c}" for c in new_versions.column_names}).execute()

    return Scd2Outcome(
        opened=new_versions.num_rows,
        closed=closing_rows.num_rows,
        tombstoned=tombstones.num_rows,
        unchanged=unchanged,
        late_skipped=late_skipped,
    )


def _tombstones(
    open_rows: pa.Table,
    open_index: dict[tuple[object, ...], int],
    arriving: set[tuple[object, ...]],
    business: list[str],
    run_time: datetime,
    *,
    allow_deletes: bool,
) -> pa.Table:
    """Closing versions for keys that vanished from the source."""
    empty = with_history(
        open_rows.select(business).slice(0, 0),
        pa.array([], type=pa.string()),
        pa.array([], type=TIMESTAMP),
        is_deleted=True,
    )
    if not allow_deletes:
        return empty

    deleted = open_rows.column(IS_DELETED).to_pylist()
    gone = [
        position
        for key, position in open_index.items()
        if key not in arriving and not deleted[position]
    ]
    if not gone:
        return empty

    rows = open_rows.take(pa.array(gone, type=pa.int64())).select(business)
    return with_history(
        rows,
        open_rows.column(ROW_HASH).take(pa.array(gone, type=pa.int64())).combine_chunks(),
        pa.array([run_time] * len(gone), type=TIMESTAMP),
        is_deleted=True,
    )
