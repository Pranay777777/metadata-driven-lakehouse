"""Gold: the star schema analysts actually query.

Silver is correct but shaped like the source. Gold is shaped like the
questions — a fact table of measurements surrounded by dimensions that
describe them, joined on surrogate keys rather than whatever identifier
the source system happened to use.

Which objects are published, and which fact column points at which
dimension, is configuration: `source_object.gold_role` and the
`gold_reference` table. Nothing here knows what `orders` is.

Three decisions worth stating:

**Surrogate keys are derived, not sequenced.** A key is a hash of the
natural key — plus `valid_from` for an SCD2 dimension, because each
version needs its own key. A sequence would need a generator, a
persisted counter and a story about what happens when Gold is rebuilt.
Gold is derived data and is rebuilt in full on every run, so its keys
must be reproducible from the input alone.

**Every dimension carries an unknown member at key 0.** A fact row whose
dimension member has not loaded yet still has to join, or it silently
vanishes from every report. Pointing it at the unknown member keeps the
row, keeps the measure in the totals, and makes the gap countable.

**Facts join SCD2 dimensions at the event time.** The point of keeping
history is answering what was true *then*. Joining a historical fact to
today's dimension version throws that away, so the lookup selects the
version whose validity interval contains the fact's event time.
"""

from __future__ import annotations

import hashlib
import time
from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from sqlalchemy import select
from sqlalchemy.orm import Session

from lakehouse.lineage import Recording, default_lineage, lake_dataset, renamed_lineage
from lakehouse.metadata.enums import GoldRole, Layer, RunStatus
from lakehouse.metadata.models import GoldReference, PipelineRun, SourceObject, TaskRun
from lakehouse.privacy import drop_restricted, load_policies
from lakehouse.tables import require_columns, to_snake_case
from lakehouse.transform.scd2 import IS_CURRENT, IS_DELETED, VALID_FROM
from lakehouse.transform.silver import read_silver

UNKNOWN_KEY = 0
"""Surrogate of the unknown member. Reserved; never produced by a hash."""

SURROGATE_SUFFIX = "_key"
_HASH_BYTES = 8
_SEPARATOR = "\x1f"
_NULL_MARKER = "\x00"


@dataclass(frozen=True)
class GoldResult:
    """Outcome of one Gold build."""

    run_id: str
    object_name: str
    role: str
    rows_read: int
    rows_written: int
    delta_version: int
    duration_seconds: float
    unresolved: dict[str, int] | None = None
    """Per fact column, how many rows fell back to the unknown member."""

    restricted: tuple[str, ...] = ()
    """Columns withheld from Gold because they are `sensitive_pii` and
    nobody has allow-listed them."""

    @property
    def unresolved_total(self) -> int:
        return sum((self.unresolved or {}).values())


def gold_path(obj: SourceObject) -> str:
    """Where an object lands in Gold, prefixed by its role."""
    prefix = "fact" if obj.gold_role == GoldRole.FACT else "dim"
    return f"{Layer.GOLD}/{prefix}_{to_snake_case(obj.object_name)}"


def silver_source(obj: SourceObject) -> str:
    """The Silver table a Gold object is published from."""
    from lakehouse.transform.silver import silver_path

    return silver_path(obj)


def surrogate_column(obj: SourceObject) -> str:
    """The surrogate key column a dimension exposes."""
    return f"{to_snake_case(obj.object_name)}{SURROGATE_SUFFIX}"


def natural_keys(obj: SourceObject) -> list[str]:
    """Conformed natural-key columns, as they appear in Silver."""
    if not obj.primary_key_columns:
        raise ValueError(f"'{obj.object_name}' needs primary_key_columns to be published to Gold")
    return [to_snake_case(c.strip()) for c in obj.primary_key_columns.split(",") if c.strip()]


def surrogate(values: tuple[object, ...]) -> int:
    """A stable, positive surrogate key derived from its inputs.

    Truncated to 63 bits so it fits a signed 64-bit integer, and offset
    away from zero so it can never collide with the unknown member.
    """
    joined = _SEPARATOR.join(_NULL_MARKER if v is None else str(v) for v in values)
    digest = hashlib.blake2b(joined.encode("utf-8"), digest_size=_HASH_BYTES).digest()
    return (int.from_bytes(digest, "big") >> 1) + 1


def _column_values(table: pa.Table, name: str) -> list[object]:
    column = table.column(name)
    if pa.types.is_timestamp(column.type):
        column = column.cast(pa.int64())
    return column.to_pylist()  # type: ignore[no-any-return]


def _row_tuples(table: pa.Table, columns: list[str]) -> list[tuple[object, ...]]:
    values = [_column_values(table, c) for c in columns]
    return [tuple(v[i] for v in values) for i in range(table.num_rows)]


def dimension_keys(table: pa.Table, keys: list[str], *, versioned: bool) -> pa.Array:
    """Surrogate key per dimension row.

    An SCD2 dimension folds `valid_from` into the key, because each
    version of a member is a distinct row that facts must be able to
    point at individually.
    """
    columns = [*keys, VALID_FROM] if versioned else list(keys)
    return pa.array([surrogate(row) for row in _row_tuples(table, columns)], type=pa.int64())


def _unknown_member(schema: pa.Schema, key_column: str) -> pa.Table:
    """A single all-null row at key 0, so no fact loses its join."""
    arrays = [
        pa.array([UNKNOWN_KEY], type=pa.int64())
        if field.name == key_column
        else pa.nulls(1, field.type)
        for field in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def build_dimension(
    session: Session, run: PipelineRun, obj: SourceObject, lake_root: Path
) -> GoldResult:
    """Publish a Silver object as a dimension with surrogate keys."""
    started, task = _start(session, run, obj)
    lineage = default_lineage()
    job = f"{Layer.GOLD}.dim_{obj.object_name}"
    lineage_run = lineage.begin(job)
    try:
        silver = read_silver(lake_root, obj)
        keys = natural_keys(obj)
        require_columns(silver, keys, f"silver table for '{obj.object_name}'")

        # The most sensitive columns do not reach the layer analysts
        # query unless somebody said so explicitly. Natural keys survive:
        # a dimension without its business key is not a dimension, and
        # masking already covered the value itself (ADR-017).
        silver, restricted = drop_restricted(silver, load_policies(session, obj), keep=keys)

        keyed = silver.append_column(
            surrogate_column(obj),
            dimension_keys(silver, keys, versioned=obj.scd2_enabled),
        )
        published = pa.concat_tables(
            [_unknown_member(keyed.schema, surrogate_column(obj)), keyed]
        ).combine_chunks()

        target = lake_root / gold_path(obj)
        target.parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(str(target), published, mode="overwrite", schema_mode="overwrite")
        version = DeltaTable(str(target)).version()
    except Exception as exc:
        _fail(session, task, exc)
        lineage.finish(job, lineage_run, error=f"{type(exc).__name__}: {exc}")
        raise

    upstream = lake_dataset(silver_source(obj))
    recording = Recording()
    recording.reads(upstream)
    recording.writes(
        lake_dataset(
            gold_path(obj),
            published.schema,
            # The surrogate is derived from the natural key, so that is
            # the edge worth recording; everything else passes through.
            column_lineage={
                **renamed_lineage(upstream, {c: c for c in silver.column_names}),
                surrogate_column(obj): [(upstream.namespace, upstream.name, k) for k in keys],
            },
        )
    )
    lineage.finish(job, lineage_run, recording)

    return _finish(
        session,
        task,
        started,
        GoldResult(
            run_id=run.run_id,
            object_name=obj.object_name,
            role=GoldRole.DIMENSION,
            rows_read=silver.num_rows,
            rows_written=published.num_rows,
            delta_version=version,
            duration_seconds=0.0,
            restricted=restricted,
        ),
    )


@dataclass(frozen=True)
class _Lookup:
    """Natural key to surrogate, with optional validity intervals."""

    versioned: bool
    current: dict[tuple[object, ...], int]
    history: dict[tuple[object, ...], tuple[list[int], list[int]]]
    """Key to (sorted valid_from values, matching surrogates)."""

    def resolve(self, key: tuple[object, ...], at: int | None) -> int:
        if not self.versioned or at is None:
            return self.current.get(key, UNKNOWN_KEY)
        versions = self.history.get(key)
        if versions is None:
            return UNKNOWN_KEY
        starts, keys = versions
        position = bisect_right(starts, at)
        if position == 0:
            # The fact predates every version of this member.
            return UNKNOWN_KEY
        return keys[position - 1]


def _build_lookup(lake_root: Path, dimension: SourceObject) -> _Lookup:
    table = read_silver(lake_root, dimension)
    keys = natural_keys(dimension)
    surrogates = dimension_keys(table, keys, versioned=dimension.scd2_enabled).to_pylist()
    rows = _row_tuples(table, keys)

    if not dimension.scd2_enabled:
        return _Lookup(False, dict(zip(rows, surrogates, strict=True)), {})

    starts = _column_values(table, VALID_FROM)
    deleted = table.column(IS_DELETED).to_pylist()
    current_flag = table.column(IS_CURRENT).to_pylist()

    history: dict[tuple[object, ...], tuple[list[int], list[int]]] = {}
    current: dict[tuple[object, ...], int] = {}
    ordered = sorted(range(len(rows)), key=lambda i: (starts[i] is None, starts[i]))
    for i in ordered:
        start = starts[i]
        if deleted[i] or not isinstance(start, int):
            continue
        bucket = history.setdefault(rows[i], ([], []))
        bucket[0].append(int(start))
        bucket[1].append(surrogates[i])
        if current_flag[i]:
            current[rows[i]] = surrogates[i]
    return _Lookup(True, current, history)


def build_fact(
    session: Session, run: PipelineRun, obj: SourceObject, lake_root: Path
) -> GoldResult:
    """Publish a Silver object as a fact, natural keys swapped for surrogates."""
    started, task = _start(session, run, obj)
    lineage = default_lineage()
    job = f"{Layer.GOLD}.fact_{obj.object_name}"
    lineage_run = lineage.begin(job)
    try:
        silver = read_silver(lake_root, obj)
        references = list(
            session.scalars(select(GoldReference).where(GoldReference.fact_object_id == obj.id))
        )
        event_times = _event_times(silver, obj)

        table = silver
        unresolved: dict[str, int] = {}
        for reference in references:
            dimension = session.get(SourceObject, reference.dimension_object_id)
            if dimension is None:
                raise ValueError(f"gold_reference {reference.id} points at a missing dimension")
            column = to_snake_case(reference.fact_column)
            require_columns(table, [column], f"silver table for '{obj.object_name}'")

            lookup = _build_lookup(lake_root, dimension)
            values = _column_values(table, column)
            resolved = [
                lookup.resolve((values[i],), event_times[i] if event_times else None)
                for i in range(table.num_rows)
            ]
            unresolved[column] = sum(1 for key in resolved if key == UNKNOWN_KEY)

            table = table.drop_columns([column]).append_column(
                surrogate_column(dimension), pa.array(resolved, type=pa.int64())
            )

        table, restricted = drop_restricted(table, load_policies(session, obj))

        target = lake_root / gold_path(obj)
        target.parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(str(target), table, mode="overwrite", schema_mode="overwrite")
        version = DeltaTable(str(target)).version()
    except Exception as exc:
        _fail(session, task, exc)
        lineage.finish(job, lineage_run, error=f"{type(exc).__name__}: {exc}")
        raise

    upstream = lake_dataset(silver_source(obj))
    recording = Recording()
    recording.reads(upstream)
    recording.writes(lake_dataset(gold_path(obj), table.schema))
    lineage.finish(job, lineage_run, recording)

    return _finish(
        session,
        task,
        started,
        GoldResult(
            run_id=run.run_id,
            object_name=obj.object_name,
            role=GoldRole.FACT,
            rows_read=silver.num_rows,
            rows_written=table.num_rows,
            delta_version=version,
            duration_seconds=0.0,
            unresolved=unresolved,
            restricted=restricted,
        ),
    )


def _event_times(table: pa.Table, obj: SourceObject) -> list[int] | None:
    """When each fact row happened, as epoch microseconds.

    Without one, an SCD2 dimension can only be joined at its current
    version — correct for a fact loaded in real time, wrong for a
    back-fill, and worth being explicit about either way.
    """
    if not obj.incremental_column:
        return None
    column = to_snake_case(obj.incremental_column)
    if column not in table.column_names:
        return None
    if not pa.types.is_timestamp(table.schema.field(column).type):
        return None
    return [int(v) for v in _column_values(table, column) if isinstance(v, int)]


def build_gold(
    session: Session, run: PipelineRun, obj: SourceObject, lake_root: Path
) -> GoldResult:
    """Publish one object according to its configured role.

    Dimensions must be built before the facts that reference them; the
    caller owns that ordering.
    """
    if obj.gold_role == GoldRole.DIMENSION:
        return build_dimension(session, run, obj, lake_root)
    if obj.gold_role == GoldRole.FACT:
        return build_fact(session, run, obj, lake_root)
    raise ValueError(f"'{obj.object_name}' has no gold_role, so it is not published to Gold")


def read_gold(lake_root: Path, obj: SourceObject) -> pa.Table:
    """Read a published Gold table."""
    return DeltaTable(str(lake_root / gold_path(obj))).to_pyarrow_table()


def _start(session: Session, run: PipelineRun, obj: SourceObject) -> tuple[float, TaskRun]:
    task = TaskRun(
        run_id=run.run_id,
        source_object_id=obj.id,
        layer=Layer.GOLD,
        status=RunStatus.RUNNING,
    )
    session.add(task)
    session.commit()
    return time.perf_counter(), task


def _fail(session: Session, task: TaskRun, exc: Exception) -> None:
    task.status = RunStatus.FAILED
    task.error_message = f"{type(exc).__name__}: {exc}"
    task.ended_at = datetime.now(UTC)
    session.commit()


def _finish(session: Session, task: TaskRun, started: float, result: GoldResult) -> GoldResult:
    elapsed = time.perf_counter() - started
    task.status = RunStatus.SUCCEEDED
    task.rows_read = result.rows_read
    task.rows_written = result.rows_written
    task.rows_rejected = result.unresolved_total
    task.ended_at = datetime.now(UTC)
    task.duration_seconds = int(elapsed)
    session.commit()
    return GoldResult(
        run_id=result.run_id,
        object_name=result.object_name,
        role=result.role,
        rows_read=result.rows_read,
        rows_written=result.rows_written,
        delta_version=result.delta_version,
        duration_seconds=elapsed,
        unresolved=result.unresolved,
        restricted=result.restricted,
    )
