"""Change-data-capture ingestion via Delta MERGE.

A CDC feed carries *changes*, not state: inserts, updates and deletes,
often several for the same key in one batch. Two things have to be right
or the target drifts from the source.

**Collapse before merging.** If a batch contains three changes for key
42, the merge must apply only the newest. Delta's MERGE raises on
multiple source rows matching one target row, and even where it does
not, the winner would be arbitrary. So the change set is reduced to one
row per key, ordered by the sequence column, before it reaches MERGE.

**Deletes are data.** A feed that signals a delete and is treated as
upsert-only leaves the row in the lake forever. Which column carries the
operation, and which value means delete, differ per feed — so both are
configuration, not convention.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import Source, add_provenance
from lakehouse.metadata.enums import Layer, LoadStrategy, RunStatus
from lakehouse.metadata.models import PipelineRun, SourceObject, TaskRun
from lakehouse.tables import latest_per_key, require_columns


@dataclass(frozen=True)
class MergeResult:
    """Row-level outcome of one CDC batch."""

    run_id: str
    object_name: str
    rows_read: int
    rows_after_collapse: int
    inserted: int
    updated: int
    deleted: int
    delta_version: int
    duration_seconds: float

    @property
    def collapsed(self) -> int:
        """Superseded changes discarded before the merge."""
        return self.rows_read - self.rows_after_collapse

    @property
    def rows_written(self) -> int:
        return self.inserted + self.updated + self.deleted


def key_columns(obj: SourceObject) -> list[str]:
    """Parse the configured primary key into column names."""
    if not obj.primary_key_columns:
        raise ValueError(f"'{obj.object_name}' has no primary_key_columns configured")
    return [c.strip() for c in obj.primary_key_columns.split(",") if c.strip()]


def collapse_changes(table: pa.Table, keys: list[str], sequence_column: str) -> pa.Table:
    """Reduce a change batch to the newest row per key.

    Thin wrapper over the shared helper — Silver performs the identical
    operation for a different reason, so the logic lives in one place.
    """
    require_columns(table, [*keys, sequence_column], "change feed")
    return latest_per_key(table, keys, sequence_column)


def split_deletes(table: pa.Table, obj: SourceObject) -> tuple[pa.Table, pa.Table]:
    """Separate deletes from upserts using the configured operation column."""
    column = obj.cdc_operation_column
    if column is None:
        return table, table.slice(0, 0)
    if column not in table.column_names:
        raise KeyError(
            f"cdc_operation_column '{column}' not present in the change feed "
            f"for '{obj.object_name}'"
        )
    is_delete = pc.equal(table.column(column), pa.scalar(obj.cdc_delete_value))
    return table.filter(pc.invert(is_delete)), table.filter(is_delete)


def _predicate(keys: list[str]) -> str:
    return " AND ".join(f"t.{k} = s.{k}" for k in keys)


def load_cdc(
    session: Session,
    run: PipelineRun,
    obj: SourceObject,
    source: Source,
    lake_root: Path,
    sequence_column: str | None = None,
) -> MergeResult:
    """Apply a change batch to Bronze with Delta MERGE.

    Args:
        sequence_column: orders changes within the batch. Defaults to the
            object's `incremental_column`.

    Raises:
        ValueError: if the object is not configured for CDC.
    """
    if obj.load_strategy != LoadStrategy.CDC:
        raise ValueError(f"'{obj.object_name}' is configured as '{obj.load_strategy}', not 'cdc'")

    keys = key_columns(obj)
    sequence = sequence_column or obj.incremental_column
    if sequence is None:
        raise ValueError(
            f"'{obj.object_name}' needs an incremental_column (or an explicit "
            "sequence_column) so changes can be ordered"
        )

    started = time.perf_counter()
    task = TaskRun(
        run_id=run.run_id,
        source_object_id=obj.id,
        layer=Layer.BRONZE,
        status=RunStatus.RUNNING,
    )
    session.add(task)
    session.commit()

    try:
        raw = source.read(obj)
        collapsed = collapse_changes(raw, keys, sequence)
        upserts, _deletes = split_deletes(collapsed, obj)
        target = lake_root / obj.target_path
        target.parent.mkdir(parents=True, exist_ok=True)

        if not (target / "_delta_log").exists():
            inserted, updated, deleted = _initial_write(target, upserts, run.run_id, obj)
        else:
            inserted, updated, deleted = _merge(target, collapsed, keys, obj, run.run_id)
        version = DeltaTable(str(target)).version()
    except Exception as exc:
        task.status = RunStatus.FAILED
        task.error_message = f"{type(exc).__name__}: {exc}"
        task.ended_at = datetime.now(UTC)
        session.commit()
        raise

    elapsed = time.perf_counter() - started
    task.status = RunStatus.SUCCEEDED
    task.rows_read = raw.num_rows
    task.rows_written = inserted + updated + deleted
    task.rows_rejected = raw.num_rows - collapsed.num_rows
    task.ended_at = datetime.now(UTC)
    task.duration_seconds = int(elapsed)
    session.commit()

    return MergeResult(
        run_id=run.run_id,
        object_name=obj.object_name,
        rows_read=raw.num_rows,
        rows_after_collapse=collapsed.num_rows,
        inserted=inserted,
        updated=updated,
        deleted=deleted,
        delta_version=version,
        duration_seconds=elapsed,
    )


def _initial_write(
    target: Path, upserts: pa.Table, run_id: str, obj: SourceObject
) -> tuple[int, int, int]:
    """First batch: there is nothing to merge into.

    Deletes are dropped rather than applied — a delete for a row that
    was never loaded is a no-op, not an error.
    """
    stamped = add_provenance(upserts, run_id, obj.object_name)
    write_deltalake(str(target), stamped, mode="overwrite", schema_mode="overwrite")
    return stamped.num_rows, 0, 0


def _merge(
    target: Path, changes: pa.Table, keys: list[str], obj: SourceObject, run_id: str
) -> tuple[int, int, int]:
    """Apply upserts and deletes in a single atomic MERGE."""
    stamped = add_provenance(changes, run_id, obj.object_name)
    updatable = [c for c in stamped.column_names if c not in keys]

    builder = DeltaTable(str(target)).merge(
        source=stamped,
        predicate=_predicate(keys),
        source_alias="s",
        target_alias="t",
    )
    if obj.cdc_operation_column:
        builder = builder.when_matched_delete(
            predicate=f"s.{obj.cdc_operation_column} = '{obj.cdc_delete_value}'"
        )
    builder = builder.when_matched_update(updates={c: f"s.{c}" for c in updatable})
    insert_predicate = (
        f"s.{obj.cdc_operation_column} <> '{obj.cdc_delete_value}'"
        if obj.cdc_operation_column
        else None
    )
    builder = builder.when_not_matched_insert(
        updates={c: f"s.{c}" for c in stamped.column_names},
        predicate=insert_predicate,
    )

    metrics = builder.execute()
    return (
        int(metrics.get("num_target_rows_inserted", 0)),
        int(metrics.get("num_target_rows_updated", 0)),
        int(metrics.get("num_target_rows_deleted", 0)),
    )
