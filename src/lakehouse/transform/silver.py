"""Silver: conformed, deduplicated data.

Bronze is deliberately messy — it is whatever the source said, including
rows re-read by the grace window and redelivered by a flaky feed. Silver
is the first layer anyone should query, which means it owes two
guarantees Bronze does not:

**One row per key.** Duplicates arrive from three directions: the source
redelivering, the grace window re-reading, and a retried run appending
again. All three look identical here and all three are removed the same
way — keep the newest row per key.

**Conformed shape.** Column names normalised, string padding stripped,
epoch integers turned into UTC timestamps. None of this is business
logic; it is making the data queryable without every consumer
re-implementing the same cleanup.

What Silver deliberately does *not* do: joins, aggregation, business
rules. Those belong in Gold. A Silver table maps one-to-one to a source
object, which keeps lineage legible.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import INGESTED_AT, read_bronze
from lakehouse.metadata.enums import Layer, RunStatus
from lakehouse.metadata.models import PipelineRun, SourceObject, TaskRun
from lakehouse.quality import QualityOutcome, evaluate, write_quarantine
from lakehouse.tables import (
    epoch_to_timestamp,
    latest_per_key,
    rename_snake_case,
    require_columns,
    to_snake_case,
    trim_strings,
)
from lakehouse.transform.scd2 import Scd2Outcome, apply_scd2, represents_current_state

TIMESTAMP_SUFFIXES = ("_at", "_date", "_time")


@dataclass(frozen=True)
class SilverResult:
    """Outcome of one Silver build."""

    run_id: str
    object_name: str
    rows_read: int
    rows_written: int
    delta_version: int
    duration_seconds: float
    scd2: Scd2Outcome | None = None
    """Version bookkeeping, present only when the object tracks history."""

    quality: QualityOutcome | None = None
    """Rule results for this build. Quarantined rows are in `quality.rejected`."""

    @property
    def duplicates_removed(self) -> int:
        """Rows the dedupe collapsed.

        Meaningful on the snapshot path, where `rows_written` is the
        whole batch. Under SCD2 `rows_written` counts new *versions*, so
        read `scd2` instead.
        """
        return self.rows_read - self.rows_written


def silver_path(obj: SourceObject) -> str:
    """Mirror the Bronze path into the silver prefix.

    Bronze paths start with `bronze/`; the Silver table sits at the same
    relative location one layer up. Keeping the derivation in one place
    means the convention is stated once rather than assumed everywhere.
    """
    parts = obj.target_path.split("/")
    if parts[0] == Layer.BRONZE:
        parts[0] = Layer.SILVER
        return "/".join(parts)
    return f"{Layer.SILVER}/{obj.target_path}"


def timestamp_columns(table: pa.Table) -> list[str]:
    """Integer columns whose name implies they carry a moment in time."""
    return [
        field.name
        for field in table.schema
        if pa.types.is_integer(field.type) and field.name.endswith(TIMESTAMP_SUFFIXES)
    ]


def dedupe_keys(obj: SourceObject, table: pa.Table) -> list[str]:
    """Columns that identify a row.

    Uses the configured primary key when there is one. Without it there
    is no principled way to decide which of two similar rows is newer,
    so every non-provenance column is used — which deduplicates exact
    repeats and nothing else.
    """
    if obj.primary_key_columns:
        keys = [c.strip() for c in obj.primary_key_columns.split(",") if c.strip()]
        require_columns(table, keys, f"bronze table for '{obj.object_name}'")
        return keys
    return [c for c in table.column_names if not c.startswith("_")]


def conform(table: pa.Table) -> pa.Table:
    """Apply the shape rules: names, whitespace, timestamps."""
    table = rename_snake_case(table)
    table = trim_strings(table)
    return epoch_to_timestamp(table, timestamp_columns(table))


def build_silver(
    session: Session,
    run: PipelineRun,
    obj: SourceObject,
    lake_root: Path,
) -> SilverResult:
    """Read Bronze, deduplicate and conform it, write Silver.

    Silver is rebuilt in full each run rather than appended to. That is
    affordable because Silver is derived — it can always be recomputed
    from Bronze — and it removes a whole class of incremental-merge bugs.
    """
    started = time.perf_counter()
    task = TaskRun(
        run_id=run.run_id,
        source_object_id=obj.id,
        layer=Layer.SILVER,
        status=RunStatus.RUNNING,
    )
    session.add(task)
    session.commit()

    try:
        bronze = read_bronze(lake_root, obj)
        keys = dedupe_keys(obj, bronze)
        sequence = obj.incremental_column if obj.incremental_column else INGESTED_AT
        require_columns(bronze, [sequence], f"bronze table for '{obj.object_name}'")

        deduped = latest_per_key(bronze, keys, sequence)
        conformed = conform(deduped)

        # Quality is enforced here rather than at Bronze: ADR-004 makes
        # Bronze deliberately faithful to the source, messiness included.
        # Silver is the first layer anyone should query, so it is the
        # first layer that owes any guarantee about its contents.
        quality = evaluate(session, task, obj, conformed)
        _dropped = bronze.num_rows - deduped.num_rows
        # Diverted rows are written before the layer itself, so a crash
        # between the two loses the load rather than the evidence.
        write_quarantine(lake_root, obj, quality.rejected, run.run_id, task.id, Layer.SILVER)
        conformed = quality.kept

        target = lake_root / silver_path(obj)
        target.parent.mkdir(parents=True, exist_ok=True)

        if obj.scd2_enabled:
            outcome = apply_scd2(
                target,
                conformed,
                [to_snake_case(k) for k in keys],
                to_snake_case(sequence),
                datetime.now(UTC),
                allow_deletes=represents_current_state(obj.load_strategy),
            )
            written = outcome.rows_written
            rejected = _dropped + quality.rows_rejected + outcome.late_skipped
        else:
            write_deltalake(str(target), conformed, mode="overwrite", schema_mode="overwrite")
            outcome = None
            written = conformed.num_rows
            rejected = _dropped + quality.rows_rejected

        version = DeltaTable(str(target)).version()
    except Exception as exc:
        task.status = RunStatus.FAILED
        task.error_message = f"{type(exc).__name__}: {exc}"
        task.ended_at = datetime.now(UTC)
        session.commit()
        raise

    elapsed = time.perf_counter() - started
    task.status = quality.status
    task.rows_read = bronze.num_rows
    task.rows_written = written
    task.rows_rejected = rejected
    task.ended_at = datetime.now(UTC)
    task.duration_seconds = int(elapsed)
    session.commit()

    return SilverResult(
        run_id=run.run_id,
        object_name=obj.object_name,
        rows_read=bronze.num_rows,
        rows_written=written,
        delta_version=version,
        duration_seconds=elapsed,
        scd2=outcome,
        quality=quality,
    )


def read_silver(lake_root: Path, obj: SourceObject, version: int | None = None) -> pa.Table:
    """Read a Silver table, optionally at an earlier version."""
    target = str(lake_root / silver_path(obj))
    dt = DeltaTable(target) if version is None else DeltaTable(target, version=version)
    return dt.to_pyarrow_table()
