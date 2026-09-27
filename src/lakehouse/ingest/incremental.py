"""Incremental ingestion driven by a persisted watermark.

The rule that makes this safe: **the watermark advances only after the
write succeeds.** A run that dies between reading and writing leaves the
watermark where it was, so the next run re-reads the same window. That
costs a few duplicate rows, which Silver deduplicates. The opposite
ordering — advancing first — loses rows permanently, and nobody notices
until someone reconciles a total months later.

Late-arriving rows — ones that appear with an `updated_at` *below* the
current watermark — are invisible to a strict `>` comparison. The fix is
`watermark_grace`: read from `watermark - grace` instead of `watermark`.
That re-reads rows already loaded, which is the deliberate trade. Bronze
is append-only, so the duplicates land there and Silver removes them;
`rows_reprocessed` on the result makes the cost visible rather than
hidden.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import Source, add_provenance
from lakehouse.metadata.drift import check_drift
from lakehouse.metadata.enums import Layer, LoadStrategy, RunStatus, WatermarkType
from lakehouse.metadata.models import LoadWatermark, PipelineRun, SourceObject, TaskRun

logger = logging.getLogger(__name__)


class IncrementalSource(Protocol):
    """A source that can restrict its read to rows above a watermark.

    Separate from `Source` because a real JDBC source pushes the
    predicate down into a WHERE clause, while a file source has to read
    and filter. Both satisfy this interface; only one is efficient.
    """

    def read_since(self, obj: SourceObject, since: object | None) -> pa.Table:
        """Return rows whose incremental column is strictly above `since`."""
        ...


@dataclass(frozen=True)
class IncrementalResult:
    """Outcome of one incremental load."""

    run_id: str
    object_name: str
    rows_read: int
    rows_written: int
    watermark_from: str | None
    watermark_to: str | None
    delta_version: int
    duration_seconds: float
    rows_reprocessed: int = 0
    """Rows re-read because of the grace window. Duplicates in Bronze that
    Silver will collapse — the price of not missing late arrivals."""

    @property
    def advanced(self) -> bool:
        """Whether this run moved the watermark."""
        return self.watermark_to != self.watermark_from


def infer_watermark_type(column: pa.ChunkedArray | pa.Array) -> WatermarkType:
    """Pick a watermark type from the column's Arrow type."""
    if pa.types.is_timestamp(column.type) or pa.types.is_date(column.type):
        return WatermarkType.TIMESTAMP
    if pa.types.is_integer(column.type):
        return WatermarkType.INTEGER
    return WatermarkType.STRING


def encode_watermark(value: object, wtype: WatermarkType) -> str:
    """Render a watermark value as the text stored in the control plane."""
    if wtype is WatermarkType.TIMESTAMP and isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def decode_watermark(text: str, wtype: WatermarkType) -> object:
    """Turn stored text back into a comparable value."""
    if wtype is WatermarkType.INTEGER:
        return int(text)
    if wtype is WatermarkType.TIMESTAMP:
        return datetime.fromisoformat(text)
    return text


def apply_grace(since: object | None, wtype: WatermarkType | None, grace: int) -> object | None:
    """Move a watermark backwards by the configured grace.

    Grace is expressed in the watermark's own units: seconds for a
    timestamp, raw units for an integer key. String watermarks have no
    arithmetic, so grace cannot apply and is ignored.
    """
    if since is None or grace <= 0 or wtype is None:
        return since
    if wtype is WatermarkType.INTEGER and isinstance(since, int):
        return since - grace
    if wtype is WatermarkType.TIMESTAMP and isinstance(since, datetime):
        return since - timedelta(seconds=grace)
    logger.warning("watermark_grace ignored for %s watermarks", wtype)
    return since


def filter_since(table: pa.Table, column: str, since: object | None) -> pa.Table:
    """Keep rows strictly above `since`. Null watermark means take all."""
    if since is None:
        return table
    if column not in table.column_names:
        raise KeyError(f"incremental column '{column}' not present in source")
    return table.filter(pc.greater(table.column(column), pa.scalar(since)))


def current_watermark(session: Session, obj: SourceObject) -> LoadWatermark | None:
    """Stored watermark for an object, or None if it has never loaded."""
    return session.get(LoadWatermark, obj.id)


@dataclass(frozen=True)
class FilteringSource:
    """Adapts a plain `Source` into an `IncrementalSource` by filtering.

    Honest about what it is: a real database source would push the
    predicate down. This reads everything and discards, which is correct
    but not efficient, and is fine for files.
    """

    inner: Source

    def read_since(self, obj: SourceObject, since: object | None) -> pa.Table:
        table = self.inner.read(obj)
        column = obj.incremental_column
        if column is None:
            raise ValueError(f"'{obj.object_name}' has no incremental_column configured")
        return filter_since(table, column, since)


def load_incremental(
    session: Session,
    run: PipelineRun,
    obj: SourceObject,
    source: IncrementalSource,
    lake_root: Path,
) -> IncrementalResult:
    """Append rows newer than the stored watermark, then advance it.

    Raises:
        ValueError: if the object is not configured for an incremental load.
    """
    if obj.load_strategy != LoadStrategy.INCREMENTAL:
        raise ValueError(
            f"'{obj.object_name}' is configured as '{obj.load_strategy}', not 'incremental'"
        )
    column = obj.incremental_column
    if column is None:  # pragma: no cover - the DB constraint prevents this
        raise ValueError(f"'{obj.object_name}' has no incremental_column configured")

    started = time.perf_counter()
    stored = current_watermark(session, obj)
    from_text = stored.watermark_value if stored else None
    wtype = WatermarkType(stored.watermark_type) if stored else None
    since = decode_watermark(from_text, wtype) if (from_text and wtype) else None
    effective_since = apply_grace(since, wtype, obj.watermark_grace)

    task = TaskRun(
        run_id=run.run_id,
        source_object_id=obj.id,
        layer=Layer.BRONZE,
        status=RunStatus.RUNNING,
        watermark_from=from_text,
    )
    session.add(task)
    session.commit()

    try:
        table = source.read_since(obj, effective_since)
        check_drift(session, obj, table)
        if table.num_rows and column not in table.column_names:
            raise KeyError(
                f"incremental column '{column}' not present in source "
                f"'{obj.object_name}' - the config and the source disagree"
            )
        target = lake_root / obj.target_path
        target.parent.mkdir(parents=True, exist_ok=True)

        if table.num_rows:
            stamped = add_provenance(table, run.run_id, obj.object_name)
            write_deltalake(str(target), stamped, mode="append")
        version = DeltaTable(str(target)).version() if target.exists() else -1
    except Exception as exc:
        task.status = RunStatus.FAILED
        task.error_message = f"{type(exc).__name__}: {exc}"
        task.ended_at = datetime.now(UTC)
        session.commit()
        # Watermark deliberately untouched: the next run retries this window.
        raise

    reprocessed = 0
    if table.num_rows and since is not None and effective_since is not since:
        reprocessed = table.num_rows - filter_since(table, column, since).num_rows

    to_text = from_text
    if table.num_rows:
        col = table.column(column)
        new_type = wtype or infer_watermark_type(col)
        candidate = encode_watermark(pc.max(col).as_py(), new_type)
        # With grace enabled the batch can be entirely old rows, whose max
        # sits below the stored watermark. Never move it backwards.
        if since is None or decode_watermark(candidate, new_type) > since:  # type: ignore[operator]
            to_text = candidate
            _advance(session, obj, to_text, new_type, run.run_id)

    elapsed = time.perf_counter() - started
    task.status = RunStatus.SUCCEEDED
    task.rows_read = table.num_rows
    task.rows_written = table.num_rows
    task.watermark_to = to_text
    task.ended_at = datetime.now(UTC)
    task.duration_seconds = int(elapsed)
    session.commit()

    return IncrementalResult(
        run_id=run.run_id,
        object_name=obj.object_name,
        rows_read=table.num_rows,
        rows_written=table.num_rows,
        watermark_from=from_text,
        watermark_to=to_text,
        delta_version=version,
        duration_seconds=elapsed,
        rows_reprocessed=reprocessed,
    )


def _advance(
    session: Session, obj: SourceObject, value: str, wtype: WatermarkType, run_id: str
) -> None:
    """Move the watermark forward. Called only after a successful write."""
    stored = session.get(LoadWatermark, obj.id)
    if stored is None:
        session.add(
            LoadWatermark(
                source_object_id=obj.id,
                watermark_value=value,
                watermark_type=wtype,
                committed_run_id=run_id,
            )
        )
    else:
        stored.watermark_value = value
        stored.watermark_type = wtype
        stored.committed_run_id = run_id
    session.commit()
