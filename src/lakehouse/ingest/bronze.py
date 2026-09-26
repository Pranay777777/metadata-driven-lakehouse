"""Bronze ingestion.

Bronze is a faithful copy of the source plus provenance columns. No
business logic, no cleaning, no joins — those belong in Silver. The only
thing Bronze adds is the ability to answer "what did the source say, and
when did we learn it".

The loader is driven entirely by a `SourceObject` row. Nothing here knows
what `orders` is; it knows a row that says where to read from, which
strategy to apply, and where to write.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake
from sqlalchemy.orm import Session

from lakehouse.metadata.enums import Layer, LoadStrategy, RunStatus
from lakehouse.metadata.models import PipelineRun, SourceObject, TaskRun

INGESTED_AT = "_ingested_at"
RUN_ID = "_run_id"
SOURCE_FILE = "_source"


class Source(Protocol):
    """Anything that can hand back a table for a named object.

    A Protocol rather than a base class so a test can pass a dictionary of
    in-memory tables without importing anything from this module.
    """

    def read(self, obj: SourceObject) -> pa.Table:
        """Return the current contents of the source object."""
        ...


@dataclass(frozen=True)
class ParquetSource:
    """Reads the files produced by `python -m lakehouse.seed`."""

    root: Path

    def read(self, obj: SourceObject) -> pa.Table:
        path = self.root / f"{obj.object_name}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"no source file for '{obj.object_name}' at {path}")
        return pq.read_table(path)


@dataclass(frozen=True)
class LoadResult:
    """What a load did. Returned as well as recorded, so callers can assert."""

    run_id: str
    object_name: str
    rows_read: int
    rows_written: int
    delta_version: int
    duration_seconds: float

    @property
    def rows_dropped(self) -> int:
        return self.rows_read - self.rows_written


def new_run_id() -> str:
    """Short, sortable-enough identifier for one pipeline invocation."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def start_pipeline_run(session: Session, name: str, triggered_by: str = "manual") -> PipelineRun:
    """Open a pipeline run and persist it immediately.

    Persisted up front rather than at the end, so a process that dies
    mid-run leaves a `running` row behind instead of no evidence at all.
    """
    run = PipelineRun(run_id=new_run_id(), pipeline_name=name, triggered_by=triggered_by)
    session.add(run)
    session.commit()
    return run


def finish_pipeline_run(session: Session, run: PipelineRun, status: RunStatus) -> None:
    """Close a pipeline run."""
    run.status = status
    run.ended_at = datetime.now(UTC)
    session.commit()


def add_provenance(table: pa.Table, run_id: str, source: str) -> pa.Table:
    """Stamp every row with where it came from and which run brought it.

    Without these three columns a Bronze table cannot answer "which run
    wrote this row", which makes both debugging and replay guesswork.
    """
    n = table.num_rows
    now = datetime.now(UTC)
    return (
        table.append_column(INGESTED_AT, pa.array([now] * n, type=pa.timestamp("us", tz="UTC")))
        .append_column(RUN_ID, pa.array([run_id] * n, type=pa.string()))
        .append_column(SOURCE_FILE, pa.array([source] * n, type=pa.string()))
    )


def load_full(
    session: Session,
    run: PipelineRun,
    obj: SourceObject,
    source: Source,
    lake_root: Path,
) -> LoadResult:
    """Truncate-and-reload an object into Bronze.

    Correct for small dimensions and for any source that cannot tell you
    what changed. The whole target is replaced atomically: Delta's
    overwrite creates a new version rather than deleting files, so the
    previous state remains readable by time travel.

    Raises:
        ValueError: if the object is not configured for a full load.
    """
    if obj.load_strategy != LoadStrategy.FULL:
        raise ValueError(f"'{obj.object_name}' is configured as '{obj.load_strategy}', not 'full'")

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
        table = source.read(obj)
        stamped = add_provenance(table, run.run_id, obj.object_name)
        target = lake_root / obj.target_path
        target.parent.mkdir(parents=True, exist_ok=True)

        write_deltalake(str(target), stamped, mode="overwrite", schema_mode="overwrite")
        version = DeltaTable(str(target)).version()
    except Exception as exc:
        task.status = RunStatus.FAILED
        task.error_message = f"{type(exc).__name__}: {exc}"
        task.ended_at = datetime.now(UTC)
        session.commit()
        raise

    elapsed = time.perf_counter() - started
    task.status = RunStatus.SUCCEEDED
    task.rows_read = table.num_rows
    task.rows_written = stamped.num_rows
    task.ended_at = datetime.now(UTC)
    task.duration_seconds = int(elapsed)
    session.commit()

    return LoadResult(
        run_id=run.run_id,
        object_name=obj.object_name,
        rows_read=table.num_rows,
        rows_written=stamped.num_rows,
        delta_version=version,
        duration_seconds=elapsed,
    )


def read_bronze(lake_root: Path, obj: SourceObject, version: int | None = None) -> pa.Table:
    """Read a Bronze table, optionally at an earlier version."""
    target = str(lake_root / obj.target_path)
    dt = DeltaTable(target) if version is None else DeltaTable(target, version=version)
    return dt.to_pyarrow_table()


def row_count(lake_root: Path, obj: SourceObject) -> int:
    """Row count of the current Bronze version."""
    table = read_bronze(lake_root, obj)
    return int(table.num_rows)


def latest_run_id(lake_root: Path, obj: SourceObject) -> str | None:
    """Run that last wrote this table, read back from the data itself."""
    table = read_bronze(lake_root, obj)
    if table.num_rows == 0:
        return None
    value = pc.max(table.column(RUN_ID)).as_py()
    return str(value) if value is not None else None
