"""Tests for the Bronze full-reload strategy."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import (
    INGESTED_AT,
    RUN_ID,
    ParquetSource,
    add_provenance,
    finish_pipeline_run,
    latest_run_id,
    load_full,
    read_bronze,
    row_count,
    start_pipeline_run,
)
from lakehouse.metadata.enums import LoadStrategy, RunStatus, SourceKind
from lakehouse.metadata.models import Base, SourceObject, SourceSystem, TaskRun


@pytest.fixture
def session() -> Iterator[Session]:
    eng: Engine = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _fk(conn: object, _: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    with Session(eng) as s:
        yield s


@pytest.fixture
def obj(session: Session) -> SourceObject:
    system = SourceSystem(name="seed", kind=SourceKind.FILE)
    session.add(system)
    session.commit()
    o = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name="customers",
        target_path="bronze/seed/customers",
        load_strategy=LoadStrategy.FULL,
    )
    session.add(o)
    session.commit()
    return o


class FakeSource:
    """In-memory source, so tests do not touch the filesystem to read."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.reads = 0

    def read(self, obj: SourceObject) -> pa.Table:
        self.reads += 1
        return self.table


def _table(n: int, offset: int = 0) -> pa.Table:
    return pa.table(
        {
            "customer_id": [f"cust_{i:05d}" for i in range(offset, offset + n)],
            "customer_city": ["sao paulo"] * n,
        }
    )


def test_full_load_writes_every_row(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "bronze")
    result = load_full(session, run, obj, FakeSource(_table(100)), tmp_path)

    assert result.rows_read == 100
    assert result.rows_written == 100
    assert result.rows_dropped == 0
    assert row_count(tmp_path, obj) == 100


def test_provenance_columns_are_added(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "bronze")
    load_full(session, run, obj, FakeSource(_table(10)), tmp_path)

    table = read_bronze(tmp_path, obj)
    assert INGESTED_AT in table.column_names
    assert RUN_ID in table.column_names
    assert latest_run_id(tmp_path, obj) == run.run_id


def test_reload_replaces_rather_than_appends(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Truncate-and-reload must not double the table on a second run."""
    run1 = start_pipeline_run(session, "bronze")
    load_full(session, run1, obj, FakeSource(_table(50)), tmp_path)
    run2 = start_pipeline_run(session, "bronze")
    result = load_full(session, run2, obj, FakeSource(_table(30, offset=1000)), tmp_path)

    assert row_count(tmp_path, obj) == 30
    assert result.delta_version == 1
    assert latest_run_id(tmp_path, obj) == run2.run_id


def test_previous_version_survives_via_time_travel(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Overwrite creates a version; it does not destroy the old state."""
    run1 = start_pipeline_run(session, "bronze")
    load_full(session, run1, obj, FakeSource(_table(50)), tmp_path)
    run2 = start_pipeline_run(session, "bronze")
    load_full(session, run2, obj, FakeSource(_table(30)), tmp_path)

    assert read_bronze(tmp_path, obj, version=0).num_rows == 50
    assert read_bronze(tmp_path, obj, version=1).num_rows == 30


def test_audit_row_records_the_load(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "bronze")
    load_full(session, run, obj, FakeSource(_table(42)), tmp_path)

    task = session.query(TaskRun).one()
    assert task.status == RunStatus.SUCCEEDED
    assert task.rows_read == 42
    assert task.rows_written == 42
    assert task.ended_at is not None


def test_failure_is_recorded_not_swallowed(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """A failed load must leave evidence and still raise."""

    class Broken:
        def read(self, obj: SourceObject) -> pa.Table:
            raise RuntimeError("source unreachable")

    run = start_pipeline_run(session, "bronze")
    with pytest.raises(RuntimeError, match="source unreachable"):
        load_full(session, run, obj, Broken(), tmp_path)

    task = session.query(TaskRun).one()
    assert task.status == RunStatus.FAILED
    assert task.error_message is not None
    assert "source unreachable" in task.error_message


def test_wrong_strategy_is_refused(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    obj.load_strategy = LoadStrategy.INCREMENTAL
    obj.incremental_column = "updated_at"
    session.commit()
    run = start_pipeline_run(session, "bronze")
    with pytest.raises(ValueError, match="not 'full'"):
        load_full(session, run, obj, FakeSource(_table(5)), tmp_path)


def test_parquet_source_reads_generated_files(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    import pyarrow.parquet as pq

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    pq.write_table(_table(25), src_dir / "customers.parquet")

    run = start_pipeline_run(session, "bronze")
    result = load_full(session, run, obj, ParquetSource(src_dir), tmp_path / "lake")
    assert result.rows_written == 25


def test_missing_source_file_is_a_clear_error(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run = start_pipeline_run(session, "bronze")
    with pytest.raises(FileNotFoundError, match="customers"):
        load_full(session, run, obj, ParquetSource(tmp_path / "nowhere"), tmp_path / "lake")


def test_pipeline_run_lifecycle(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "bronze", triggered_by="pytest")
    assert run.status == RunStatus.RUNNING
    load_full(session, run, obj, FakeSource(_table(5)), tmp_path)
    finish_pipeline_run(session, run, RunStatus.SUCCEEDED)
    assert run.status == RunStatus.SUCCEEDED
    assert run.ended_at is not None


def test_add_provenance_preserves_original_columns() -> None:
    stamped = add_provenance(_table(3), "run-1", "customers")
    assert stamped.num_rows == 3
    assert "customer_id" in stamped.column_names
    assert stamped.column(RUN_ID).to_pylist() == ["run-1"] * 3
