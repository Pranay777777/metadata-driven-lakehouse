"""Tests for the incremental watermark load.

The important ones are the failure and no-op cases. A happy-path
incremental load is easy; the value is in proving that a crash does not
lose rows and a re-run does not duplicate them.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import read_bronze, start_pipeline_run
from lakehouse.ingest.incremental import (
    FilteringSource,
    current_watermark,
    decode_watermark,
    encode_watermark,
    filter_since,
    infer_watermark_type,
    load_incremental,
)
from lakehouse.metadata.enums import LoadStrategy, RunStatus, SourceKind, WatermarkType
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
        object_name="orders",
        target_path="bronze/seed/orders",
        load_strategy=LoadStrategy.INCREMENTAL,
        incremental_column="updated_at",
    )
    session.add(o)
    session.commit()
    return o


class Rows:
    """Mutable in-memory source, so a test can add rows between runs."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def _rows(ids: list[int], stamps: list[int]) -> pa.Table:
    return pa.table({"order_id": ids, "updated_at": stamps})


def _source(table: pa.Table) -> FilteringSource:
    return FilteringSource(Rows(table))


def test_first_load_takes_everything(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "bronze")
    result = load_incremental(session, run, obj, _source(_rows([1, 2, 3], [10, 20, 30])), tmp_path)

    assert result.rows_written == 3
    assert result.watermark_from is None
    assert result.watermark_to == "30"
    assert result.advanced


def test_second_load_takes_only_new_rows(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run1 = start_pipeline_run(session, "bronze")
    load_incremental(session, run1, obj, _source(_rows([1, 2], [10, 20])), tmp_path)

    run2 = start_pipeline_run(session, "bronze")
    result = load_incremental(
        session, run2, obj, _source(_rows([1, 2, 3, 4], [10, 20, 30, 40])), tmp_path
    )

    assert result.rows_written == 2
    assert result.watermark_from == "20"
    assert result.watermark_to == "40"
    assert read_bronze(tmp_path, obj).num_rows == 4


def test_rerun_with_no_new_data_is_a_noop(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Re-running an up-to-date load must not duplicate anything."""
    table = _rows([1, 2, 3], [10, 20, 30])
    run1 = start_pipeline_run(session, "bronze")
    load_incremental(session, run1, obj, _source(table), tmp_path)

    run2 = start_pipeline_run(session, "bronze")
    result = load_incremental(session, run2, obj, _source(table), tmp_path)

    assert result.rows_written == 0
    assert not result.advanced
    assert read_bronze(tmp_path, obj).num_rows == 3


def test_watermark_does_not_advance_when_the_write_fails(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """The property the whole design rests on.

    If the watermark moved before the write, this crash would skip rows
    10-30 forever and nobody would notice until a reconciliation.
    """

    class Exploding:
        def read_since(self, obj: SourceObject, since: object | None) -> pa.Table:
            raise RuntimeError("connection reset")

    run = start_pipeline_run(session, "bronze")
    with pytest.raises(RuntimeError, match="connection reset"):
        load_incremental(session, run, obj, Exploding(), tmp_path)

    assert current_watermark(session, obj) is None
    task = session.query(TaskRun).one()
    assert task.status == RunStatus.FAILED


def test_retry_after_failure_loses_nothing(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """A failed run then a good run must land every row exactly once."""

    class FailOnce:
        def __init__(self, table: pa.Table) -> None:
            self.table = table
            self.calls = 0

        def read_since(self, obj: SourceObject, since: object | None) -> pa.Table:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient")
            return filter_since(self.table, "updated_at", since)

    src = FailOnce(_rows([1, 2, 3], [10, 20, 30]))
    run1 = start_pipeline_run(session, "bronze")
    with pytest.raises(RuntimeError):
        load_incremental(session, run1, obj, src, tmp_path)

    run2 = start_pipeline_run(session, "bronze")
    result = load_incremental(session, run2, obj, src, tmp_path)
    assert result.rows_written == 3
    assert read_bronze(tmp_path, obj).num_rows == 3


def test_late_arriving_rows_are_missed(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    """Documents a known gap rather than hiding it.

    A strict `>` comparison cannot see a row that appears later with an
    older timestamp. Step 22 adds a grace window; until then this test
    pins the current behaviour so the fix is provable.
    """
    run1 = start_pipeline_run(session, "bronze")
    load_incremental(session, run1, obj, _source(_rows([1, 2], [10, 40])), tmp_path)

    # Row 3 arrives now, but its timestamp (25) is below the watermark (40).
    run2 = start_pipeline_run(session, "bronze")
    result = load_incremental(session, run2, obj, _source(_rows([1, 2, 3], [10, 40, 25])), tmp_path)

    assert result.rows_written == 0
    assert read_bronze(tmp_path, obj).num_rows == 2


def test_audit_records_the_watermark_window(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run1 = start_pipeline_run(session, "bronze")
    load_incremental(session, run1, obj, _source(_rows([1], [10])), tmp_path)
    run2 = start_pipeline_run(session, "bronze")
    load_incremental(session, run2, obj, _source(_rows([1, 2], [10, 99])), tmp_path)

    latest = session.query(TaskRun).order_by(TaskRun.id.desc()).first()
    assert latest is not None
    assert latest.watermark_from == "10"
    assert latest.watermark_to == "99"


def test_wrong_strategy_is_refused(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    obj.load_strategy = LoadStrategy.FULL
    obj.incremental_column = None
    session.commit()
    run = start_pipeline_run(session, "bronze")
    with pytest.raises(ValueError, match="not 'incremental'"):
        load_incremental(session, run, obj, _source(_rows([1], [1])), tmp_path)


def test_missing_incremental_column_fails_loudly(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Config says updated_at; the source has no such column.

    This must fail on the very first load with a message naming both the
    column and the object, not several steps later inside Arrow.
    """
    run = start_pipeline_run(session, "bronze")
    bad = FilteringSource(Rows(pa.table({"order_id": [1]})))
    with pytest.raises(KeyError, match="config and the source disagree"):
        load_incremental(session, run, obj, bad, tmp_path)

    assert current_watermark(session, obj) is None
    assert session.query(TaskRun).one().status == RunStatus.FAILED


def test_watermark_encoding_round_trips() -> None:
    assert decode_watermark("42", WatermarkType.INTEGER) == 42
    assert decode_watermark("abc", WatermarkType.STRING) == "abc"
    text = encode_watermark(7, WatermarkType.INTEGER)
    assert decode_watermark(text, WatermarkType.INTEGER) == 7


def test_watermark_type_is_inferred_from_the_column() -> None:
    assert infer_watermark_type(pa.array([1, 2], type=pa.int64())) is WatermarkType.INTEGER
    assert infer_watermark_type(pa.array(["a"], type=pa.string())) is WatermarkType.STRING
    stamps = pa.array([0], type=pa.timestamp("us", tz="UTC"))
    assert infer_watermark_type(stamps) is WatermarkType.TIMESTAMP


def test_filter_since_with_no_watermark_returns_everything() -> None:
    table = _rows([1, 2], [10, 20])
    assert filter_since(table, "updated_at", None).num_rows == 2
