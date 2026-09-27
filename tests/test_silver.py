"""Tests for the Silver layer."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.ingest.runner import run_pipeline
from lakehouse.metadata.enums import Layer, LoadStrategy, RunStatus, SourceKind
from lakehouse.metadata.models import Base, SourceObject, SourceSystem, TaskRun
from lakehouse.transform.silver import build_silver, read_silver, silver_path


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
        primary_key_columns="customer_id",
        incremental_column="updated_at",
    )
    session.add(o)
    session.commit()
    return o


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def _bronze(session: Session, obj: SourceObject, table: pa.Table, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "bronze")
    load_full(session, run, obj, Fixed(table), tmp_path)


def test_silver_path_mirrors_bronze(obj: SourceObject) -> None:
    assert silver_path(obj) == "silver/seed/customers"


def test_silver_path_when_bronze_prefix_absent(obj: SourceObject) -> None:
    obj.target_path = "raw/customers"
    assert silver_path(obj) == "silver/raw/customers"


def test_duplicates_are_removed(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    """Redelivered rows collapse to the newest per key."""
    table = pa.table(
        {
            "customer_id": ["c1", "c1", "c2"],
            "city": ["rio", "manaus", "recife"],
            "updated_at": [10, 20, 10],
        }
    )
    _bronze(session, obj, table, tmp_path)

    run = start_pipeline_run(session, "silver")
    result = build_silver(session, run, obj, tmp_path)

    assert result.rows_read == 3
    assert result.rows_written == 2
    assert result.duplicates_removed == 1
    silver = read_silver(tmp_path, obj).sort_by("customer_id")
    assert silver.column("city").to_pylist() == ["manaus", "recife"]


def test_column_names_are_normalised(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    obj.primary_key_columns = "CustomerID"
    session.commit()
    table = pa.table({"CustomerID": ["c1"], "Customer City": ["rio"], "updated_at": [1]})
    _bronze(session, obj, table, tmp_path)

    run = start_pipeline_run(session, "silver")
    build_silver(session, run, obj, tmp_path)

    names = read_silver(tmp_path, obj).column_names
    assert "customer_id" in names
    assert "customer_city" in names


def test_whitespace_is_trimmed(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    table = pa.table({"customer_id": ["c1"], "city": ["  rio  "], "updated_at": [1]})
    _bronze(session, obj, table, tmp_path)

    run = start_pipeline_run(session, "silver")
    build_silver(session, run, obj, tmp_path)
    assert read_silver(tmp_path, obj).column("city").to_pylist() == ["rio"]


def test_epoch_columns_become_utc_timestamps(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    table = pa.table({"customer_id": ["c1"], "updated_at": [86_400]})
    _bronze(session, obj, table, tmp_path)

    run = start_pipeline_run(session, "silver")
    build_silver(session, run, obj, tmp_path)

    column = read_silver(tmp_path, obj).column("updated_at")
    assert pa.types.is_timestamp(column.type)
    assert column.type.tz == "UTC"


def test_without_a_primary_key_only_exact_repeats_collapse(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    obj.primary_key_columns = None
    session.commit()
    table = pa.table({"customer_id": ["c1", "c1"], "city": ["rio", "manaus"], "updated_at": [1, 2]})
    _bronze(session, obj, table, tmp_path)

    run = start_pipeline_run(session, "silver")
    result = build_silver(session, run, obj, tmp_path)
    assert result.rows_written == 2


def test_rebuild_is_idempotent(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    table = pa.table({"customer_id": ["c1", "c2"], "city": ["a", "b"], "updated_at": [1, 1]})
    _bronze(session, obj, table, tmp_path)

    run1 = start_pipeline_run(session, "silver")
    build_silver(session, run1, obj, tmp_path)
    run2 = start_pipeline_run(session, "silver")
    build_silver(session, run2, obj, tmp_path)

    assert read_silver(tmp_path, obj).num_rows == 2


def test_audit_records_the_silver_build(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    table = pa.table({"customer_id": ["c1", "c1"], "city": ["a", "b"], "updated_at": [1, 2]})
    _bronze(session, obj, table, tmp_path)

    run = start_pipeline_run(session, "silver")
    build_silver(session, run, obj, tmp_path)

    task = session.query(TaskRun).filter(TaskRun.layer == Layer.SILVER).one()
    assert task.status == RunStatus.SUCCEEDED
    assert task.rows_read == 2
    assert task.rows_written == 1
    assert task.rows_rejected == 1


def test_missing_key_column_fails_and_is_recorded(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    obj.primary_key_columns = "does_not_exist"
    session.commit()
    _bronze(session, obj, pa.table({"customer_id": ["c1"], "updated_at": [1]}), tmp_path)

    run = start_pipeline_run(session, "silver")
    with pytest.raises(KeyError, match="does_not_exist"):
        build_silver(session, run, obj, tmp_path)

    task = session.query(TaskRun).filter(TaskRun.layer == Layer.SILVER).one()
    assert task.status == RunStatus.FAILED


def test_end_to_end_bronze_then_silver(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    """The duplicates the grace window creates are the ones Silver removes."""
    table = pa.table(
        {"customer_id": ["c1", "c2", "c1"], "city": ["a", "b", "c"], "updated_at": [1, 1, 9]}
    )
    run_pipeline(session, Fixed(table), tmp_path)

    run = start_pipeline_run(session, "silver")
    build_silver(session, run, obj, tmp_path)

    silver = read_silver(tmp_path, obj).sort_by("customer_id")
    assert silver.num_rows == 2
    assert silver.column("city").to_pylist() == ["c", "b"]
