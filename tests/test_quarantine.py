"""Tests for the quarantine table."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.enums import Layer, LoadStrategy, RuleType, Severity, SourceKind
from lakehouse.metadata.models import Base, DataQualityRule, SourceObject, SourceSystem
from lakehouse.quality.engine import REJECTION_REASON
from lakehouse.quality.quarantine import (
    QUARANTINED_AT,
    REJECTED_FROM,
    RUN_ID,
    TASK_RUN_ID,
    quarantine_path,
    read_quarantine,
    write_quarantine,
)
from lakehouse.transform.silver import build_silver, read_silver


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


def quarantine_rule(session: Session, obj: SourceObject, **kwargs: object) -> None:
    session.add(
        DataQualityRule(
            source_object_id=obj.id,
            rule_type=RuleType.RANGE,
            column_name="score",
            expression='{"maximum": 100}',
            severity=Severity.QUARANTINE,
            **kwargs,
        )
    )
    session.commit()


def batch(scores: list[int]) -> pa.Table:
    return pa.table(
        {
            "customer_id": [f"c{i}" for i in range(len(scores))],
            "score": scores,
            "updated_at": list(range(1, len(scores) + 1)),
        }
    )


# --------------------------------------------------------------------------
# Paths and stamping
# --------------------------------------------------------------------------


def test_the_path_mirrors_the_object(obj: SourceObject) -> None:
    assert quarantine_path(obj) == "quarantine/seed/customers"


def test_the_path_handles_an_unprefixed_object(obj: SourceObject) -> None:
    obj.target_path = "raw/customers"
    assert quarantine_path(obj) == "quarantine/raw/customers"


def test_writing_nothing_creates_no_table(obj: SourceObject, tmp_path: Path) -> None:
    """A clean run must not litter the lake with empty tables."""
    empty = batch([]).slice(0, 0)
    assert write_quarantine(tmp_path, obj, empty, "r1", 1) is None
    assert not (tmp_path / quarantine_path(obj)).exists()


def test_rejected_rows_carry_their_provenance(obj: SourceObject, tmp_path: Path) -> None:
    result = write_quarantine(tmp_path, obj, batch([500]), "run-7", 42, Layer.SILVER)

    assert result is not None
    assert result.rows_written == 1

    stored = read_quarantine(tmp_path, obj)
    assert stored.column(RUN_ID).to_pylist() == ["run-7"]
    assert stored.column(TASK_RUN_ID).to_pylist() == [42]
    assert stored.column(REJECTED_FROM).to_pylist() == [Layer.SILVER]
    assert QUARANTINED_AT in stored.column_names


def test_writes_append_rather_than_replace(obj: SourceObject, tmp_path: Path) -> None:
    """The quarantine log must not lose history to a later run."""
    write_quarantine(tmp_path, obj, batch([500]), "r1", 1)
    write_quarantine(tmp_path, obj, batch([600]), "r2", 2)

    stored = read_quarantine(tmp_path, obj)
    assert stored.num_rows == 2
    assert set(stored.column(RUN_ID).to_pylist()) == {"r1", "r2"}


def test_a_wider_batch_merges_rather_than_overwrites(obj: SourceObject, tmp_path: Path) -> None:
    """Step 26 allows additive drift, so quarantined shapes can differ."""
    write_quarantine(tmp_path, obj, batch([500]), "r1", 1)
    wider = batch([600]).append_column("state", pa.array(["RJ"]))
    write_quarantine(tmp_path, obj, wider, "r2", 2)

    stored = read_quarantine(tmp_path, obj)
    assert stored.num_rows == 2
    assert "state" in stored.column_names


# --------------------------------------------------------------------------
# Reasons
# --------------------------------------------------------------------------


def test_a_diverted_row_records_the_rule_it_breached(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    quarantine_rule(session, obj)
    load_full(
        session, start_pipeline_run(session, "bronze"), obj, Fixed(batch([50, 500])), tmp_path
    )
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    stored = read_quarantine(tmp_path, obj)
    assert stored.num_rows == 1
    assert stored.column(REJECTION_REASON).to_pylist() == ["range:score"]


def test_a_row_breaching_two_rules_records_both(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    quarantine_rule(session, obj)
    session.add(
        DataQualityRule(
            source_object_id=obj.id,
            rule_type=RuleType.NOT_NULL,
            column_name="city",
            severity=Severity.QUARANTINE,
        )
    )
    session.commit()

    table = pa.table({"customer_id": ["c1"], "score": [500], "city": [None], "updated_at": [1]})
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(table), tmp_path)
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    reason = read_quarantine(tmp_path, obj).column(REJECTION_REASON).to_pylist()[0]
    assert "range:score" in reason
    assert "not_null:city" in reason


# --------------------------------------------------------------------------
# Wired into Silver
# --------------------------------------------------------------------------


def test_diverted_rows_are_kept_not_lost(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """The whole point: rows leave Silver but do not leave the lake."""
    quarantine_rule(session, obj)
    load_full(
        session, start_pipeline_run(session, "bronze"), obj, Fixed(batch([50, 500, 700])), tmp_path
    )
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert read_silver(tmp_path, obj).num_rows == 1
    assert read_quarantine(tmp_path, obj).num_rows == 2


def test_a_clean_silver_build_writes_no_quarantine(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    quarantine_rule(session, obj)
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(batch([50])), tmp_path)
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert not (tmp_path / quarantine_path(obj)).exists()


def test_the_quarantine_table_links_back_to_the_task(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    quarantine_rule(session, obj)
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(batch([500])), tmp_path)
    run = start_pipeline_run(session, "silver")
    build_silver(session, run, obj, tmp_path)

    stored = read_quarantine(tmp_path, obj)
    assert stored.column(RUN_ID).to_pylist() == [run.run_id]
    assert stored.column(TASK_RUN_ID).to_pylist()[0] > 0, (
        "a quarantined row must be traceable to the task that rejected it"
    )


def test_repeated_runs_accumulate_in_quarantine(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    quarantine_rule(session, obj)
    for _ in range(3):
        load_full(
            session, start_pipeline_run(session, "bronze"), obj, Fixed(batch([500])), tmp_path
        )
        build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert read_quarantine(tmp_path, obj).num_rows == 3
    assert DeltaTable(str(tmp_path / quarantine_path(obj))).version() == 2
