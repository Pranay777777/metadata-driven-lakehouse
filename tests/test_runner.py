"""Tests for the metadata-driven pipeline runner.

The dispatch tests prove the platform is configuration-driven. The crash
and resume tests prove it is safe to restart, which is the property that
matters at 3am.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import read_bronze
from lakehouse.ingest.runner import active_objects, run_pipeline
from lakehouse.metadata.enums import LoadStrategy, RunStatus, SourceKind
from lakehouse.metadata.models import Base, PipelineRun, SourceObject, SourceSystem, TaskRun


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
def system(session: Session) -> SourceSystem:
    s = SourceSystem(name="seed", kind=SourceKind.FILE)
    session.add(s)
    session.commit()
    return s


def _add(session: Session, system: SourceSystem, name: str, **kw: object) -> SourceObject:
    defaults: dict[str, object] = {
        "source_system_id": system.id,
        "schema_name": "public",
        "object_name": name,
        "target_path": f"bronze/{name}",
        "load_strategy": LoadStrategy.FULL,
    }
    defaults.update(kw)
    obj = SourceObject(**defaults)
    session.add(obj)
    session.commit()
    return obj


class Catalog:
    """Source backed by a dict of tables, with optional failures."""

    def __init__(self, tables: dict[str, pa.Table], broken: set[str] | None = None) -> None:
        self.tables = tables
        self.broken = broken or set()
        self.reads: list[str] = []

    def read(self, obj: SourceObject) -> pa.Table:
        self.reads.append(obj.object_name)
        if obj.object_name in self.broken:
            raise RuntimeError(f"{obj.object_name} unreachable")
        return self.tables[obj.object_name]


def _rows(n: int, seq_start: int = 1) -> pa.Table:
    return pa.table(
        {
            "id": [f"k{i}" for i in range(n)],
            "seq": list(range(seq_start, seq_start + n)),
            "op": ["I"] * n,
        }
    )


def test_runs_every_active_object(session: Session, system: SourceSystem, tmp_path: Path) -> None:
    _add(session, system, "customers")
    _add(session, system, "products")
    source = Catalog({"customers": _rows(3), "products": _rows(5)})

    summary = run_pipeline(session, source, tmp_path)

    assert len(summary.succeeded) == 2
    assert summary.rows_written == 8
    assert summary.status is RunStatus.SUCCEEDED


def test_dispatches_each_strategy_from_metadata(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    """One runner, three strategies, chosen by configuration alone."""
    _add(session, system, "dim", load_strategy=LoadStrategy.FULL)
    _add(
        session,
        system,
        "events",
        load_strategy=LoadStrategy.INCREMENTAL,
        incremental_column="seq",
    )
    _add(
        session,
        system,
        "accounts",
        load_strategy=LoadStrategy.CDC,
        primary_key_columns="id",
        incremental_column="seq",
        cdc_operation_column="op",
    )
    source = Catalog({"dim": _rows(2), "events": _rows(4), "accounts": _rows(3)})

    summary = run_pipeline(session, source, tmp_path)

    assert len(summary.succeeded) == 3
    by_name = {o.object_name: o.strategy for o in summary.outcomes}
    assert by_name == {"dim": "full", "events": "incremental", "accounts": "cdc"}


def test_inactive_objects_are_not_loaded(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    _add(session, system, "live")
    _add(session, system, "retired", active=False)
    source = Catalog({"live": _rows(2), "retired": _rows(2)})

    run_pipeline(session, source, tmp_path)
    assert source.reads == ["live"]


def test_load_order_is_respected(session: Session, system: SourceSystem, tmp_path: Path) -> None:
    _add(session, system, "third", load_order=30)
    _add(session, system, "first", load_order=10)
    _add(session, system, "second", load_order=20)
    source = Catalog({n: _rows(1) for n in ("first", "second", "third")})

    run_pipeline(session, source, tmp_path)
    assert source.reads == ["first", "second", "third"]


def test_one_failure_does_not_stop_the_others(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    """A single unreachable source must not stale the whole lake."""
    for name in ("a", "broken", "c"):
        _add(session, system, name, load_order=10)
    source = Catalog({"a": _rows(2), "broken": _rows(2), "c": _rows(2)}, broken={"broken"})

    summary = run_pipeline(session, source, tmp_path)

    assert len(summary.succeeded) == 2
    assert len(summary.failed) == 1
    assert summary.failed[0].object_name == "broken"
    assert "unreachable" in (summary.failed[0].error or "")
    assert summary.status is RunStatus.FAILED
    assert read_bronze(tmp_path, active_objects(session)[0]).num_rows == 2


def test_stop_on_error_aborts_the_run(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    _add(session, system, "a", load_order=10)
    _add(session, system, "broken", load_order=20)
    _add(session, system, "c", load_order=30)
    source = Catalog({"a": _rows(1), "broken": _rows(1), "c": _rows(1)}, broken={"broken"})

    summary = run_pipeline(session, source, tmp_path, stop_on_error=True)

    assert source.reads == ["a", "broken"]
    assert len(summary.outcomes) == 2


def test_resume_skips_what_already_succeeded(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    """The crash-and-restart case."""
    _add(session, system, "a", load_order=10)
    _add(session, system, "broken", load_order=20)
    _add(session, system, "c", load_order=30)
    tables = {"a": _rows(2), "broken": _rows(2), "c": _rows(2)}

    first = run_pipeline(session, Catalog(tables, broken={"broken"}), tmp_path)
    assert len(first.failed) == 1

    healed = Catalog(tables)
    second = run_pipeline(session, healed, tmp_path, resume_run_id=first.run_id)

    assert healed.reads == ["broken"]
    assert len(second.skipped) == 2
    assert len(second.succeeded) == 1
    assert second.status is RunStatus.SUCCEEDED


def test_resume_does_not_duplicate_rows(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    obj = _add(
        session,
        system,
        "events",
        load_strategy=LoadStrategy.INCREMENTAL,
        incremental_column="seq",
    )
    _add(session, system, "broken", load_order=90)
    tables = {"events": _rows(5), "broken": _rows(1)}

    first = run_pipeline(session, Catalog(tables, broken={"broken"}), tmp_path)
    run_pipeline(session, Catalog(tables), tmp_path, resume_run_id=first.run_id)

    assert read_bronze(tmp_path, obj).num_rows == 5


def test_resuming_an_unknown_run_is_refused(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="unknown run"):
        run_pipeline(session, Catalog({}), tmp_path, resume_run_id="never-existed")


def test_running_twice_is_idempotent(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    """Two complete runs over unchanged data leave the same row counts."""
    full = _add(session, system, "dim", load_strategy=LoadStrategy.FULL)
    inc = _add(
        session,
        system,
        "events",
        load_strategy=LoadStrategy.INCREMENTAL,
        incremental_column="seq",
    )
    cdc = _add(
        session,
        system,
        "accounts",
        load_strategy=LoadStrategy.CDC,
        primary_key_columns="id",
        incremental_column="seq",
        cdc_operation_column="op",
    )
    tables = {"dim": _rows(4), "events": _rows(6), "accounts": _rows(5)}

    run_pipeline(session, Catalog(tables), tmp_path)
    before = {o.object_name: read_bronze(tmp_path, o).num_rows for o in (full, inc, cdc)}

    run_pipeline(session, Catalog(tables), tmp_path)
    after = {o.object_name: read_bronze(tmp_path, o).num_rows for o in (full, inc, cdc)}

    assert before == after == {"dim": 4, "events": 6, "accounts": 5}


def test_object_names_restrict_the_run(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    _add(session, system, "a")
    _add(session, system, "b")
    source = Catalog({"a": _rows(1), "b": _rows(1)})

    run_pipeline(session, source, tmp_path, object_names=["b"])
    assert source.reads == ["b"]


def test_audit_rows_are_written_for_every_object(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    _add(session, system, "a")
    _add(session, system, "broken")
    source = Catalog({"a": _rows(1), "broken": _rows(1)}, broken={"broken"})

    summary = run_pipeline(session, source, tmp_path)

    tasks = session.query(TaskRun).all()
    assert len(tasks) == 2
    assert {t.status for t in tasks} == {RunStatus.SUCCEEDED, RunStatus.FAILED}
    run = session.get(PipelineRun, summary.run_id)
    assert run is not None
    assert run.status == RunStatus.FAILED
    assert run.ended_at is not None


def test_report_is_readable(session: Session, system: SourceSystem, tmp_path: Path) -> None:
    _add(session, system, "a")
    _add(session, system, "broken")
    source = Catalog({"a": _rows(3), "broken": _rows(1)}, broken={"broken"})

    text = run_pipeline(session, source, tmp_path).report()
    assert "a" in text
    assert "FAIL" in text
    assert "1 failed" in text
