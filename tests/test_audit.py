"""Tests for the audit trail and run tracking."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.audit import (
    derive_status,
    object_health,
    recent_runs,
    rule_performance,
    stale_runs,
    summarise_run,
    track_run,
)
from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.enums import Layer, LoadStrategy, RuleType, RunStatus, Severity, SourceKind
from lakehouse.metadata.models import Base, DataQualityRule, SourceObject, SourceSystem, TaskRun
from lakehouse.transform.silver import build_silver


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
        owner="data-platform@example.com",
    )
    session.add(o)
    session.commit()
    return o


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


TABLE = pa.table({"customer_id": ["c1", "c2"], "score": [10, 500], "updated_at": [1, 2]})


def task(session: Session, obj: SourceObject, run_id: str, **kwargs: object) -> TaskRun:
    t = TaskRun(run_id=run_id, source_object_id=obj.id, layer=Layer.BRONZE, **kwargs)
    session.add(t)
    session.commit()
    return t


# --------------------------------------------------------------------------
# Run tracking
# --------------------------------------------------------------------------


def test_track_run_closes_a_clean_run(session: Session, obj: SourceObject) -> None:
    with track_run(session, "nightly") as run:
        task(session, obj, run.run_id, status=RunStatus.SUCCEEDED)

    assert run.status == RunStatus.SUCCEEDED
    assert run.ended_at is not None


def test_track_run_closes_a_crashed_run_as_failed(session: Session, obj: SourceObject) -> None:
    """A killed process used to leave 'running' behind forever."""
    with pytest.raises(RuntimeError), track_run(session, "nightly") as run:
        task(session, obj, run.run_id, status=RunStatus.SUCCEEDED)
        raise RuntimeError("boom")

    assert run.status == RunStatus.FAILED
    assert run.ended_at is not None


def test_track_run_re_raises_rather_than_swallowing(session: Session) -> None:
    with pytest.raises(ValueError, match="propagate"), track_run(session, "nightly"):
        raise ValueError("must propagate")


def test_a_run_inherits_its_worst_task(session: Session, obj: SourceObject) -> None:
    with track_run(session, "nightly") as run:
        task(session, obj, run.run_id, status=RunStatus.SUCCEEDED)
        task(session, obj, run.run_id, status=RunStatus.FAILED)

    assert run.status == RunStatus.FAILED


def test_quarantine_beats_success_but_loses_to_failure(session: Session, obj: SourceObject) -> None:
    with track_run(session, "a") as first:
        task(session, obj, first.run_id, status=RunStatus.SUCCEEDED)
        task(session, obj, first.run_id, status=RunStatus.QUARANTINED)
    assert first.status == RunStatus.QUARANTINED

    with track_run(session, "b") as second:
        task(session, obj, second.run_id, status=RunStatus.QUARANTINED)
        task(session, obj, second.run_id, status=RunStatus.FAILED)
    assert second.status == RunStatus.FAILED


def test_a_run_with_no_tasks_succeeds(session: Session) -> None:
    with track_run(session, "empty") as run:
        pass
    assert run.status == RunStatus.SUCCEEDED


def test_derive_status_reads_the_tasks(session: Session, obj: SourceObject) -> None:
    run = start_pipeline_run(session, "manual")
    task(session, obj, run.run_id, status=RunStatus.QUARANTINED)
    assert derive_status(session, run.run_id) == RunStatus.QUARANTINED


def test_the_trigger_is_recorded(session: Session) -> None:
    with track_run(session, "nightly", triggered_by="cron") as run:
        pass
    assert run.triggered_by == "cron"


# --------------------------------------------------------------------------
# Summaries
# --------------------------------------------------------------------------


def test_a_run_is_summarised_per_stage(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    with track_run(session, "nightly") as run:
        load_full(session, run, obj, Fixed(TABLE), tmp_path)
        build_silver(session, run, obj, tmp_path)

    summary = summarise_run(session, run.run_id)
    assert summary is not None
    assert {s.layer for s in summary.stages} == {Layer.BRONZE, Layer.SILVER}
    assert summary.duration_seconds is not None
    assert summary.failed_tasks == 0


def test_the_summary_totals_rows_per_stage(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    with track_run(session, "nightly") as run:
        load_full(session, run, obj, Fixed(TABLE), tmp_path)

    summary = summarise_run(session, run.run_id)
    assert summary is not None
    bronze = next(s for s in summary.stages if s.layer == Layer.BRONZE)
    assert bronze.rows_read == 2
    assert bronze.rows_written == 2
    assert bronze.tasks == 1


def test_rejected_rows_surface_in_the_summary(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    session.add(
        DataQualityRule(
            source_object_id=obj.id,
            rule_type=RuleType.RANGE,
            column_name="score",
            expression='{"maximum": 100}',
            severity=Severity.QUARANTINE,
        )
    )
    session.commit()

    with track_run(session, "nightly") as run:
        load_full(session, run, obj, Fixed(TABLE), tmp_path)
        build_silver(session, run, obj, tmp_path)

    summary = summarise_run(session, run.run_id)
    assert summary is not None
    assert summary.rows_rejected == 1
    assert run.status == RunStatus.QUARANTINED

    silver = next(s for s in summary.stages if s.layer == Layer.SILVER)
    assert silver.rejection_rate == pytest.approx(0.5)


def test_a_failed_task_is_counted(session: Session, obj: SourceObject) -> None:
    with track_run(session, "nightly") as run:
        task(session, obj, run.run_id, status=RunStatus.FAILED)

    summary = summarise_run(session, run.run_id)
    assert summary is not None
    assert summary.failed_tasks == 1


def test_an_unknown_run_summarises_to_nothing(session: Session) -> None:
    assert summarise_run(session, "no-such-run") is None


def test_rejection_rate_of_an_empty_stage_is_zero(session: Session, obj: SourceObject) -> None:
    with track_run(session, "nightly") as run:
        task(session, obj, run.run_id, status=RunStatus.SUCCEEDED)

    summary = summarise_run(session, run.run_id)
    assert summary is not None
    assert summary.stages[0].rejection_rate == 0.0


# --------------------------------------------------------------------------
# Fleet views
# --------------------------------------------------------------------------


def test_recent_runs_are_newest_first(session: Session) -> None:
    """Timestamps have one-second resolution, so set them explicitly."""
    base = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
    for offset, name in enumerate(("first", "second", "third")):
        with track_run(session, name) as run:
            run.started_at = base + timedelta(minutes=offset)
    session.commit()

    assert [r.pipeline_name for r in recent_runs(session, limit=2)] == ["third", "second"]


def test_stale_runs_finds_the_abandoned_one(session: Session) -> None:
    start_pipeline_run(session, "abandoned")
    with track_run(session, "clean"):
        pass

    stale = stale_runs(session)
    assert [r.pipeline_name for r in stale] == ["abandoned"]


def test_object_health_reports_the_latest_load(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    with track_run(session, "nightly") as run:
        load_full(session, run, obj, Fixed(TABLE), tmp_path)

    health = object_health(session)
    assert len(health) == 1
    assert health[0].object_name == "customers"
    assert health[0].owner == "data-platform@example.com"
    assert health[0].last_status == RunStatus.SUCCEEDED
    assert health[0].rows_written == 2


def test_object_health_covers_a_never_loaded_object(session: Session, obj: SourceObject) -> None:
    """An object that has never run is the one worth noticing."""
    health = object_health(session)
    assert health[0].last_run_id is None
    assert health[0].last_loaded_at is None


def test_rule_performance_reports_a_pass_rate(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    session.add(
        DataQualityRule(
            source_object_id=obj.id,
            rule_type=RuleType.RANGE,
            column_name="score",
            expression='{"maximum": 100}',
            severity=Severity.WARN,
        )
    )
    session.commit()

    for _ in range(2):
        with track_run(session, "nightly") as run:
            load_full(session, run, obj, Fixed(TABLE), tmp_path)
            build_silver(session, run, obj, tmp_path)

    performance = rule_performance(session)
    assert performance[0].evaluations == 2
    assert performance[0].failures == 2
    assert performance[0].pass_rate == 0.0


def test_an_unevaluated_rule_has_a_full_pass_rate(session: Session, obj: SourceObject) -> None:
    session.add(
        DataQualityRule(source_object_id=obj.id, rule_type=RuleType.NOT_NULL, column_name="score")
    )
    session.commit()
    assert rule_performance(session)[0].pass_rate == 1.0
