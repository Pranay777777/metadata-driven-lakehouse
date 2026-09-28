"""Tests for the operational views the dashboard reads (ADR-019)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event, select
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import Session

from lakehouse.audit import (
    close_abandoned,
    day,
    freshness,
    object_health,
    privacy_posture,
    quarantine_summary,
    rule_performance,
    snapshot,
    track_run,
    unknown_members,
    volume_trend,
)
from lakehouse.audit import main as audit_main
from lakehouse.ingest.bronze import load_full
from lakehouse.metadata.enums import (
    GoldRole,
    Layer,
    LoadStrategy,
    MaskingStrategy,
    RuleType,
    RunStatus,
    Sensitivity,
    Severity,
    SourceKind,
)
from lakehouse.metadata.models import (
    Base,
    ColumnMetadata,
    DataQualityRule,
    PipelineRun,
    SourceObject,
    SourceSystem,
    TaskRun,
)
from lakehouse.transform.silver import build_silver

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


@pytest.fixture
def engine() -> Engine:
    eng = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _fk(conn: object, _: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def session(engine: Engine) -> Iterator[Session]:
    with Session(engine) as s:
        yield s


@pytest.fixture
def system(session: Session) -> SourceSystem:
    s = SourceSystem(name="seed", kind=SourceKind.FILE)
    session.add(s)
    session.commit()
    return s


def make_object(
    session: Session, system: SourceSystem, name: str, **kwargs: object
) -> SourceObject:
    kwargs.setdefault("primary_key_columns", f"{name}_id")
    o = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name=name,
        target_path=f"bronze/seed/{name}",
        load_strategy=LoadStrategy.FULL,
        incremental_column="updated_at",
        **kwargs,
    )
    session.add(o)
    session.commit()
    return o


def run(session: Session, run_id: str) -> str:
    session.add(PipelineRun(run_id=run_id, pipeline_name="nightly"))
    session.commit()
    return run_id


def task(session: Session, obj: SourceObject, run_id: str, **kwargs: object) -> TaskRun:
    t = TaskRun(run_id=run_id, source_object_id=obj.id, **kwargs)
    session.add(t)
    session.commit()
    return t


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


# --- the N+1 is gone -----------------------------------------------------


def count_statements(engine: Engine) -> list[str]:
    seen: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _count(conn: object, cursor: object, statement: str, *args: object) -> None:
        seen.append(statement)

    return seen


def test_object_health_is_one_statement_however_many_objects(
    engine: Engine, session: Session, system: SourceSystem
) -> None:
    rid = run(session, "r1")
    for i in range(6):
        obj = make_object(session, system, f"obj{i}")
        task(session, obj, rid, layer=Layer.BRONZE, rows_written=i)
    session.expire_all()

    seen = count_statements(engine)
    health = object_health(session)

    assert len(health) == 6
    assert len(seen) == 1


def test_rule_performance_is_one_statement_however_many_rules(
    engine: Engine, session: Session, system: SourceSystem
) -> None:
    obj = make_object(session, system, "customers")
    for column in ("a", "b", "c", "d"):
        session.add(
            DataQualityRule(
                source_object_id=obj.id, rule_type=RuleType.NOT_NULL, column_name=column
            )
        )
    session.commit()

    seen = count_statements(engine)
    assert len(rule_performance(session)) == 4
    assert len(seen) == 1


def test_object_health_counts_only_the_recent_tasks(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "customers")
    rid = run(session, "r1")
    base = NOW - timedelta(hours=10)
    for i in range(5):
        task(
            session,
            obj,
            rid,
            layer=Layer.BRONZE,
            rows_written=10,
            status=RunStatus.FAILED if i == 0 else RunStatus.SUCCEEDED,
            started_at=base + timedelta(hours=i),
        )

    health = object_health(session, limit_per_object=3)[0]

    assert health.rows_written == 30
    assert health.failures == 0  # the failure is the oldest, outside the window
    assert health.last_status == RunStatus.SUCCEEDED


def test_the_latest_task_breaks_same_second_ties_by_id(
    session: Session, system: SourceSystem
) -> None:
    obj = make_object(session, system, "customers")
    rid = run(session, "r1")
    task(session, obj, rid, layer=Layer.BRONZE, status=RunStatus.SUCCEEDED, started_at=NOW)
    task(session, obj, rid, layer=Layer.SILVER, status=RunStatus.FAILED, started_at=NOW)

    assert object_health(session)[0].last_status == RunStatus.FAILED


# --- freshness -------------------------------------------------------------


def test_a_recent_load_is_fresh(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders", freshness_sla_minutes=60)
    task(
        session,
        obj,
        run(session, "r1"),
        layer=Layer.SILVER,
        status=RunStatus.SUCCEEDED,
        ended_at=NOW - timedelta(minutes=30),
    )
    row = freshness(session, NOW)[0]
    assert row.age_minutes == pytest.approx(30)
    assert not row.breached


def test_an_old_load_breaches_its_sla(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders", freshness_sla_minutes=60)
    task(
        session,
        obj,
        run(session, "r1"),
        layer=Layer.SILVER,
        status=RunStatus.SUCCEEDED,
        ended_at=NOW - timedelta(hours=2),
    )
    assert freshness(session, NOW)[0].breached


def test_a_never_loaded_object_with_an_sla_is_breached(
    session: Session, system: SourceSystem
) -> None:
    make_object(session, system, "orders", freshness_sla_minutes=60)
    row = freshness(session, NOW)[0]
    assert row.last_loaded_at is None
    assert row.breached


def test_no_sla_means_nothing_to_breach(session: Session, system: SourceSystem) -> None:
    make_object(session, system, "orders")
    assert not freshness(session, NOW)[0].breached


def test_a_failed_build_does_not_count_as_fresh(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders", freshness_sla_minutes=60)
    rid = run(session, "r1")
    task(
        session,
        obj,
        rid,
        layer=Layer.SILVER,
        status=RunStatus.SUCCEEDED,
        ended_at=NOW - timedelta(hours=3),
    )
    task(
        session,
        obj,
        rid,
        layer=Layer.SILVER,
        status=RunStatus.FAILED,
        ended_at=NOW - timedelta(minutes=5),
    )
    assert freshness(session, NOW)[0].age_minutes == pytest.approx(180)


def test_a_quarantined_build_still_counts_as_fresh(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders", freshness_sla_minutes=60)
    task(
        session,
        obj,
        run(session, "r1"),
        layer=Layer.SILVER,
        status=RunStatus.QUARANTINED,
        ended_at=NOW - timedelta(minutes=10),
    )
    assert not freshness(session, NOW)[0].breached


def test_inactive_objects_are_not_reported(session: Session, system: SourceSystem) -> None:
    make_object(session, system, "retired", active=False, freshness_sla_minutes=60)
    assert freshness(session, NOW) == []


# --- volume ---------------------------------------------------------------


def test_volume_is_bucketed_by_day_and_layer(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders")
    rid = run(session, "r1")
    for started, layer, rows, secs in (
        (NOW - timedelta(days=1, hours=1), Layer.BRONZE, 100, 3),
        (NOW - timedelta(days=1), Layer.BRONZE, 50, 2),
        (NOW - timedelta(days=1), Layer.SILVER, 40, 7),
        (NOW - timedelta(hours=1), Layer.BRONZE, 10, 1),
    ):
        task(
            session,
            obj,
            rid,
            layer=layer,
            started_at=started,
            rows_written=rows,
            duration_seconds=secs,
        )

    points = volume_trend(session, days=7, now=NOW)

    by_key = {(p.day, p.layer): p for p in points}
    assert by_key[("2026-09-26", Layer.BRONZE)].rows_written == 150
    assert by_key[("2026-09-26", Layer.BRONZE)].compute_seconds == 5
    assert by_key[("2026-09-26", Layer.SILVER)].tasks == 1
    assert by_key[("2026-09-27", Layer.BRONZE)].rows_written == 10


def test_volume_ignores_tasks_outside_the_window(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders")
    task(
        session,
        obj,
        run(session, "r1"),
        layer=Layer.BRONZE,
        started_at=NOW - timedelta(days=40),
        rows_written=1,
    )
    assert volume_trend(session, days=30, now=NOW) == []


def test_a_task_without_a_duration_costs_nothing(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders")
    task(session, obj, run(session, "r1"), layer=Layer.BRONZE, started_at=NOW, rows_written=1)
    assert volume_trend(session, days=1, now=NOW + timedelta(minutes=1))[0].compute_seconds == 0


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (SQLiteDialect, "date(task_run.started_at)"),
        (PGDialect, "date(task_run.started_at)"),
        (MSDialect, "CAST(task_run.started_at AS DATE)"),
    ],
)
def test_day_compiles_per_dialect(dialect: type[Dialect], expected: str) -> None:
    """CAST AS DATE is silently wrong on SQLite; date() does not exist on Azure SQL."""
    compiled = str(select(day(TaskRun.started_at)).compile(dialect=dialect()))
    assert expected in compiled


# --- quarantine and unknown members ---------------------------------------


def test_only_quarantine_rules_count_as_quarantine(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    obj = make_object(session, system, "customers", primary_key_columns="customer_id")
    session.add_all(
        [
            DataQualityRule(
                source_object_id=obj.id,
                rule_type=RuleType.RANGE,
                column_name="score",
                expression='{"maximum": 100}',
                severity=Severity.QUARANTINE,
            ),
            DataQualityRule(
                source_object_id=obj.id,
                rule_type=RuleType.RANGE,
                column_name="score",
                expression='{"maximum": 5}',
                severity=Severity.WARN,
            ),
        ]
    )
    session.commit()
    table = pa.table({"customer_id": ["c1", "c2"], "score": [10, 500], "updated_at": [1, 2]})
    with track_run(session, "nightly") as r:
        load_full(session, r, obj, Fixed(table), tmp_path)
        build_silver(session, r, obj, tmp_path)

    summary = quarantine_summary(session)

    assert len(summary) == 1
    assert summary[0].rows_quarantined == 1
    assert summary[0].breaches == 1


def test_unknown_members_come_from_the_latest_gold_build(
    session: Session, system: SourceSystem
) -> None:
    fact = make_object(session, system, "orders", gold_role=GoldRole.FACT)
    dim = make_object(session, system, "customers", gold_role=GoldRole.DIMENSION)
    old, new = run(session, "r-old"), run(session, "r-new")
    task(session, fact, old, layer=Layer.GOLD, rows_rejected=40, started_at=NOW - timedelta(days=1))
    task(session, fact, new, layer=Layer.GOLD, rows_rejected=3, started_at=NOW)
    task(session, fact, new, layer=Layer.SILVER, rows_rejected=999, started_at=NOW)
    task(session, dim, new, layer=Layer.GOLD, rows_rejected=7, started_at=NOW)

    result = unknown_members(session)

    assert [(u.object_name, u.rows, u.run_id) for u in result] == [("orders", 3, "r-new")]


# --- privacy --------------------------------------------------------------


def test_privacy_posture_shows_what_reaches_gold(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "customers")
    session.add_all(
        [
            ColumnMetadata(
                source_object_id=obj.id,
                column_name="customer_email",
                sensitivity=Sensitivity.PII,
                masking_strategy=MaskingStrategy.HASH,
            ),
            ColumnMetadata(
                source_object_id=obj.id,
                column_name="customer_document",
                sensitivity=Sensitivity.SENSITIVE_PII,
                masking_strategy=MaskingStrategy.HASH,
            ),
            ColumnMetadata(
                source_object_id=obj.id,
                column_name="passport_scan",
                sensitivity=Sensitivity.SENSITIVE_PII,
                masking_strategy=MaskingStrategy.REDACT,
                allow_in_gold=True,
            ),
            ColumnMetadata(
                source_object_id=obj.id,
                column_name="notes",
                sensitivity=Sensitivity.NONE,
            ),
        ]
    )
    session.commit()

    posture = {c.column_name: c for c in privacy_posture(session)}

    assert set(posture) == {"customer_email", "customer_document", "passport_scan"}
    assert posture["customer_email"].reaches_gold
    assert not posture["customer_document"].reaches_gold
    assert posture["passport_scan"].reaches_gold


# --- snapshot -------------------------------------------------------------


def test_an_empty_control_plane_snapshots_as_empty(session: Session) -> None:
    snap = snapshot(session, NOW)
    assert snap.empty
    assert snap.rows_quarantined == 0
    assert snap.unknown_total == 0


def test_the_snapshot_reads_every_view_against_one_clock(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    obj = make_object(
        session, system, "customers", primary_key_columns="customer_id", freshness_sla_minutes=1
    )
    session.add(
        ColumnMetadata(
            source_object_id=obj.id,
            column_name="customer_email",
            sensitivity=Sensitivity.PII,
            masking_strategy=MaskingStrategy.HASH,
        )
    )
    session.commit()
    table = pa.table(
        {"customer_id": ["c1"], "customer_email": ["a@example.com"], "updated_at": [1]}
    )
    with track_run(session, "nightly") as r:
        load_full(session, r, obj, Fixed(table), tmp_path)
        build_silver(session, r, obj, tmp_path)

    later = datetime.now(UTC) + timedelta(hours=1)
    snap = snapshot(session, later)

    assert snap.taken_at == later
    assert not snap.empty
    assert len(snap.runs) == 1
    assert snap.freshness_breaches == 1  # an hour past a one-minute SLA
    assert snap.masked_columns == 1
    assert {v.layer for v in snap.volume} == {Layer.BRONZE, Layer.SILVER}


# --- repairing abandoned runs ---------------------------------------------


def open_run(session: Session, run_id: str, started: datetime) -> PipelineRun:
    r = PipelineRun(run_id=run_id, pipeline_name="silver", started_at=started)
    session.add(r)
    session.commit()
    return r


def test_close_abandoned_shows_before_it_writes(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders")
    open_run(session, "old", NOW - timedelta(days=3))
    task(session, obj, "old", layer=Layer.SILVER, status=RunStatus.SUCCEEDED, ended_at=NOW)

    assert close_abandoned(session, now=NOW) == [("old", RunStatus.SUCCEEDED)]
    session.expire_all()
    assert session.get(PipelineRun, "old").status == RunStatus.RUNNING  # type: ignore[union-attr]


def test_close_abandoned_derives_the_status_from_the_tasks(
    session: Session, system: SourceSystem
) -> None:
    obj = make_object(session, system, "orders")
    open_run(session, "ok", NOW - timedelta(days=3))
    open_run(session, "bad", NOW - timedelta(days=3))
    finished = NOW - timedelta(days=3) + timedelta(minutes=4)
    task(session, obj, "ok", layer=Layer.SILVER, status=RunStatus.SUCCEEDED, ended_at=finished)
    task(session, obj, "bad", layer=Layer.SILVER, status=RunStatus.FAILED, ended_at=finished)

    closed = dict(close_abandoned(session, now=NOW, apply=True))

    assert closed == {"ok": RunStatus.SUCCEEDED, "bad": RunStatus.FAILED}
    repaired = session.get(PipelineRun, "ok")
    assert repaired is not None
    assert repaired.status == RunStatus.SUCCEEDED
    # The run ends when its last task did, not when it was repaired.
    assert repaired.ended_at is not None
    assert repaired.ended_at.replace(tzinfo=UTC) == finished


def test_a_run_that_never_started_a_task_closes_as_failed(session: Session) -> None:
    open_run(session, "empty", NOW - timedelta(days=3))
    assert close_abandoned(session, now=NOW) == [("empty", RunStatus.FAILED)]


def test_a_run_that_died_mid_task_closes_as_failed(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "orders")
    open_run(session, "died", NOW - timedelta(days=3))
    task(session, obj, "died", layer=Layer.SILVER, status=RunStatus.RUNNING)
    assert close_abandoned(session, now=NOW) == [("died", RunStatus.FAILED)]


def test_a_recent_run_is_left_alone(session: Session) -> None:
    open_run(session, "in-flight", NOW - timedelta(minutes=10))
    assert close_abandoned(session, now=NOW) == []


def test_the_audit_cli_is_a_dry_run_unless_told(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    url = f"sqlite:///{tmp_path / 'control.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    eng = create_engine(url)
    Base.metadata.create_all(eng)
    with Session(eng) as s:
        s.add(PipelineRun(run_id="stuck", pipeline_name="gold", started_at=NOW - timedelta(days=9)))
        s.commit()

    assert audit_main(["--close-abandoned"]) == 0
    assert "would close 1" in capsys.readouterr().out

    assert audit_main(["--close-abandoned", "--apply"]) == 0
    assert "closed 1" in capsys.readouterr().out
    with Session(eng) as s:
        assert s.get(PipelineRun, "stuck").status == RunStatus.FAILED  # type: ignore[union-attr]


def test_the_audit_cli_explains_itself_with_no_action(capsys: pytest.CaptureFixture[str]) -> None:
    assert audit_main([]) == 2
    assert "--close-abandoned" in capsys.readouterr().out
