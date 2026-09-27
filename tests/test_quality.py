"""Tests for the data quality engine."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.enums import Layer, LoadStrategy, RuleType, RunStatus, Severity, SourceKind
from lakehouse.metadata.models import (
    Base,
    DataQualityResult,
    DataQualityRule,
    PipelineRun,
    SourceObject,
    SourceSystem,
    TaskRun,
)
from lakehouse.quality import DataQualityError, evaluate
from lakehouse.quality.engine import RuleEvaluationError, active_rules
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


@pytest.fixture
def task(session: Session, obj: SourceObject) -> TaskRun:
    run = PipelineRun(run_id="r1", pipeline_name="test")
    session.add(run)
    session.commit()
    t = TaskRun(run_id=run.run_id, source_object_id=obj.id, layer=Layer.SILVER)
    session.add(t)
    session.commit()
    return t


def add_rule(
    session: Session,
    obj: SourceObject,
    rule_type: str,
    *,
    column: str | None = None,
    expression: str | None = None,
    severity: str = Severity.WARN,
    active: bool = True,
) -> DataQualityRule:
    rule = DataQualityRule(
        source_object_id=obj.id,
        rule_type=rule_type,
        column_name=column,
        expression=expression,
        severity=severity,
        active=active,
    )
    session.add(rule)
    session.commit()
    return rule


TABLE = pa.table(
    {
        "customer_id": ["c1", "c2", "c3", None],
        "score": [10, 200, 50, 50],
        "city": ["rio", "recife", "!!!", "manaus"],
    }
)


# --------------------------------------------------------------------------
# Individual rules
# --------------------------------------------------------------------------


def test_not_null_counts_the_nulls(session: Session, obj: SourceObject, task: TaskRun) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="customer_id")
    outcome = evaluate(session, task, obj, TABLE)
    assert outcome.results[0].failed_rows == 1


def test_a_clean_batch_passes_every_rule(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="customer_id")
    clean = pa.table({"customer_id": ["c1"], "score": [1], "city": ["rio"]})
    outcome = evaluate(session, task, obj, clean)
    assert outcome.failures == []
    assert outcome.status == RunStatus.SUCCEEDED


def test_range_flags_both_bounds(session: Session, obj: SourceObject, task: TaskRun) -> None:
    add_rule(
        session, obj, RuleType.RANGE, column="score", expression='{"minimum": 20, "maximum": 100}'
    )
    outcome = evaluate(session, task, obj, TABLE)
    assert outcome.results[0].failed_rows == 2, "10 is below the floor, 200 above the ceiling"


def test_range_with_only_a_minimum(session: Session, obj: SourceObject, task: TaskRun) -> None:
    add_rule(session, obj, RuleType.RANGE, column="score", expression='{"minimum": 20}')
    assert evaluate(session, task, obj, TABLE).results[0].failed_rows == 1


def test_allowed_values_flags_the_outsider(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(
        session,
        obj,
        RuleType.ALLOWED_VALUES,
        column="city",
        expression='{"values": ["rio", "recife", "manaus"]}',
    )
    assert evaluate(session, task, obj, TABLE).results[0].failed_rows == 1


def test_regex_flags_the_non_matching_row(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.REGEX, column="city", expression='{"pattern": "^[a-z]+$"}')
    assert evaluate(session, task, obj, TABLE).results[0].failed_rows == 1


def test_unique_flags_every_copy_of_a_duplicate(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.UNIQUE, column="score")
    assert evaluate(session, task, obj, TABLE).results[0].failed_rows == 2


def test_nulls_pass_rules_other_than_not_null(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    """A null is an absent value, not a wrong one."""
    add_rule(session, obj, RuleType.RANGE, column="score", expression='{"minimum": 0}')
    table = pa.table({"customer_id": ["c1"], "score": [None], "city": ["rio"]})
    assert evaluate(session, task, obj, table).failures == []


def test_row_count_bounds(session: Session, obj: SourceObject, task: TaskRun) -> None:
    add_rule(session, obj, RuleType.ROW_COUNT, expression='{"minimum": 10}')
    outcome = evaluate(session, task, obj, TABLE)
    assert not outcome.results[0].passed
    assert "minimum 10" in outcome.results[0].detail


def test_freshness_passes_for_a_recent_batch(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(
        session,
        obj,
        RuleType.FRESHNESS,
        column="updated_at",
        expression='{"max_age_minutes": 60}',
    )
    now = datetime.now(UTC)
    table = pa.table({"updated_at": [int(now.timestamp())]})
    assert evaluate(session, task, obj, table, now=now).failures == []


def test_freshness_fails_for_a_stale_batch(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(
        session,
        obj,
        RuleType.FRESHNESS,
        column="updated_at",
        expression='{"max_age_minutes": 60}',
    )
    now = datetime.now(UTC)
    stale = now - timedelta(hours=5)
    table = pa.table({"updated_at": [int(stale.timestamp())]})
    outcome = evaluate(session, task, obj, table, now=now)
    assert not outcome.results[0].passed
    assert "SLA 60" in outcome.results[0].detail


def test_freshness_on_an_empty_batch_fails(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(
        session, obj, RuleType.FRESHNESS, column="updated_at", expression='{"max_age_minutes": 60}'
    )
    empty = pa.table({"updated_at": pa.array([], type=pa.int64())})
    assert not evaluate(session, task, obj, empty).results[0].passed


def test_an_inactive_rule_is_not_evaluated(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="customer_id", active=False)
    assert active_rules(session, obj) == []
    assert evaluate(session, task, obj, TABLE).results == []


# --------------------------------------------------------------------------
# Severity
# --------------------------------------------------------------------------


def test_warn_records_but_keeps_every_row(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="customer_id", severity=Severity.WARN)
    outcome = evaluate(session, task, obj, TABLE)

    assert outcome.kept.num_rows == TABLE.num_rows
    assert outcome.rows_rejected == 0
    assert outcome.status == RunStatus.SUCCEEDED
    assert len(outcome.failures) == 1


def test_quarantine_diverts_only_the_failing_rows(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(
        session,
        obj,
        RuleType.RANGE,
        column="score",
        expression='{"maximum": 100}',
        severity=Severity.QUARANTINE,
    )
    outcome = evaluate(session, task, obj, TABLE)

    assert outcome.rows_rejected == 1
    assert outcome.kept.num_rows == 3
    assert outcome.rejected.column("score").to_pylist() == [200]
    assert outcome.status == RunStatus.QUARANTINED


def test_quarantine_rules_combine(session: Session, obj: SourceObject, task: TaskRun) -> None:
    """A row failing either rule is diverted once, not twice."""
    add_rule(
        session,
        obj,
        RuleType.RANGE,
        column="score",
        expression='{"maximum": 100}',
        severity=Severity.QUARANTINE,
    )
    add_rule(
        session,
        obj,
        RuleType.REGEX,
        column="city",
        expression='{"pattern": "^[a-z]+$"}',
        severity=Severity.QUARANTINE,
    )
    outcome = evaluate(session, task, obj, TABLE)
    assert outcome.rows_rejected == 2
    assert outcome.kept.num_rows == 2


def test_fail_aborts_and_writes_nothing(session: Session, obj: SourceObject, task: TaskRun) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="customer_id", severity=Severity.FAIL)
    with pytest.raises(DataQualityError, match="fail-severity"):
        evaluate(session, task, obj, TABLE)


def test_every_rule_is_evaluated_before_anything_aborts(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    """The first failure is rarely the informative one."""
    add_rule(session, obj, RuleType.NOT_NULL, column="customer_id", severity=Severity.FAIL)
    add_rule(session, obj, RuleType.ROW_COUNT, expression='{"minimum": 99}')

    with pytest.raises(DataQualityError):
        evaluate(session, task, obj, TABLE)

    assert session.query(DataQualityResult).count() == 2, "both results recorded"


def test_results_are_recorded_against_the_task(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    rule = add_rule(session, obj, RuleType.NOT_NULL, column="customer_id")
    evaluate(session, task, obj, TABLE)

    result = session.query(DataQualityResult).one()
    assert result.task_run_id == task.id
    assert result.dq_rule_id == rule.id
    assert result.failed_row_count == 1
    assert result.passed is False


# --------------------------------------------------------------------------
# Misconfiguration
# --------------------------------------------------------------------------


def test_an_unevaluable_rule_type_is_an_error(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    """A silently skipped rule is worse than no rule."""
    add_rule(session, obj, RuleType.CUSTOM_SQL, column="score", expression='{"sql": "1=1"}')
    with pytest.raises(RuleEvaluationError, match="cannot evaluate"):
        evaluate(session, task, obj, TABLE)


def test_a_rule_naming_an_absent_column_is_an_error(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="nope")
    with pytest.raises(RuleEvaluationError, match="not in the batch"):
        evaluate(session, task, obj, TABLE)


def test_a_rule_with_no_column_is_an_error(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.NOT_NULL)
    with pytest.raises(RuleEvaluationError, match="needs a column"):
        evaluate(session, task, obj, TABLE)


def test_an_unparseable_expression_is_an_error(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.RANGE, column="score", expression="not json")
    with pytest.raises(RuleEvaluationError, match="unparseable"):
        evaluate(session, task, obj, TABLE)


def test_a_range_with_no_bounds_is_an_error(
    session: Session, obj: SourceObject, task: TaskRun
) -> None:
    add_rule(session, obj, RuleType.RANGE, column="score", expression="{}")
    with pytest.raises(RuleEvaluationError, match="neither bound"):
        evaluate(session, task, obj, TABLE)


# --------------------------------------------------------------------------
# Wired into Silver
# --------------------------------------------------------------------------


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def test_silver_drops_quarantined_rows_and_audits_them(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    add_rule(
        session,
        obj,
        RuleType.RANGE,
        column="score",
        expression='{"maximum": 100}',
        severity=Severity.QUARANTINE,
    )
    table = pa.table({"customer_id": ["c1", "c2"], "score": [50, 500], "updated_at": [1, 2]})
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(table), tmp_path)

    result = build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert result.quality is not None
    assert result.quality.rows_rejected == 1
    assert read_silver(tmp_path, obj).num_rows == 1

    task = session.query(TaskRun).filter(TaskRun.layer == Layer.SILVER).one()
    assert task.status == RunStatus.QUARANTINED
    assert task.rows_rejected == 1


def test_a_fail_rule_stops_the_silver_build(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    add_rule(session, obj, RuleType.NOT_NULL, column="city", severity=Severity.FAIL)
    table = pa.table({"customer_id": ["c1"], "city": [None], "updated_at": [1]})
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(table), tmp_path)

    run = start_pipeline_run(session, "silver")
    with pytest.raises(DataQualityError):
        build_silver(session, run, obj, tmp_path)

    task = session.query(TaskRun).filter(TaskRun.layer == Layer.SILVER).one()
    assert task.status == RunStatus.FAILED
