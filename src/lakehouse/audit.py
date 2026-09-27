"""Reading the audit trail.

Every loader has been writing `pipeline_run`, `task_run` and `dq_result`
rows since step 18. Nothing has ever read them. An audit table nobody
queries is a table nobody notices is wrong, so this module is both the
reporting layer and the thing that proves the instrumentation works.

Two gaps are closed here as well:

**A crashed process left its run open forever.** `start_pipeline_run`
persists a `running` row up front deliberately, so a killed process
leaves evidence rather than nothing. But nothing closed it on the way
out, so "still running" and "died three weeks ago" looked identical.
`track_run` is a context manager that always closes the run, with
`failed` if an exception escaped.

**A run's status ignored its tasks.** A run whose every task failed
still reported success, because the status was whatever the caller
passed. It is now derived from the tasks: any failure fails the run, any
quarantine marks it quarantined.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import new_run_id
from lakehouse.metadata.enums import RunStatus
from lakehouse.metadata.models import (
    DataQualityResult,
    DataQualityRule,
    PipelineRun,
    SourceObject,
    TaskRun,
)


@dataclass(frozen=True)
class StageSummary:
    """One layer's contribution to a run."""

    layer: str
    tasks: int
    rows_read: int
    rows_written: int
    rows_rejected: int
    failed: int

    @property
    def rejection_rate(self) -> float:
        return self.rows_rejected / self.rows_read if self.rows_read else 0.0


@dataclass(frozen=True)
class RunSummary:
    """Everything worth knowing about one pipeline run."""

    run_id: str
    pipeline_name: str
    status: str
    triggered_by: str | None
    started_at: datetime
    ended_at: datetime | None
    stages: list[StageSummary]

    @property
    def duration_seconds(self) -> float | None:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.started_at).total_seconds()

    @property
    def rows_rejected(self) -> int:
        return sum(s.rows_rejected for s in self.stages)

    @property
    def failed_tasks(self) -> int:
        return sum(s.failed for s in self.stages)


@dataclass(frozen=True)
class ObjectHealth:
    """How one source object has been behaving lately."""

    object_name: str
    owner: str | None
    last_run_id: str | None
    last_status: str | None
    last_loaded_at: datetime | None
    rows_written: int
    rows_rejected: int
    failures: int


@dataclass(frozen=True)
class RulePerformance:
    """How often one rule has been breached."""

    rule_id: int
    rule_type: str
    column: str | None
    severity: str
    evaluations: int
    failures: int

    @property
    def pass_rate(self) -> float:
        if not self.evaluations:
            return 1.0
        return (self.evaluations - self.failures) / self.evaluations


def derive_status(session: Session, run_id: str) -> str:
    """A run is only as healthy as its worst task."""
    statuses = set(session.scalars(select(TaskRun.status).where(TaskRun.run_id == run_id)).all())
    if RunStatus.FAILED in statuses:
        return RunStatus.FAILED
    if RunStatus.QUARANTINED in statuses:
        return RunStatus.QUARANTINED
    return RunStatus.SUCCEEDED


@contextmanager
def track_run(session: Session, name: str, triggered_by: str = "manual") -> Iterator[PipelineRun]:
    """Open a run and guarantee it is closed, however the block exits.

    An exception closes the run as failed and is re-raised. Swallowing
    it would turn a crash into a silent no-op, which is the failure mode
    the audit trail exists to prevent.
    """
    run = PipelineRun(run_id=new_run_id(), pipeline_name=name, triggered_by=triggered_by)
    session.add(run)
    session.commit()
    try:
        yield run
    except Exception:
        run.status = RunStatus.FAILED
        run.ended_at = datetime.now(UTC)
        session.commit()
        raise
    run.status = derive_status(session, run.run_id)
    run.ended_at = datetime.now(UTC)
    session.commit()


def summarise_run(session: Session, run_id: str) -> RunSummary | None:
    """Per-stage totals for one run, or None if there is no such run."""
    run = session.get(PipelineRun, run_id)
    if run is None:
        return None

    rows = session.execute(
        select(
            TaskRun.layer,
            func.count(TaskRun.id),
            func.coalesce(func.sum(TaskRun.rows_read), 0),
            func.coalesce(func.sum(TaskRun.rows_written), 0),
            func.coalesce(func.sum(TaskRun.rows_rejected), 0),
            # CASE rather than IIF: Postgres has no IIF, and this DDL targets it.
            func.sum(case((TaskRun.status == RunStatus.FAILED, 1), else_=0)),
        )
        .where(TaskRun.run_id == run_id)
        .group_by(TaskRun.layer)
        .order_by(TaskRun.layer)
    ).all()

    stages = [
        StageSummary(
            layer=layer,
            tasks=tasks,
            rows_read=int(read),
            rows_written=int(written),
            rows_rejected=int(rejected),
            failed=int(failed or 0),
        )
        for layer, tasks, read, written, rejected, failed in rows
    ]
    return RunSummary(
        run_id=run.run_id,
        pipeline_name=run.pipeline_name,
        status=run.status,
        triggered_by=run.triggered_by,
        started_at=run.started_at,
        ended_at=run.ended_at,
        stages=stages,
    )


def recent_runs(session: Session, limit: int = 10) -> list[PipelineRun]:
    """The most recent runs, newest first."""
    # `started_at` has one-second resolution, so runs started in the
    # same second tie. `run_id` breaks it, which makes the order stable
    # across calls rather than whatever the database happens to return.
    return list(
        session.scalars(
            select(PipelineRun)
            .order_by(PipelineRun.started_at.desc(), PipelineRun.run_id.desc())
            .limit(limit)
        )
    )


def stale_runs(session: Session) -> list[PipelineRun]:
    """Runs still marked running — a crashed process, or one in flight."""
    return list(session.scalars(select(PipelineRun).where(PipelineRun.status == RunStatus.RUNNING)))


def object_health(session: Session, limit_per_object: int = 20) -> list[ObjectHealth]:
    """A row per configured object, summarising its recent task runs."""
    health: list[ObjectHealth] = []
    for obj in session.scalars(select(SourceObject).order_by(SourceObject.object_name)):
        tasks = list(
            session.scalars(
                select(TaskRun)
                .where(TaskRun.source_object_id == obj.id)
                .order_by(TaskRun.started_at.desc())
                .limit(limit_per_object)
            )
        )
        latest = tasks[0] if tasks else None
        health.append(
            ObjectHealth(
                object_name=obj.object_name,
                owner=obj.owner,
                last_run_id=latest.run_id if latest else None,
                last_status=latest.status if latest else None,
                last_loaded_at=latest.ended_at if latest else None,
                rows_written=sum(t.rows_written for t in tasks),
                rows_rejected=sum(t.rows_rejected for t in tasks),
                failures=sum(1 for t in tasks if t.status == RunStatus.FAILED),
            )
        )
    return health


def rule_performance(session: Session) -> list[RulePerformance]:
    """Pass rate per rule across every evaluation recorded."""
    performance: list[RulePerformance] = []
    for rule in session.scalars(select(DataQualityRule).order_by(DataQualityRule.id)):
        results = list(
            session.scalars(
                select(DataQualityResult).where(DataQualityResult.dq_rule_id == rule.id)
            )
        )
        performance.append(
            RulePerformance(
                rule_id=rule.id,
                rule_type=rule.rule_type,
                column=rule.column_name,
                severity=rule.severity,
                evaluations=len(results),
                failures=sum(1 for r in results if not r.passed),
            )
        )
    return performance
