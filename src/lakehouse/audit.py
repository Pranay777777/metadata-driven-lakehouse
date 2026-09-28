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

**Aggregation happened in Python.** `object_health` and
`rule_performance` issued one query per object and per rule, then counted
rows in a loop — fine at five objects, a page that takes minutes at five
hundred. Every reporting query here is now a single statement, and a test
counts the statements to keep it that way. The dashboard (step 38, ADR-019)
reads only through this module.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import String, and_, case, func, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.functions import FunctionElement

from lakehouse.ingest.bronze import new_run_id
from lakehouse.metadata.enums import GoldRole, Layer, RunStatus, Sensitivity, Severity
from lakehouse.metadata.models import (
    ColumnMetadata,
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


ABANDONED_AFTER = timedelta(hours=24)
"""How long a run may stay 'running' before it is presumed dead. Longer
than any batch this platform runs, short enough that yesterday's crash
does not read as in flight."""


def close_abandoned(
    session: Session,
    older_than: timedelta = ABANDONED_AFTER,
    now: datetime | None = None,
    apply: bool = False,
) -> list[tuple[str, str]]:
    """Close runs left 'running' by a process that is no longer alive.

    Returns `(run_id, status it closes as)`. With `apply` false nothing
    is written: the control plane is the audit record, so a repair to it
    is shown before it is made.

    The status is derived from the run's tasks, with two exceptions that
    `derive_status` does not need to handle for a run closing normally:
    a run with no tasks never got going, and a run with a task still
    'running' died mid-task. Both close as failed. The end time is the
    last task's, so the run's duration stays truthful.

    Exists mainly because of a bug fixed in step 38: the CLI and Dagster
    opened Silver and Gold runs without closing them, so every run before
    the fix left two of these behind.
    """
    cutoff = (now or datetime.now(UTC)) - older_than
    closed: list[tuple[str, str]] = []
    abandoned = session.scalars(
        select(PipelineRun)
        .where(PipelineRun.status == RunStatus.RUNNING, PipelineRun.started_at < cutoff)
        .order_by(PipelineRun.started_at)
    ).all()
    for run in abandoned:
        statuses, last_end = session.execute(
            select(
                func.count(TaskRun.id),
                func.max(TaskRun.ended_at),
            ).where(TaskRun.run_id == run.run_id)
        ).one()
        still_running = session.scalar(
            select(func.count(TaskRun.id)).where(
                TaskRun.run_id == run.run_id, TaskRun.status == RunStatus.RUNNING
            )
        )
        if not statuses or still_running:
            status: str = RunStatus.FAILED
        else:
            status = derive_status(session, run.run_id)
        closed.append((run.run_id, status))
        if apply:
            run.status = status
            run.ended_at = last_end or run.started_at
    if apply:
        session.commit()
    return closed


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


def _ranked_tasks(layer: str | None = None) -> Any:
    """Task runs numbered newest-first within each object.

    The window function is what lets "the last N tasks per object" be one
    statement instead of one query per object. `id` breaks ties, because
    `started_at` has one-second resolution and a pipeline starts several
    tasks inside the same second.
    """
    query = select(
        TaskRun.source_object_id,
        TaskRun.run_id,
        TaskRun.status,
        TaskRun.ended_at,
        TaskRun.rows_written,
        TaskRun.rows_rejected,
        func.row_number()
        .over(
            partition_by=TaskRun.source_object_id,
            order_by=(TaskRun.started_at.desc(), TaskRun.id.desc()),
        )
        .label("rn"),
    )
    if layer is not None:
        query = query.where(TaskRun.layer == layer)
    return query.subquery()


def object_health(session: Session, limit_per_object: int = 20) -> list[ObjectHealth]:
    """A row per configured object, summarising its recent task runs."""
    ranked = _ranked_tasks()
    recent = (
        select(
            ranked.c.source_object_id,
            func.sum(ranked.c.rows_written).label("written"),
            func.sum(ranked.c.rows_rejected).label("rejected"),
            func.sum(case((ranked.c.status == RunStatus.FAILED, 1), else_=0)).label("failures"),
        )
        .where(ranked.c.rn <= limit_per_object)
        .group_by(ranked.c.source_object_id)
        .subquery()
    )
    latest = select(ranked).where(ranked.c.rn == 1).subquery()

    rows = session.execute(
        select(
            SourceObject.object_name,
            SourceObject.owner,
            latest.c.run_id,
            latest.c.status,
            latest.c.ended_at,
            func.coalesce(recent.c.written, 0),
            func.coalesce(recent.c.rejected, 0),
            func.coalesce(recent.c.failures, 0),
        )
        .outerjoin(recent, recent.c.source_object_id == SourceObject.id)
        .outerjoin(latest, latest.c.source_object_id == SourceObject.id)
        .order_by(SourceObject.object_name)
    ).all()
    return [
        ObjectHealth(
            object_name=name,
            owner=owner,
            last_run_id=run_id,
            last_status=status,
            last_loaded_at=ended,
            rows_written=int(written),
            rows_rejected=int(rejected),
            failures=int(failures),
        )
        for name, owner, run_id, status, ended, written, rejected, failures in rows
    ]


def rule_performance(session: Session) -> list[RulePerformance]:
    """Pass rate per rule across every evaluation recorded."""
    rows = session.execute(
        select(
            DataQualityRule.id,
            DataQualityRule.rule_type,
            DataQualityRule.column_name,
            DataQualityRule.severity,
            func.count(DataQualityResult.id),
            func.coalesce(func.sum(case((~DataQualityResult.passed, 1), else_=0)), 0),
        )
        .outerjoin(DataQualityResult, DataQualityResult.dq_rule_id == DataQualityRule.id)
        .group_by(
            DataQualityRule.id,
            DataQualityRule.rule_type,
            DataQualityRule.column_name,
            DataQualityRule.severity,
        )
        .order_by(DataQualityRule.id)
    ).all()
    return [
        RulePerformance(
            rule_id=rule_id,
            rule_type=rule_type,
            column=column,
            severity=severity,
            evaluations=int(evaluations),
            failures=int(failures),
        )
        for rule_id, rule_type, column, severity, evaluations, failures in rows
    ]


# --------------------------------------------------------------------------
# Operational views — what the dashboard shows (ADR-019)
# --------------------------------------------------------------------------


class day(FunctionElement[str]):  # noqa: N801 — reads as SQL: day(started_at)
    """The calendar day of a timestamp, portable across the three dialects.

    `CAST(x AS DATE)` is correct on Postgres and Azure SQL and silently
    wrong on SQLite, where it yields the year as a number. `date(x)` is
    correct on SQLite and Postgres and does not exist on Azure SQL. So the
    expression is compiled per dialect rather than picked once.
    """

    type = String()
    name = "day"
    inherit_cache = True


@compiles(day)
def _day_default(element: day, compiler: SQLCompiler, **kw: Any) -> str:
    return f"date({compiler.process(element.clauses, **kw)})"


@compiles(day, "mssql")
def _day_mssql(element: day, compiler: SQLCompiler, **kw: Any) -> str:
    return f"CAST({compiler.process(element.clauses, **kw)} AS DATE)"


def _aware(moment: datetime | None) -> datetime | None:
    """SQLite hands back naive datetimes; everything here is UTC."""
    if moment is None or moment.tzinfo is not None:
        return moment
    return moment.replace(tzinfo=UTC)


_COMPLETED = (RunStatus.SUCCEEDED, RunStatus.QUARANTINED)
"""Statuses that mean the data landed. A quarantined build still wrote the
rows that passed, so it counts as fresh."""


@dataclass(frozen=True)
class Freshness:
    """How long since an object last landed, against its SLA."""

    object_name: str
    owner: str | None
    sla_minutes: int | None
    last_loaded_at: datetime | None
    age_minutes: float | None

    @property
    def breached(self) -> bool:
        """Never loaded counts as breached when there is an SLA to breach."""
        if self.sla_minutes is None:
            return False
        return self.age_minutes is None or self.age_minutes > self.sla_minutes


def freshness(
    session: Session, now: datetime | None = None, layer: str = Layer.SILVER
) -> list[Freshness]:
    """Minutes since each object's last completed build in `layer`.

    Silver by default: it is the first layer anyone should query, so its
    age is the age of what analysts actually see. This measures when the
    platform last *loaded* an object, not how new the newest source row
    is — the latter needs the data, and this module reads only the audit
    trail.
    """
    moment = now or datetime.now(UTC)
    last = (
        select(TaskRun.source_object_id, func.max(TaskRun.ended_at).label("ended"))
        .where(TaskRun.layer == layer, TaskRun.status.in_(_COMPLETED))
        .group_by(TaskRun.source_object_id)
        .subquery()
    )
    rows = session.execute(
        select(
            SourceObject.object_name,
            SourceObject.owner,
            SourceObject.freshness_sla_minutes,
            last.c.ended,
        )
        .outerjoin(last, last.c.source_object_id == SourceObject.id)
        .where(SourceObject.active.is_(True))
        .order_by(SourceObject.object_name)
    ).all()
    result = []
    for name, owner, sla, ended in rows:
        loaded = _aware(ended)
        age = (moment - loaded).total_seconds() / 60 if loaded else None
        result.append(Freshness(name, owner, sla, loaded, age))
    return result


@dataclass(frozen=True)
class VolumePoint:
    """One layer on one day."""

    day: str
    layer: str
    tasks: int
    rows_written: int
    compute_seconds: int
    """Summed task duration — the cost proxy. See ADR-019 for why there is
    no currency figure."""


def volume_trend(
    session: Session, days: int = 30, now: datetime | None = None
) -> list[VolumePoint]:
    """Rows written and compute time per layer per day."""
    since = (now or datetime.now(UTC)) - timedelta(days=days)
    bucket = day(TaskRun.started_at)
    rows = session.execute(
        select(
            bucket,
            TaskRun.layer,
            func.count(TaskRun.id),
            func.coalesce(func.sum(TaskRun.rows_written), 0),
            func.coalesce(func.sum(TaskRun.duration_seconds), 0),
        )
        .where(TaskRun.started_at >= since)
        .group_by(bucket, TaskRun.layer)
        .order_by(bucket, TaskRun.layer)
    ).all()
    return [
        # Postgres returns a date, SQLite a string; both print as ISO.
        VolumePoint(str(d)[:10], layer, int(n), int(written or 0), int(seconds or 0))
        for d, layer, n, written, seconds in rows
    ]


@dataclass(frozen=True)
class QuarantineCount:
    """Rows one object has diverted to quarantine, over all runs."""

    object_name: str
    rows_quarantined: int
    breaches: int
    """Evaluations of a quarantine-severity rule that failed."""


def quarantine_summary(session: Session) -> list[QuarantineCount]:
    """Rows diverted by quarantine-severity rules, per object.

    Read from `dq_result.failed_row_count`, not `task_run.rows_rejected`.
    Silver's rejected count also includes removed duplicates and late
    rows skipped by SCD2, and a dashboard that calls those "quarantined"
    would be wrong in the direction that causes panic.
    """
    rows = session.execute(
        select(
            SourceObject.object_name,
            func.coalesce(func.sum(DataQualityResult.failed_row_count), 0),
            func.coalesce(func.sum(case((~DataQualityResult.passed, 1), else_=0)), 0),
        )
        .join(DataQualityRule, DataQualityRule.source_object_id == SourceObject.id)
        .join(DataQualityResult, DataQualityResult.dq_rule_id == DataQualityRule.id)
        .where(DataQualityRule.severity == Severity.QUARANTINE)
        .group_by(SourceObject.object_name)
        .order_by(SourceObject.object_name)
    ).all()
    return [QuarantineCount(name, int(rows_), int(breaches)) for name, rows_, breaches in rows]


@dataclass(frozen=True)
class UnknownMembers:
    """Fact rows that fell back to a dimension's unknown member."""

    object_name: str
    rows: int
    run_id: str


def unknown_members(session: Session) -> list[UnknownMembers]:
    """Unknown-member fallbacks in each fact's latest Gold build.

    Gold records these as the task's `rows_rejected` (ADR-006): the rows
    are kept, but they point at key 0 because their dimension member had
    not loaded. Only the latest build matters — Gold is rebuilt in full,
    so older counts describe tables that no longer exist.
    """
    ranked = _ranked_tasks(Layer.GOLD)
    rows = session.execute(
        select(SourceObject.object_name, ranked.c.rows_rejected, ranked.c.run_id)
        .join(ranked, ranked.c.source_object_id == SourceObject.id)
        .where(ranked.c.rn == 1, SourceObject.gold_role == GoldRole.FACT)
        .order_by(SourceObject.object_name)
    ).all()
    return [UnknownMembers(name, int(count), run_id) for name, count, run_id in rows]


@dataclass(frozen=True)
class ColumnPosture:
    """How one classified column is handled on its way through the lake."""

    object_name: str
    column_name: str
    sensitivity: str
    strategy: str
    reaches_gold: bool


def privacy_posture(session: Session) -> list[ColumnPosture]:
    """Every classified column, with its masking and Gold treatment."""
    withheld = and_(
        ColumnMetadata.sensitivity == Sensitivity.SENSITIVE_PII,
        ColumnMetadata.allow_in_gold.is_(False),
    )
    rows = session.execute(
        select(
            SourceObject.object_name,
            ColumnMetadata.column_name,
            ColumnMetadata.sensitivity,
            ColumnMetadata.masking_strategy,
            case((withheld, 0), else_=1),
        )
        .join(SourceObject, SourceObject.id == ColumnMetadata.source_object_id)
        .where(ColumnMetadata.sensitivity != Sensitivity.NONE)
        .order_by(SourceObject.object_name, ColumnMetadata.column_name)
    ).all()
    return [
        ColumnPosture(name, column, sensitivity, strategy, bool(in_gold))
        for name, column, sensitivity, strategy, in_gold in rows
    ]


@dataclass(frozen=True)
class Snapshot:
    """Everything the dashboard renders, read in one place.

    Assembled here rather than in the app so the whole page is testable
    without Streamlit, and so the app stays a renderer with no logic to
    get wrong.
    """

    taken_at: datetime
    runs: list[RunSummary]
    stale: list[PipelineRun]
    health: list[ObjectHealth]
    freshness: list[Freshness]
    volume: list[VolumePoint]
    rules: list[RulePerformance]
    quarantine: list[QuarantineCount]
    unknown: list[UnknownMembers]
    privacy: list[ColumnPosture]
    notes: list[str] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not self.health

    @property
    def rows_quarantined(self) -> int:
        return sum(q.rows_quarantined for q in self.quarantine)

    @property
    def unknown_total(self) -> int:
        return sum(u.rows for u in self.unknown)

    @property
    def freshness_breaches(self) -> int:
        return sum(1 for f in self.freshness if f.breached)

    @property
    def masked_columns(self) -> int:
        return sum(1 for c in self.privacy if c.strategy != "none")


def snapshot(
    session: Session, now: datetime | None = None, runs: int = 10, days: int = 30
) -> Snapshot:
    """Read every view the dashboard shows, against one clock."""
    moment = now or datetime.now(UTC)
    summaries = [
        summary
        for run in recent_runs(session, runs)
        if (summary := summarise_run(session, run.run_id)) is not None
    ]
    return Snapshot(
        taken_at=moment,
        runs=summaries,
        stale=stale_runs(session),
        health=object_health(session),
        freshness=freshness(session, moment),
        volume=volume_trend(session, days, moment),
        rules=rule_performance(session),
        quarantine=quarantine_summary(session),
        unknown=unknown_members(session),
        privacy=privacy_posture(session),
    )


__all__ = [
    "ABANDONED_AFTER",
    "ColumnPosture",
    "Freshness",
    "ObjectHealth",
    "QuarantineCount",
    "RulePerformance",
    "RunSummary",
    "Snapshot",
    "StageSummary",
    "UnknownMembers",
    "VolumePoint",
    "close_abandoned",
    "day",
    "derive_status",
    "freshness",
    "object_health",
    "privacy_posture",
    "quarantine_summary",
    "recent_runs",
    "rule_performance",
    "snapshot",
    "stale_runs",
    "summarise_run",
    "track_run",
    "unknown_members",
    "volume_trend",
]


def main(argv: list[str] | None = None) -> int:
    """`python -m lakehouse.audit --close-abandoned [--apply]`."""
    import argparse

    from sqlalchemy import create_engine

    from lakehouse.config import Settings
    from lakehouse.credentials import database_url

    parser = argparse.ArgumentParser(prog="lakehouse.audit", description=__doc__)
    parser.add_argument(
        "--close-abandoned",
        action="store_true",
        help="close runs left 'running' by a process that is no longer alive",
    )
    parser.add_argument("--older-than-hours", type=float, default=24.0)
    parser.add_argument("--apply", action="store_true", help="write the repair (default: show it)")
    args = parser.parse_args(argv)
    if not args.close_abandoned:
        parser.print_help()
        return 2

    engine = create_engine(database_url(Settings()))
    with Session(engine) as session:
        closed = close_abandoned(session, timedelta(hours=args.older_than_hours), apply=args.apply)
    for run_id, status in closed:
        print(f"  {run_id}  →  {status}")
    verb = "closed" if args.apply else "would close"
    print(f"{verb} {len(closed)} abandoned run(s)")
    if closed and not args.apply:
        print("nothing written — re-run with --apply to close them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
