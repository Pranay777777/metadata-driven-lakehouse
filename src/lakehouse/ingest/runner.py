"""The pipeline runner.

This is where the metadata-driven claim is either true or it is not.
Nothing in this module knows what `orders` or `customers` are. It reads
the active rows of `source_object`, dispatches each to the loader named
by its `load_strategy`, and records what happened.

Two behaviours matter more than the dispatch itself:

**Failure isolation.** One unreachable source must not stop the other
nineteen. Each object is loaded independently; failures are recorded and
the run continues, then the run as a whole is marked failed. The
alternative — abort on first error — means a single flaky source leaves
the entire lake stale.

**Resume.** Re-running with the same `run_id` skips objects that already
succeeded in that run. Combined with the crash-safety of the individual
loaders, a pipeline that dies two thirds of the way through can be
restarted without redoing work and without duplicating rows.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import (
    Source,
    finish_pipeline_run,
    load_full,
    start_pipeline_run,
)
from lakehouse.ingest.cdc import load_cdc
from lakehouse.ingest.incremental import FilteringSource, load_incremental
from lakehouse.metadata.enums import LoadStrategy, RunStatus
from lakehouse.metadata.models import PipelineRun, SourceObject, TaskRun

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ObjectOutcome:
    """What happened to one object in one run."""

    object_name: str
    strategy: str
    status: RunStatus
    rows_written: int = 0
    error: str | None = None
    skipped_reason: str | None = None


@dataclass
class RunSummary:
    """Aggregate result of a pipeline run."""

    run_id: str
    outcomes: list[ObjectOutcome] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def succeeded(self) -> list[ObjectOutcome]:
        return [o for o in self.outcomes if o.status is RunStatus.SUCCEEDED]

    @property
    def failed(self) -> list[ObjectOutcome]:
        return [o for o in self.outcomes if o.status is RunStatus.FAILED]

    @property
    def skipped(self) -> list[ObjectOutcome]:
        return [o for o in self.outcomes if o.status is RunStatus.SKIPPED]

    @property
    def rows_written(self) -> int:
        return sum(o.rows_written for o in self.outcomes)

    @property
    def status(self) -> RunStatus:
        return RunStatus.FAILED if self.failed else RunStatus.SUCCEEDED

    def report(self) -> str:
        """One line per object, for logs and the CLI."""
        lines = [f"run {self.run_id}"]
        for o in self.outcomes:
            mark = {"succeeded": "ok", "failed": "FAIL", "skipped": "skip"}.get(o.status, "?")
            detail = o.error or o.skipped_reason or f"{o.rows_written:,} rows"
            lines.append(f"  {mark:<5}{o.object_name:<20}{o.strategy:<14}{detail}")
        lines.append(
            f"  {len(self.succeeded)} ok, {len(self.failed)} failed, "
            f"{len(self.skipped)} skipped, {self.rows_written:,} rows, "
            f"{self.duration_seconds:.1f}s"
        )
        return "\n".join(lines)


def active_objects(session: Session, names: list[str] | None = None) -> list[SourceObject]:
    """Active source objects in configured load order."""
    stmt = select(SourceObject).where(SourceObject.active.is_(True))
    if names is not None:
        stmt = stmt.where(SourceObject.object_name.in_(names))
    stmt = stmt.order_by(SourceObject.load_order, SourceObject.id)
    return list(session.scalars(stmt))


def completed_in_run(session: Session, run_id: str) -> set[int]:
    """Object ids that already succeeded in this run — used to resume."""
    stmt = select(TaskRun.source_object_id).where(
        TaskRun.run_id == run_id, TaskRun.status == RunStatus.SUCCEEDED
    )
    return set(session.scalars(stmt))


def _dispatch(
    session: Session,
    run: PipelineRun,
    obj: SourceObject,
    source: Source,
    lake_root: Path,
) -> int:
    """Route one object to the loader its configuration names."""
    strategy = LoadStrategy(obj.load_strategy)
    if strategy is LoadStrategy.FULL:
        return load_full(session, run, obj, source, lake_root).rows_written
    if strategy is LoadStrategy.INCREMENTAL:
        return load_incremental(session, run, obj, FilteringSource(source), lake_root).rows_written
    return load_cdc(session, run, obj, source, lake_root).rows_written


def run_pipeline(
    session: Session,
    source: Source,
    lake_root: Path,
    pipeline_name: str = "bronze",
    object_names: list[str] | None = None,
    resume_run_id: str | None = None,
    stop_on_error: bool = False,
) -> RunSummary:
    """Load every active object, dispatching on its configured strategy.

    Args:
        object_names: restrict the run to these objects.
        resume_run_id: continue an existing run, skipping what succeeded.
        stop_on_error: abort on the first failure instead of isolating it.
            Off by default — one bad source should not stale the lake.
    """
    started = time.perf_counter()
    if resume_run_id:
        run = session.get(PipelineRun, resume_run_id)
        if run is None:
            raise ValueError(f"cannot resume unknown run '{resume_run_id}'")
        run.status = RunStatus.RUNNING
        session.commit()
        already = completed_in_run(session, run.run_id)
    else:
        run = start_pipeline_run(session, pipeline_name)
        already = set()

    summary = RunSummary(run_id=run.run_id)

    for obj in active_objects(session, object_names):
        if obj.id in already:
            summary.outcomes.append(
                ObjectOutcome(
                    object_name=obj.object_name,
                    strategy=obj.load_strategy,
                    status=RunStatus.SKIPPED,
                    skipped_reason="already loaded in this run",
                )
            )
            continue

        try:
            written = _dispatch(session, run, obj, source, lake_root)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            logger.error("object %s failed: %s", obj.object_name, message)
            summary.outcomes.append(
                ObjectOutcome(
                    object_name=obj.object_name,
                    strategy=obj.load_strategy,
                    status=RunStatus.FAILED,
                    error=message,
                )
            )
            if stop_on_error:
                break
            continue

        summary.outcomes.append(
            ObjectOutcome(
                object_name=obj.object_name,
                strategy=obj.load_strategy,
                status=RunStatus.SUCCEEDED,
                rows_written=written,
            )
        )

    summary.duration_seconds = time.perf_counter() - started
    finish_pipeline_run(session, run, summary.status)
    return summary
