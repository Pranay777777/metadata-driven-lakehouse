"""Replay: rewind a source, or restore a table, to a point in time.

Two situations need this, and they need different tools.

**The source sent bad data and has since fixed it.** Everything loaded
after some date is wrong and the corrected rows exist upstream. The fix
is to rewind the source's watermark to that date so the next run
re-reads everything since. Silver deduplicates on key and sequence
(ADR-004), so re-reading an overlap is safe rather than doubling rows.

**A transform wrote something wrong into the lake.** The source was
fine; a Silver or Gold table is not. The fix is Delta time travel:
restore the table to the version it had at a given moment. Delta records
the restore as a new commit, so it is itself reversible.

## The watermark rule, and its one exception

Design rule two says the watermark never moves backwards. That rule
exists to stop *automatic* regression — a grace window, a clock skew, a
bug — from silently re-reading or skipping data. Replay moves it
backwards on purpose, which is the only legitimate way for that to
happen. So a rewind is:

- **explicit** — only ever invoked by a person, never by the pipeline;
- **audited** — recorded as a task run with the old and new values, so
  "why did orders reload four months of data on Tuesday" has an answer;
- **dry-run by default** — the command prints what it would do.

Restores obey the same dry-run default, and the maintenance module's
one-week vacuum floor exists partly so that this tool has history to
travel to.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path

from deltalake import DeltaTable
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.credentials import database_url
from lakehouse.ingest.bronze import start_pipeline_run
from lakehouse.metadata.enums import Layer, LoadStrategy, RunStatus, WatermarkType
from lakehouse.metadata.models import LoadWatermark, SourceObject, TaskRun


class ReplayError(Exception):
    """Raised when a replay is impossible or unsafe."""


@dataclass(frozen=True)
class TableVersion:
    version: int
    timestamp: datetime
    operation: str


@dataclass(frozen=True)
class RewindPlan:
    """What a rewind would change, before it changes anything."""

    object_name: str
    previous: str | None
    new: str
    watermark_type: str


def parse_moment(text: str) -> datetime:
    """A date or ISO timestamp, interpreted as UTC.

    A bare date means the start of that day, which is what anyone
    typing `--from 2026-03-01` means.
    """
    try:
        if len(text) == 10:
            return datetime.combine(date.fromisoformat(text), time.min, tzinfo=UTC)
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ReplayError(f"'{text}' is not a date (YYYY-MM-DD) or ISO timestamp") from exc
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def history(path: Path) -> list[TableVersion]:
    """Every retained version of a table, oldest first."""
    if not (path / "_delta_log").exists():
        raise ReplayError(f"no Delta table at {path}")
    versions = []
    for entry in DeltaTable(str(path)).history():
        stamp = entry.get("timestamp")
        moment = (
            datetime.fromtimestamp(int(stamp) / 1000, tz=UTC)
            if stamp is not None
            else datetime.min.replace(tzinfo=UTC)
        )
        versions.append(
            TableVersion(
                version=int(entry["version"]),
                timestamp=moment,
                operation=str(entry.get("operation", "")),
            )
        )
    return sorted(versions, key=lambda v: v.version)


def version_at(path: Path, moment: datetime) -> int:
    """The newest version committed at or before `moment`.

    Raises:
        ReplayError: if the table did not exist yet, or its history
            before that moment has been vacuumed away.
    """
    candidates = [v for v in history(path) if v.timestamp <= moment]
    if not candidates:
        raise ReplayError(
            f"{path} has no version at or before {moment.isoformat()} — "
            "it did not exist yet, or that history has been vacuumed"
        )
    return max(v.version for v in candidates)


def restore(path: Path, moment: datetime, dry_run: bool = True) -> int:
    """Restore a table to its state at `moment`. Returns the target version.

    The restore is a new commit, so it can itself be undone by restoring
    to the version before it.
    """
    target = version_at(path, moment)
    if not dry_run:
        DeltaTable(str(path)).restore(target)
    return target


def render_watermark(moment: datetime, watermark_type: str) -> str:
    """Express a moment in the watermark's own type."""
    if watermark_type == WatermarkType.TIMESTAMP:
        return moment.isoformat()
    if watermark_type == WatermarkType.INTEGER:
        # Integer watermarks on these sources are epoch seconds.
        return str(int(moment.timestamp()))
    raise ReplayError(
        f"cannot rewind a {watermark_type} watermark to a moment in time — "
        "string keys have no ordering relationship with dates"
    )


def plan_rewind(session: Session, obj: SourceObject, moment: datetime) -> RewindPlan:
    """Work out what a rewind would do, without doing it."""
    if obj.load_strategy != LoadStrategy.INCREMENTAL:
        raise ReplayError(
            f"'{obj.object_name}' loads {obj.load_strategy}, so it has no watermark to "
            "rewind — the next run already reloads everything"
        )
    stored = session.get(LoadWatermark, obj.id)
    watermark_type = stored.watermark_type if stored else WatermarkType.INTEGER
    return RewindPlan(
        object_name=obj.object_name,
        previous=stored.watermark_value if stored else None,
        new=render_watermark(moment, watermark_type),
        watermark_type=watermark_type,
    )


def rewind(session: Session, obj: SourceObject, moment: datetime) -> RewindPlan:
    """Move the watermark back to `moment`, and record that it happened.

    This is the one sanctioned way for a watermark to move backwards.
    It is recorded as a task run carrying both values, so a replay is
    never indistinguishable from a bug.
    """
    plan = plan_rewind(session, obj, moment)

    run = start_pipeline_run(session, "replay", triggered_by="operator")
    task = TaskRun(
        run_id=run.run_id,
        source_object_id=obj.id,
        layer=Layer.BRONZE,
        status=RunStatus.SUCCEEDED,
        watermark_from=plan.previous,
        watermark_to=plan.new,
        error_message=f"replay: watermark rewound from {plan.previous} to {plan.new}",
        ended_at=datetime.now(UTC),
    )
    session.add(task)

    stored = session.get(LoadWatermark, obj.id)
    if stored is None:
        session.add(
            LoadWatermark(
                source_object_id=obj.id,
                watermark_value=plan.new,
                watermark_type=plan.watermark_type,
                committed_run_id=run.run_id,
            )
        )
    else:
        stored.watermark_value = plan.new
        stored.committed_run_id = run.run_id

    run.status = RunStatus.SUCCEEDED
    run.ended_at = datetime.now(UTC)
    session.commit()
    return plan


def find_object(session: Session, name: str) -> SourceObject:
    obj = session.scalars(select(SourceObject).where(SourceObject.object_name == name)).first()
    if obj is None:
        raise ReplayError(f"no registered source object called '{name}'")
    return obj


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse.replay", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    show = sub.add_parser("history", help="list a table's versions")
    show.add_argument("--path", type=Path, required=True)

    back = sub.add_parser("rewind", help="rewind a source's watermark to a date")
    back.add_argument("--source", required=True)
    back.add_argument("--from", dest="moment", required=True)
    back.add_argument("--apply", action="store_true", help="make the change; default is a dry run")

    rest = sub.add_parser("restore", help="restore a table to its state at a date")
    rest.add_argument("--path", type=Path, required=True)
    rest.add_argument("--to", dest="moment", required=True)
    rest.add_argument("--apply", action="store_true")

    args = parser.parse_args(argv)
    try:
        if args.command == "history":
            for v in history(args.path):
                print(f"  v{v.version:<4} {v.timestamp.isoformat()}  {v.operation}")
            return 0

        moment = parse_moment(args.moment)

        if args.command == "restore":
            target = restore(args.path, moment, dry_run=not args.apply)
            verb = "restored" if args.apply else "would restore"
            print(f"{verb} {args.path} to v{target} (state at {moment.isoformat()})")
        else:
            settings = Settings()
            with Session(create_engine(database_url(settings))) as session:
                obj = find_object(session, args.source)
                plan = (
                    rewind(session, obj, moment)
                    if args.apply
                    else plan_rewind(session, obj, moment)
                )
            verb = "rewound" if args.apply else "would rewind"
            print(f"{verb} {plan.object_name}: {plan.previous} -> {plan.new}")
            if args.apply:
                print("the next pipeline run re-reads everything since then")
    except ReplayError as exc:
        print(f"refused — {exc}", file=sys.stderr)
        return 1

    if not args.apply:
        print("dry run — nothing changed. Add --apply to do it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
