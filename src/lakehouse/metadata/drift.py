"""Schema drift detection and evolution policy.

A source changes shape and nobody tells you. A column is added, renamed,
retyped or dropped upstream, and the pipeline either absorbs it silently
or fails somewhere far from the cause. This is the most common way a
working pipeline starts producing wrong answers, and it almost never
announces itself.

The policy here separates two cases that deserve opposite treatment:

**Additive drift evolves automatically.** A new nullable column breaks
nothing — old rows simply lack it, which is what nullable means. Failing
the load would mean a pipeline that stops every time an upstream team
ships a feature, and a team that learns to ignore the alerts.

**Breaking drift stops the load.** A removed column or a changed type
invalidates assumptions downstream has already baked in. Loading anyway
produces a table that looks fine and joins wrong. The load fails, loudly,
with both schemas named.

Every observed schema is recorded in `schema_version`, so drift is one
hash comparison and the history of a source's shape is queryable rather
than reconstructed from a git log.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum

import pyarrow as pa
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from lakehouse.metadata.models import SchemaVersion, SourceObject


class DriftKind(StrEnum):
    """What changed between the recorded schema and the arriving one."""

    NONE = "none"
    ADDITIVE = "additive"
    """Only new columns. Safe to evolve."""

    BREAKING = "breaking"
    """Columns removed or retyped. Downstream assumptions are invalidated."""


class SchemaDriftError(Exception):
    """Raised when a source changes shape in a way that cannot be absorbed."""


@dataclass(frozen=True)
class Drift:
    """The difference between two schemas."""

    kind: str
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    retyped: list[tuple[str, str, str]] = field(default_factory=list)
    """(column, recorded type, arriving type)."""

    version: int = 0
    """Version now current for the object."""

    def describe(self) -> str:
        parts = []
        if self.added:
            parts.append(f"added {', '.join(self.added)}")
        if self.removed:
            parts.append(f"removed {', '.join(self.removed)}")
        if self.retyped:
            parts.append(
                "retyped " + ", ".join(f"{c} ({was} -> {now})" for c, was, now in self.retyped)
            )
        return "; ".join(parts) or "no change"


def describe_schema(table: pa.Table) -> list[dict[str, object]]:
    """The parts of an Arrow schema worth tracking, in column order.

    Nullability is included because a column becoming non-nullable is a
    real change; Arrow metadata and field ordering are not, because they
    churn without meaning anything.
    """
    return [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in table.schema]


def schema_hash(schema: list[dict[str, object]]) -> str:
    """Hash of a schema description, so drift is one string comparison."""
    canonical = json.dumps(sorted(schema, key=lambda c: str(c["name"])), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compare(recorded: list[dict[str, object]], arriving: list[dict[str, object]]) -> Drift:
    """Classify the difference between a recorded and an arriving schema.

    A column that both changed type and stayed present counts as
    breaking even when other columns were merely added — the worst
    finding decides the outcome.
    """
    was = {str(c["name"]): c for c in recorded}
    now = {str(c["name"]): c for c in arriving}

    added = sorted(set(now) - set(was))
    removed = sorted(set(was) - set(now))
    retyped = sorted(
        (name, str(was[name]["type"]), str(now[name]["type"]))
        for name in set(was) & set(now)
        if was[name]["type"] != now[name]["type"]
    )

    if removed or retyped:
        kind = DriftKind.BREAKING
    elif added:
        kind = DriftKind.ADDITIVE
    else:
        kind = DriftKind.NONE
    return Drift(kind=kind, added=added, removed=removed, retyped=retyped)


def current_version(session: Session, obj: SourceObject) -> SchemaVersion | None:
    """The schema version currently recorded for an object."""
    return session.scalars(
        select(SchemaVersion)
        .where(SchemaVersion.source_object_id == obj.id)
        .where(SchemaVersion.is_current)
    ).one_or_none()


def _record(
    session: Session, obj: SourceObject, schema: list[dict[str, object]], version: int
) -> SchemaVersion:
    session.execute(
        update(SchemaVersion)
        .where(SchemaVersion.source_object_id == obj.id)
        .values(is_current=False)
    )
    recorded = SchemaVersion(
        source_object_id=obj.id,
        version=version,
        schema_json=json.dumps(schema),
        schema_hash=schema_hash(schema),
        is_current=True,
    )
    session.add(recorded)
    session.commit()
    return recorded


def check_drift(session: Session, obj: SourceObject, table: pa.Table) -> Drift:
    """Compare an arriving batch against the recorded schema and act.

    The first sighting of an object records its schema as version 1 and
    reports no drift — there is nothing to compare against, and treating
    an unknown source as broken would make onboarding impossible.

    Raises:
        SchemaDriftError: on a removed or retyped column. Nothing is
            recorded in that case, so the next run re-reports the same
            drift rather than quietly accepting it.
    """
    arriving = describe_schema(table)
    existing = current_version(session, obj)

    if existing is None:
        _record(session, obj, arriving, version=1)
        return Drift(kind=DriftKind.NONE, version=1)

    if existing.schema_hash == schema_hash(arriving):
        return Drift(kind=DriftKind.NONE, version=existing.version)

    recorded: list[dict[str, object]] = json.loads(existing.schema_json)
    drift = compare(recorded, arriving)

    if drift.kind == DriftKind.BREAKING:
        raise SchemaDriftError(
            f"breaking schema change on '{obj.object_name}': {drift.describe()}. "
            "The recorded schema is unchanged; fix the source or record a new "
            "version deliberately before loading."
        )

    version = existing.version + 1
    _record(session, obj, arriving, version=version)
    return Drift(
        kind=drift.kind,
        added=drift.added,
        removed=drift.removed,
        retyped=drift.retyped,
        version=version,
    )
