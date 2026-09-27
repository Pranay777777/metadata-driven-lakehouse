"""The quarantine table.

Step 28 diverts rows that breach a quarantine-severity rule so the rest
of the batch can load. Diverting them is only half an answer: a row that
is removed and not written anywhere has been deleted, and the pipeline
reports success while losing data. That is worse than failing, because
it is quiet.

Quarantined rows land in their own Delta table alongside the layer they
were rejected from, each carrying the reason it was rejected and the run
that rejected it. Three things then become possible that are impossible
with a dropped row: counting what is being lost, showing a data owner
the actual offending rows, and replaying them once the source is fixed.

The table is append-only and never rewritten. A quarantine table that
gets cleaned up automatically is a log nobody can trust.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from lakehouse.metadata.enums import Layer
from lakehouse.metadata.models import SourceObject

QUARANTINE = "quarantine"

QUARANTINED_AT = "_quarantined_at"
RUN_ID = "_run_id"
TASK_RUN_ID = "_task_run_id"
REJECTED_FROM = "_rejected_from"


@dataclass(frozen=True)
class QuarantineResult:
    """What one quarantine write recorded."""

    object_name: str
    rows_written: int
    delta_version: int
    path: str


def quarantine_path(obj: SourceObject) -> str:
    """Mirror the object's path under the quarantine prefix.

    Bronze paths start with `bronze/`; the quarantine table sits at the
    same relative location, so a rejected row is easy to find from the
    table it failed to enter.
    """
    parts = obj.target_path.split("/")
    if parts[0] in (Layer.BRONZE, Layer.SILVER, Layer.GOLD):
        parts[0] = QUARANTINE
        return "/".join(parts)
    return f"{QUARANTINE}/{obj.target_path}"


def stamp(
    rejected: pa.Table, run_id: str, task_run_id: int, layer: str, at: datetime | None = None
) -> pa.Table:
    """Add the provenance that makes a quarantined row actionable.

    Without the run and the layer, a row in here is an orphan: you can
    see it is wrong and not what produced it or when.
    """
    n = rejected.num_rows
    moment = at or datetime.now(UTC)
    return (
        rejected.append_column(
            QUARANTINED_AT, pa.array([moment] * n, type=pa.timestamp("us", tz="UTC"))
        )
        .append_column(RUN_ID, pa.array([run_id] * n, type=pa.string()))
        .append_column(TASK_RUN_ID, pa.array([task_run_id] * n, type=pa.int64()))
        .append_column(REJECTED_FROM, pa.array([layer] * n, type=pa.string()))
    )


def write_quarantine(
    lake_root: Path,
    obj: SourceObject,
    rejected: pa.Table,
    run_id: str,
    task_run_id: int,
    layer: str = Layer.SILVER,
    at: datetime | None = None,
) -> QuarantineResult | None:
    """Append rejected rows to the object's quarantine table.

    Returns None when there is nothing to quarantine, so a clean run
    does not create an empty table and clutter the lake.

    Schema is merged rather than overwritten: a source that gains a
    column under step 26's additive policy will produce quarantined rows
    of a new shape, and losing the older ones to make room would defeat
    the purpose.
    """
    if rejected.num_rows == 0:
        return None

    stamped = stamp(rejected, run_id, task_run_id, layer, at)
    target = lake_root / quarantine_path(obj)
    target.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(target), stamped, mode="append", schema_mode="merge")

    return QuarantineResult(
        object_name=obj.object_name,
        rows_written=stamped.num_rows,
        delta_version=DeltaTable(str(target)).version(),
        path=quarantine_path(obj),
    )


def read_quarantine(lake_root: Path, obj: SourceObject) -> pa.Table:
    """Read an object's quarantine table."""
    return DeltaTable(str(lake_root / quarantine_path(obj))).to_pyarrow_table()
