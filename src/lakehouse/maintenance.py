"""Table maintenance: compaction, Z-ordering and vacuum.

Every incremental load appends a few files. Nothing ever merges them, so
a table that has loaded daily for a year is thousands of small Parquet
files, and every read pays to open each one. This is the most common
way a Delta lake gets slow without anything visibly breaking.

Three operations, each doing one job:

**Compaction** rewrites many small files into fewer large ones. It
changes no data, only layout, so it is always safe to run.

**Z-ordering** co-locates rows with similar values in the chosen columns
within the same files. Each file's min/max statistics then cover a
narrow range, so a filter on those columns can skip whole files without
opening them. It helps only for columns you actually filter on, which is
why it is opt-in per column rather than automatic.

**Vacuum** deletes files no longer referenced by the current table
version. It is the only destructive operation here and is treated that
way: dry run by default, a retention floor that cannot be undercut, and
a hard refusal to touch quarantine tables.

Partitioning is deliberately not here. See ADR-015 for why a table this
size is faster without it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

from lakehouse.quality.quarantine import QUARANTINE

MIN_RETENTION_HOURS = 168
"""One week. Vacuuming below this breaks time travel inside the window
someone is most likely to need it — including the replay CLI."""

DEFAULT_TARGET_SIZE = 128 * 1024 * 1024
"""128 MB. The usual sweet spot: large enough that per-file overhead is
negligible, small enough that one file is not a straggler task."""

SMALL_FILE_BYTES = 16 * 1024 * 1024
"""Files under this size count as 'small' in the stats report."""


class MaintenanceError(Exception):
    """Raised when a maintenance operation is refused as unsafe."""


@dataclass(frozen=True)
class FileStats:
    """The layout of a table's current version."""

    files: int
    total_bytes: int
    small_files: int

    @property
    def average_bytes(self) -> float:
        return self.total_bytes / self.files if self.files else 0.0

    def describe(self) -> str:
        mb = self.total_bytes / (1024 * 1024)
        avg = self.average_bytes / (1024 * 1024)
        return f"{self.files} files, {mb:.1f} MB total, {avg:.2f} MB avg, {self.small_files} small"


@dataclass(frozen=True)
class MaintenanceResult:
    """What one operation did to one table."""

    path: str
    operation: str
    before: FileStats
    after: FileStats
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def files_removed(self) -> int:
        return self.before.files - self.after.files


def file_stats(path: Path) -> FileStats:
    """Count and size the files in the table's current version."""
    # delta-rs 1.x returns an arro3 table rather than a pyarrow one;
    # pyarrow accepts it through the Arrow C stream interface.
    actions = pa.table(DeltaTable(str(path)).get_add_actions(flatten=True))
    sizes = [int(s) for s in actions.column("size_bytes").to_pylist()]
    return FileStats(
        files=len(sizes),
        total_bytes=sum(sizes),
        small_files=sum(1 for s in sizes if s < SMALL_FILE_BYTES),
    )


def is_quarantine(path: Path) -> bool:
    """Whether a path is part of the quarantine store."""
    return QUARANTINE in path.parts


def compact(path: Path, target_size: int = DEFAULT_TARGET_SIZE) -> MaintenanceResult:
    """Merge small files into larger ones. Changes layout, never data."""
    before = file_stats(path)
    metrics = DeltaTable(str(path)).optimize.compact(target_size=target_size)
    return MaintenanceResult(str(path), "compact", before, file_stats(path), metrics)


def zorder(
    path: Path, columns: list[str], target_size: int = DEFAULT_TARGET_SIZE
) -> MaintenanceResult:
    """Cluster rows by `columns` so filters on them can skip files.

    Raises:
        MaintenanceError: if a requested column is not in the table. A
            misspelt column would otherwise rewrite every file for no
            benefit, which is an expensive way to do nothing.
    """
    table = DeltaTable(str(path))
    available = set(table.schema().to_arrow().names)
    missing = [c for c in columns if c not in available]
    if missing:
        raise MaintenanceError(f"cannot Z-order {path} by {', '.join(missing)}: not in the table")
    before = file_stats(path)
    metrics = table.optimize.z_order(columns, target_size=target_size)
    return MaintenanceResult(str(path), "zorder", before, file_stats(path), metrics)


def vacuum(
    path: Path, retention_hours: int = MIN_RETENTION_HOURS, dry_run: bool = True
) -> list[str]:
    """Delete files the current version no longer references.

    Returns the files that were — or, in a dry run, would be — removed.

    Raises:
        MaintenanceError: for a quarantine table, whose history is the
            evidence it exists to keep (ADR-010), or for a retention
            below the one-week floor, which would break time travel.
    """
    if is_quarantine(path):
        raise MaintenanceError(
            f"refusing to vacuum {path}: quarantine tables are append-only evidence"
        )
    if retention_hours < MIN_RETENTION_HOURS:
        raise MaintenanceError(
            f"retention {retention_hours}h is below the {MIN_RETENTION_HOURS}h floor; "
            "vacuuming that aggressively breaks time travel and the replay CLI"
        )
    return list(
        DeltaTable(str(path)).vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=True,
        )
    )


def delta_tables(root: Path) -> list[Path]:
    """Every Delta table under a directory, sorted, quarantine included."""
    return sorted(p.parent for p in root.rglob("_delta_log") if p.is_dir())


def main(argv: list[str] | None = None) -> int:
    """Maintain every Delta table under a layer.

    python -m lakehouse.maintenance --layer silver --compact
    python -m lakehouse.maintenance --layer gold --zorder-by customers_key
    python -m lakehouse.maintenance --vacuum            # dry run
    python -m lakehouse.maintenance --vacuum --apply    # actually delete
    """
    import argparse

    from lakehouse.config import Settings

    parser = argparse.ArgumentParser(prog="lakehouse.maintenance", description=main.__doc__)
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    parser.add_argument("--compact", action="store_true")
    parser.add_argument(
        "--zorder-by",
        nargs="+",
        default=None,
        help="Z-order tables that contain every one of these columns",
    )
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--apply", action="store_true", help="vacuum for real, not a dry run")
    parser.add_argument("--lake-root", type=Path, default=None)
    args = parser.parse_args(argv)

    if not (args.compact or args.zorder_by or args.vacuum):
        parser.error("choose at least one of --compact, --zorder-by, --vacuum")

    root = args.lake_root or Path(Settings().lake_root)
    base = root if args.layer == "all" else root / args.layer
    if not base.exists():
        print(f"nothing at {base} — run the pipeline first")
        return 2

    tables = [t for t in delta_tables(base) if not is_quarantine(t)]
    print(f"{len(tables)} table(s) under {base}\n")

    for table in tables:
        name = table.relative_to(root)
        if args.compact:
            result = compact(table)
            print(f"compact  {name}: {result.before.files} -> {result.after.files} files")
        if args.zorder_by:
            columns = set(DeltaTable(str(table)).schema().to_arrow().names)
            if set(args.zorder_by) <= columns:
                result = zorder(table, list(args.zorder_by))
                print(f"zorder   {name}: by {', '.join(args.zorder_by)}")
            else:
                print(f"zorder   {name}: skipped, lacks {', '.join(args.zorder_by)}")
        if args.vacuum:
            removed = vacuum(table, dry_run=not args.apply)
            verb = "removed" if args.apply else "would remove"
            print(f"vacuum   {name}: {verb} {len(removed)} file(s)")

    if args.vacuum and not args.apply:
        print("\ndry run — nothing was deleted. Add --apply to vacuum for real.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
