"""Measure what compaction and Z-ordering actually buy.

    python scripts/benchmark_maintenance.py --batches 200

Builds a table the way incremental loading does — many small appends —
then times a selective read, compacts, times it again, Z-orders, and
times it a third time. The numbers in ADR-015 come from this script.

Each read is repeated and the median taken, because a single timing of
a sub-second read is mostly noise.

The compaction target is kept small on purpose. Compacted into a single
file, a Z-order has nothing to skip *between*, and any gain it shows is
row-group ordering inside that one file — real, but not what Z-ordering
is for. Several files is the honest test.
"""

from __future__ import annotations

import argparse
import platform
import statistics
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

from lakehouse.maintenance import compact, file_stats, zorder

REPEATS = 7


def fragmented_table(path: Path, batches: int, rows_per_batch: int) -> None:
    """Append many small batches, as a year of incremental loads would.

    Keys are shuffled across batches so every file spans the full key
    range — the realistic worst case, and the one Z-ordering fixes.
    """
    import random

    rng = random.Random(42)  # noqa: S311 — seeded test data, not cryptography
    for batch in range(batches):
        keys = [rng.randrange(1_000_000) for _ in range(rows_per_batch)]
        write_deltalake(
            str(path),
            pa.table(
                {
                    "customer_id": pa.array(keys, type=pa.int64()),
                    "amount": pa.array([rng.random() * 100 for _ in keys]),
                    "batch": pa.array([batch] * rows_per_batch, type=pa.int32()),
                }
            ),
            mode="append",
        )


def selective_read(path: Path) -> float:
    """Median seconds to read a narrow key range."""
    timings = []
    for _ in range(REPEATS):
        started = time.perf_counter()
        dataset = DeltaTable(str(path)).to_pyarrow_dataset()
        dataset.to_table(
            filter=(pc.field("customer_id") >= 500_000) & (pc.field("customer_id") < 501_000)
        )
        timings.append(time.perf_counter() - started)
    return statistics.median(timings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batches", type=int, default=200)
    parser.add_argument("--rows-per-batch", type=int, default=5_000)
    parser.add_argument(
        "--target-mb",
        type=int,
        default=2,
        help="compaction target; small enough to leave several files so "
        "Z-ordering has files to skip between",
    )
    args = parser.parse_args(argv)

    print(f"{platform.system()} {platform.machine()}, Python {platform.python_version()}")
    total = args.batches * args.rows_per_batch
    print(f"{args.batches} appends x {args.rows_per_batch:,} rows = {total:,} rows\n")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "fragmented"
        fragmented_table(path, args.batches, args.rows_per_batch)

        rows = []
        rows.append(("fragmented", file_stats(path), selective_read(path)))
        target = args.target_mb * 1024 * 1024
        compact(path, target_size=target)
        rows.append(("compacted", file_stats(path), selective_read(path)))
        zorder(path, ["customer_id"], target_size=target)
        rows.append(("z-ordered", file_stats(path), selective_read(path)))

    print(f"{'state':<12}{'files':>7}{'avg MB':>9}{'read ms':>10}{'vs start':>10}")
    print("-" * 48)
    baseline = rows[0][2]
    for state, stats, seconds in rows:
        speedup = baseline / seconds if seconds else 0.0
        print(
            f"{state:<12}{stats.files:>7}{stats.average_bytes / 1024 / 1024:>9.2f}"
            f"{seconds * 1000:>10.1f}{speedup:>9.1f}x"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
