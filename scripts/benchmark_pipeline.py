"""Time the whole pipeline — seed, Bronze, Silver, Gold — end to end.

    python scripts/benchmark_pipeline.py --rows 1000000 --runs 3

Each run starts from nothing in a temporary directory, with a SQLite
control plane so the stack is not needed. Per-layer throughput comes from
the audit trail itself (`task_run.duration_seconds`), so the benchmark
measures what the platform records about itself. The README quotes the
median of the runs, on the machine this prints.
"""

from __future__ import annotations

import argparse
import os
import platform
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

LAYERS = ("bronze", "silver", "gold")


def one_run(rows: int) -> tuple[float, dict[str, float], dict[str, float]]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        env = dict(
            os.environ,
            DATABASE_URL=f"sqlite:///{root / 'control.db'}",
            LAKE_ROOT=str(root / "lake"),
            MASKING_KEY="benchmark",
            LOG_LEVEL="WARNING",
        )
        data = str(root / "data")

        def run(*args: str) -> None:
            # Fixed internal module names, never user input.
            subprocess.run(  # noqa: S603
                [sys.executable, "-m", *args], env=env, check=True, capture_output=True
            )

        run("lakehouse.seed", "--rows", str(rows), "--output-dir", data)
        run("lakehouse.pipeline", "--register", "--data-dir", data)
        started = time.perf_counter()
        run("lakehouse.pipeline", "--data-dir", data)
        wall = time.perf_counter() - started

        with sqlite3.connect(root / "control.db") as conn:
            rows_per_sec = {}
            for layer, written, seconds in conn.execute(
                "SELECT layer, SUM(rows_written), SUM(duration_seconds) "
                "FROM task_run GROUP BY layer"
            ):
                rows_per_sec[layer] = written / seconds if seconds else 0.0
        sizes = {
            layer: sum(f.stat().st_size for f in (root / "lake" / layer).rglob("*") if f.is_file())
            / 1_048_576
            for layer in LAYERS
        }
        return wall, rows_per_sec, sizes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000, help="order_items rows to seed")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    print(
        f"{platform.system()} {platform.machine()}, Python {platform.python_version()}, "
        f"{os.cpu_count()} CPU(s)"
    )
    results = [one_run(args.rows) for _ in range(args.runs)]
    walls = [r[0] for r in results]
    print(f"\n{args.rows:,} order_items rows, {args.runs} run(s), medians:\n")
    print(
        f"  pipeline wall time   {statistics.median(walls):8.2f} s   (runs: "
        + ", ".join(f"{w:.2f}" for w in walls)
        + ")"
    )
    for layer in LAYERS:
        rate = statistics.median(r[1].get(layer, 0.0) for r in results)
        size = statistics.median(r[2][layer] for r in results)
        print(f"  {layer:<8} {rate:>12,.0f} rows/s   {size:7.1f} MB on disk")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
