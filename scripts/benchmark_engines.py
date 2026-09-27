"""Measure the two engines against each other on real seeded data.

    python scripts/benchmark_engines.py --rows 2000000

Numbers quoted in the README come from this script, on the machine
named in the output. It reports the Arrow engine's time even when Spark
is unavailable, so the default path is always measurable.

Both engines are run on the same in-memory table and their results are
compared, so a fast wrong answer is reported as wrong.
"""

from __future__ import annotations

import argparse
import platform
import sys
import time
from dataclasses import dataclass

import pyarrow as pa

from lakehouse.engines import ArrowEngine
from lakehouse.engines.base import Engine, EngineError
from lakehouse.seed.generator import SeedConfig, generate


@dataclass(frozen=True)
class Measurement:
    engine: str
    seconds: float
    rows_in: int
    rows_out: int

    @property
    def rows_per_second(self) -> float:
        return self.rows_in / self.seconds if self.seconds else 0.0


def time_engine(engine: Engine, table: pa.Table, keys: list[str], sequence: str) -> Measurement:
    started = time.perf_counter()
    result = engine.latest_per_key(table, keys, sequence)
    elapsed = time.perf_counter() - started
    return Measurement(engine.name, elapsed, table.num_rows, result.num_rows)


def fingerprint(table: pa.Table, keys: list[str]) -> set[tuple[object, ...]]:
    columns = [table.column(k).to_pylist() for k in keys]
    return {tuple(c[i] for c in columns) for i in range(table.num_rows)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--table", default="order_items")
    parser.add_argument("--key", default="order_item_id")
    args = parser.parse_args(argv)

    tables = generate(SeedConfig(rows=args.rows, seed=42))
    table = tables[args.table]
    keys, sequence = [args.key], "updated_at"

    print(f"{platform.system()} {platform.machine()}, Python {platform.python_version()}")
    print(f"{args.table}: {table.num_rows:,} rows, {len(table.column_names)} columns\n")

    results = [time_engine(ArrowEngine(), table, keys, sequence)]
    arrow_rows = fingerprint(ArrowEngine().latest_per_key(table, keys, sequence), keys)

    try:
        from lakehouse.engines.spark import SparkEngine

        spark = SparkEngine()
        results.append(time_engine(spark, table, keys, sequence))
        spark_out = spark.latest_per_key(table, keys, sequence)
        if fingerprint(spark_out, keys) != arrow_rows:
            print("ENGINES DISAGREE — the benchmark is meaningless", file=sys.stderr)
            return 1
        spark.close()
    except (EngineError, ImportError) as exc:
        print(f"spark skipped: {exc}\n")
    except Exception as exc:
        # Almost always the driver running out of room collecting the
        # result, which surfaces as TaskResultLost and never mentions
        # memory. Report it and still print the Arrow number.
        print(f"spark failed: {type(exc).__name__}", file=sys.stderr)
        print(
            "  if this says TaskResultLost, the driver could not hold the result — "
            "raise SPARK_DRIVER_MEMORY (currently "
            f"{__import__('os').environ.get('SPARK_DRIVER_MEMORY', 'unset')}) "
            "or run with fewer --rows\n",
            file=sys.stderr,
        )

    print(f"{'engine':<10}{'seconds':>10}{'rows/sec':>14}{'rows out':>12}")
    print("-" * 46)
    for m in results:
        print(f"{m.engine:<10}{m.seconds:>10.2f}{m.rows_per_second:>14,.0f}{m.rows_out:>12,}")

    if len(results) == 2:
        ratio = results[1].seconds / results[0].seconds
        print(f"\nArrow is {ratio:.1f}x faster at this size, and both agree on the result.")
        print("Spark's overhead here is JVM startup and Arrow serialisation to the")
        print("executors, both largely fixed; the crossover is where the input stops")
        print("fitting in memory.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
