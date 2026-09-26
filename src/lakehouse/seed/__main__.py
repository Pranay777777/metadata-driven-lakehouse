"""CLI: python -m lakehouse.seed --rows 5000000"""

from __future__ import annotations

import argparse
from pathlib import Path

from lakehouse.seed.generator import SeedConfig, generate, horizon, summarise, write


def parse_args(argv: list[str] | None = None) -> SeedConfig:
    """Turn command-line arguments into a SeedConfig."""
    p = argparse.ArgumentParser(
        prog="lakehouse.seed",
        description="Generate deterministic synthetic source data.",
    )
    p.add_argument("--rows", type=int, default=1_000_000, help="order_items rows")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--null-rate", type=float, default=0.03)
    p.add_argument("--duplicate-rate", type=float, default=0.015)
    p.add_argument("--late-rate", type=float, default=0.02)
    p.add_argument("--days", type=int, default=540)
    p.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    a = p.parse_args(argv)
    return SeedConfig(
        rows=a.rows,
        seed=a.seed,
        null_rate=a.null_rate,
        duplicate_rate=a.duplicate_rate,
        late_rate=a.late_rate,
        days=a.days,
        output_dir=a.output_dir,
    )


def main(argv: list[str] | None = None) -> None:
    """Generate and write the dataset."""
    cfg = parse_args(argv)
    tables = generate(cfg)
    paths = write(tables, cfg.output_dir)
    start, end = horizon(cfg)
    print(summarise(tables))
    print(f"\nhistory : {start:%Y-%m-%d} to {end:%Y-%m-%d}")
    print(f"seed    : {cfg.seed} (rerun with the same seed for identical output)")
    print(f"written : {len(paths)} files to {cfg.output_dir}")


if __name__ == "__main__":
    main()
