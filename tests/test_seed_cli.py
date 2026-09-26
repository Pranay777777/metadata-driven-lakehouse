"""Tests for the seed command-line interface."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse.seed.__main__ import main, parse_args


def test_defaults() -> None:
    cfg = parse_args([])
    assert cfg.rows == 1_000_000
    assert cfg.seed == 42
    assert cfg.output_dir == Path("data/generated")


def test_arguments_are_parsed() -> None:
    cfg = parse_args(
        ["--rows", "500", "--seed", "7", "--null-rate", "0.5", "--output-dir", "/tmp/x"]
    )
    assert cfg.rows == 500
    assert cfg.seed == 7
    assert cfg.null_rate == 0.5
    assert cfg.output_dir == Path("/tmp/x")


def test_invalid_rate_is_rejected_by_config() -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        parse_args(["--null-rate", "5"])


def test_main_writes_files_and_reports(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--rows", "500", "--output-dir", str(tmp_path)])
    written = sorted(p.name for p in tmp_path.glob("*.parquet"))
    assert written == [
        "customers.parquet",
        "order_items.parquet",
        "orders.parquet",
        "products.parquet",
        "sellers.parquet",
    ]
    out = capsys.readouterr().out
    assert "order_items" in out
    assert "seed    : 42" in out
