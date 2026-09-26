"""Tests for the synthetic data generator.

The generator's contract is: same seed, same data; and the imperfection
rates it promises are actually present. Both matter, because the quality
gates built in later steps are tested against this output.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from lakehouse.seed.generator import (
    SeedConfig,
    fingerprint,
    generate,
    horizon,
    summarise,
    write,
)

SMALL = SeedConfig(rows=5_000, output_dir=Path("unused"))


def test_same_seed_produces_identical_data() -> None:
    """Reproducibility is the whole point of a synthetic dataset."""
    a = generate(SMALL)
    b = generate(SMALL)
    assert fingerprint(a) == fingerprint(b)
    for name in a:
        assert a[name].equals(b[name])


def test_different_seed_produces_different_data() -> None:
    a = generate(SMALL)
    b = generate(SeedConfig(rows=5_000, seed=99, output_dir=Path("unused")))
    assert not a["order_items"].equals(b["order_items"])


def test_all_tables_are_generated() -> None:
    tables = generate(SMALL)
    assert set(tables) == {"customers", "products", "sellers", "orders", "order_items"}
    for table in tables.values():
        assert table.num_rows > 0


def test_table_sizes_scale_from_rows() -> None:
    tables = generate(SMALL)
    assert tables["customers"].num_rows == SMALL.n_customers
    assert tables["products"].num_rows == SMALL.n_products
    # Facts carry duplicates, so they exceed the base count.
    assert tables["order_items"].num_rows > SMALL.rows


def test_duplicates_are_injected() -> None:
    cfg = SeedConfig(rows=5_000, duplicate_rate=0.10, output_dir=Path("unused"))
    items = generate(cfg)["order_items"]
    unique = len(set(items.column("order_item_id").to_pylist()))
    assert items.num_rows > unique
    assert items.num_rows == pytest.approx(unique * 1.10, rel=0.05)


def test_duplicate_rate_zero_gives_unique_rows() -> None:
    cfg = SeedConfig(rows=2_000, duplicate_rate=0.0, output_dir=Path("unused"))
    items = generate(cfg)["order_items"]
    assert items.num_rows == len(set(items.column("order_item_id").to_pylist()))


def test_nulls_appear_at_roughly_the_configured_rate() -> None:
    cfg = SeedConfig(rows=20_000, null_rate=0.20, output_dir=Path("unused"))
    city = generate(cfg)["customers"].column("customer_city")
    assert city.null_count / len(city) == pytest.approx(0.20, abs=0.03)


def test_null_rate_zero_gives_no_nulls() -> None:
    cfg = SeedConfig(rows=2_000, null_rate=0.0, output_dir=Path("unused"))
    assert generate(cfg)["customers"].column("customer_city").null_count == 0


def test_late_arriving_rows_are_backdated() -> None:
    """Rows whose updated_at precedes their event are what break naive
    watermark loads — the pipeline must handle them, so they must exist."""
    cfg = SeedConfig(rows=20_000, late_rate=0.15, output_dir=Path("unused"))
    orders = generate(cfg)["orders"]
    purchased = orders.column("purchased_at").to_pylist()
    updated = orders.column("updated_at").to_pylist()
    late = sum(1 for p, u in zip(purchased, updated, strict=True) if u < p)
    assert late > 0
    assert late / len(purchased) == pytest.approx(0.15, abs=0.04)


def test_undelivered_orders_have_null_delivery() -> None:
    orders = generate(SMALL)["orders"]
    status = orders.column("order_status").to_pylist()
    delivered = orders.column("delivered_at").to_pylist()
    for s, d in zip(status, delivered, strict=True):
        assert (d is not None) == (s == "delivered")


def test_foreign_keys_point_at_real_rows() -> None:
    tables = generate(SMALL)
    customers = set(tables["customers"].column("customer_id").to_pylist())
    order_customers = set(tables["orders"].column("customer_id").to_pylist())
    assert order_customers <= customers


def test_write_creates_one_parquet_per_table(tmp_path: Path) -> None:
    tables = generate(SMALL)
    paths = write(tables, tmp_path)
    assert len(paths) == len(tables)
    for name, path in paths.items():
        assert path.exists()
        assert pq.read_table(path).num_rows == tables[name].num_rows


def test_rejects_invalid_rates() -> None:
    for bad in ({"null_rate": 1.5}, {"duplicate_rate": -0.1}, {"late_rate": 2.0}):
        with pytest.raises(ValueError, match="between 0 and 1"):
            SeedConfig(**bad)  # type: ignore[arg-type]


def test_rejects_zero_rows() -> None:
    with pytest.raises(ValueError, match="rows must be positive"):
        SeedConfig(rows=0)


def test_summarise_lists_every_table() -> None:
    text = summarise(generate(SMALL))
    for name in ("customers", "products", "sellers", "orders", "order_items"):
        assert name in text


def test_horizon_spans_configured_days() -> None:
    start, end = horizon(SeedConfig(days=100))
    assert (end - start).days == 100
