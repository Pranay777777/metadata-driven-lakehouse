"""Deterministic synthetic source data.

The point of this module is that anyone can clone the repo and run the
whole pipeline in ten minutes without an Azure subscription or a download.

Two design choices worth knowing:

**Numpy, not Faker, for the bulk.** Faker generates roughly 50k rows a
minute per field — at five million rows that is hours. Instead a small
pool of realistic values is built once with plain Python and then sampled
with numpy, which is three orders of magnitude faster and produces data
with the same distribution properties.

**Imperfections are deliberate and configurable.** Clean data proves
nothing: the pipeline's quality gates, deduplication and late-arrival
handling can only be demonstrated against data that contains nulls,
duplicates and backdated rows. Rates are configurable so tests can set
them to zero, or to one, and assert the handling works.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

EPOCH: Final = datetime(2026, 1, 1, tzinfo=UTC)

_CITIES: Final = [
    "sao paulo",
    "rio de janeiro",
    "belo horizonte",
    "brasilia",
    "curitiba",
    "porto alegre",
    "salvador",
    "fortaleza",
    "recife",
    "manaus",
]
_STATES: Final = ["SP", "RJ", "MG", "DF", "PR", "RS", "BA", "CE", "PE", "AM"]
_CATEGORIES: Final = [
    "bed_bath_table",
    "health_beauty",
    "sports_leisure",
    "furniture_decor",
    "computers_accessories",
    "housewares",
    "watches_gifts",
    "telephony",
    "garden_tools",
    "auto",
]
_ORDER_STATUS: Final = [
    "delivered",
    "shipped",
    "processing",
    "canceled",
    "invoiced",
    "approved",
]
_STATUS_WEIGHTS: Final = [0.72, 0.10, 0.07, 0.05, 0.04, 0.02]


@dataclass(frozen=True)
class SeedConfig:
    """Controls the size and messiness of the generated dataset."""

    rows: int = 1_000_000
    """Number of order_items rows. Other tables are scaled from this."""

    seed: int = 42
    """Same seed, same bytes. Reproducibility is the whole point."""

    null_rate: float = 0.03
    """Fraction of nullable fields left empty."""

    duplicate_rate: float = 0.015
    """Fraction of rows emitted twice, to exercise Silver deduplication."""

    late_rate: float = 0.02
    """Fraction of rows backdated behind the watermark, to exercise
    late-arriving data handling."""

    days: int = 540
    """Span of the generated history."""

    output_dir: Path = field(default=Path("data/generated"))

    def __post_init__(self) -> None:
        if self.rows < 1:
            raise ValueError("rows must be positive")
        for name in ("null_rate", "duplicate_rate", "late_rate"):
            value: float = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {value}")

    @property
    def n_customers(self) -> int:
        return max(1, self.rows // 10)

    @property
    def n_orders(self) -> int:
        return max(1, self.rows // 2)

    @property
    def n_products(self) -> int:
        return max(1, self.rows // 100)

    @property
    def n_sellers(self) -> int:
        return max(1, self.rows // 500)


def _ids(prefix: str, count: int) -> pa.Array:
    """Stable, readable surrogate keys: 'cust_00000001'."""
    return pa.array([f"{prefix}_{i:08d}" for i in range(count)], type=pa.string())


def _mask_nulls(
    values: list[str] | np.ndarray, rng: np.random.Generator, rate: float
) -> list[str | None]:
    """Blank out `rate` of the values, so nullability is exercised."""
    out: list[str | None] = [str(v) for v in values]
    if rate <= 0:
        return out
    holes = rng.random(len(out)) < rate
    return [None if h else v for v, h in zip(out, holes, strict=True)]


def _timestamps(
    rng: np.random.Generator, count: int, cfg: SeedConfig
) -> tuple[np.ndarray, np.ndarray]:
    """Return (event_time, updated_at) in epoch seconds.

    `updated_at` normally trails the event slightly. For `late_rate` of
    rows it is pushed *backwards* well behind the event, which is what a
    late-arriving record looks like to a watermark-based load.
    """
    span = cfg.days * 86_400
    event = rng.integers(0, span, size=count, dtype=np.int64)
    updated = event + rng.integers(0, 7_200, size=count, dtype=np.int64)

    if cfg.late_rate > 0:
        late = rng.random(count) < cfg.late_rate
        updated = np.where(late, event - rng.integers(86_400, 30 * 86_400, size=count), updated)

    base = int(EPOCH.timestamp())
    return event + base, updated + base


def _apply_duplicates(table: pa.Table, rng: np.random.Generator, rate: float) -> pa.Table:
    """Append a random sample of rows again, then shuffle.

    Real feeds redeliver. Shuffling matters too: rows arriving in
    timestamp order would hide ordering bugs that production would find.
    """
    n = table.num_rows
    extra = int(n * rate)
    indices = np.arange(n)
    if extra > 0:
        indices = np.concatenate([indices, rng.choice(n, size=extra, replace=False)])
    rng.shuffle(indices)
    return table.take(pa.array(indices))


def build_customers(cfg: SeedConfig, rng: np.random.Generator) -> pa.Table:
    """Customer dimension."""
    n = cfg.n_customers
    _, updated = _timestamps(rng, n, cfg)
    return pa.table(
        {
            "customer_id": _ids("cust", n),
            "customer_city": pa.array(
                _mask_nulls(rng.choice(_CITIES, n), rng, cfg.null_rate), type=pa.string()
            ),
            "customer_state": pa.array(rng.choice(_STATES, n).tolist(), type=pa.string()),
            "customer_zip_prefix": pa.array(rng.integers(1000, 99999, n).tolist(), type=pa.int32()),
            "updated_at": pa.array(updated.tolist(), type=pa.int64()),
        }
    )


def build_products(cfg: SeedConfig, rng: np.random.Generator) -> pa.Table:
    """Product dimension, with a nullable weight to exercise null rules."""
    n = cfg.n_products
    _, updated = _timestamps(rng, n, cfg)
    weights = rng.integers(50, 30_000, n).astype(float)
    holes = rng.random(n) < cfg.null_rate
    return pa.table(
        {
            "product_id": _ids("prod", n),
            "product_category": pa.array(rng.choice(_CATEGORIES, n).tolist(), type=pa.string()),
            "product_weight_g": pa.array(
                [None if h else float(w) for w, h in zip(weights, holes, strict=True)],
                type=pa.float64(),
            ),
            "updated_at": pa.array(updated.tolist(), type=pa.int64()),
        }
    )


def build_sellers(cfg: SeedConfig, rng: np.random.Generator) -> pa.Table:
    """Seller dimension."""
    n = cfg.n_sellers
    _, updated = _timestamps(rng, n, cfg)
    return pa.table(
        {
            "seller_id": _ids("sell", n),
            "seller_city": pa.array(rng.choice(_CITIES, n).tolist(), type=pa.string()),
            "seller_state": pa.array(rng.choice(_STATES, n).tolist(), type=pa.string()),
            "updated_at": pa.array(updated.tolist(), type=pa.int64()),
        }
    )


def build_orders(cfg: SeedConfig, rng: np.random.Generator) -> pa.Table:
    """Order header. Delivery timestamp is null for undelivered orders."""
    n = cfg.n_orders
    purchased, updated = _timestamps(rng, n, cfg)
    status = rng.choice(_ORDER_STATUS, n, p=_STATUS_WEIGHTS)
    delivered_offset = rng.integers(86_400, 20 * 86_400, n)
    delivered = [
        int(p + d) if s == "delivered" else None
        for p, d, s in zip(purchased, delivered_offset, status, strict=True)
    ]
    return pa.table(
        {
            "order_id": _ids("ordr", n),
            "customer_id": pa.array(
                [f"cust_{i:08d}" for i in rng.integers(0, cfg.n_customers, n)], type=pa.string()
            ),
            "order_status": pa.array(status.tolist(), type=pa.string()),
            "purchased_at": pa.array(purchased.tolist(), type=pa.int64()),
            "delivered_at": pa.array(delivered, type=pa.int64()),
            "updated_at": pa.array(updated.tolist(), type=pa.int64()),
        }
    )


def build_order_items(cfg: SeedConfig, rng: np.random.Generator) -> pa.Table:
    """The fact table — the only one generated at full `rows` scale."""
    n = cfg.rows
    _, updated = _timestamps(rng, n, cfg)
    price = np.round(rng.gamma(2.0, 45.0, n) + 5.0, 2)
    freight = np.round(rng.gamma(1.5, 8.0, n) + 1.0, 2)
    return pa.table(
        {
            "order_item_id": _ids("item", n),
            "order_id": pa.array(
                [f"ordr_{i:08d}" for i in rng.integers(0, cfg.n_orders, n)], type=pa.string()
            ),
            "product_id": pa.array(
                [f"prod_{i:08d}" for i in rng.integers(0, cfg.n_products, n)], type=pa.string()
            ),
            "seller_id": pa.array(
                [f"sell_{i:08d}" for i in rng.integers(0, cfg.n_sellers, n)], type=pa.string()
            ),
            "price": pa.array(price.tolist(), type=pa.float64()),
            "freight_value": pa.array(freight.tolist(), type=pa.float64()),
            "updated_at": pa.array(updated.tolist(), type=pa.int64()),
        }
    )


BUILDERS: Final = {
    "customers": build_customers,
    "products": build_products,
    "sellers": build_sellers,
    "orders": build_orders,
    "order_items": build_order_items,
}


def generate(cfg: SeedConfig) -> dict[str, pa.Table]:
    """Build every table. Duplicates are applied to facts only."""
    rng = np.random.default_rng(cfg.seed)
    tables: dict[str, pa.Table] = {}
    for name, builder in BUILDERS.items():
        table = builder(cfg, rng)
        if name in {"orders", "order_items"}:
            table = _apply_duplicates(table, rng, cfg.duplicate_rate)
        tables[name] = table
    return tables


def write(tables: dict[str, pa.Table], out_dir: Path) -> dict[str, Path]:
    """Write each table to Parquet and return the paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for name, table in tables.items():
        path = out_dir / f"{name}.parquet"
        pq.write_table(table, path, compression="snappy")
        written[name] = path
    return written


def fingerprint(tables: dict[str, pa.Table]) -> str:
    """Stable hash of the dataset, used to assert determinism in tests."""
    digest = hashlib.sha256()
    for name in sorted(tables):
        digest.update(name.encode())
        digest.update(str(tables[name].num_rows).encode())
        digest.update(tables[name].schema.serialize().to_pybytes())
    return digest.hexdigest()[:16]


def summarise(tables: dict[str, pa.Table]) -> str:
    """Human-readable report, printed after a seed run."""
    lines = [f"{'table':<14}{'rows':>12}{'columns':>10}"]
    lines.append("-" * 36)
    for name, table in tables.items():
        lines.append(f"{name:<14}{table.num_rows:>12,}{table.num_columns:>10}")
    total = sum(t.num_rows for t in tables.values())
    lines.append("-" * 36)
    lines.append(f"{'total':<14}{total:>12,}")
    return "\n".join(lines)


def horizon(cfg: SeedConfig) -> tuple[datetime, datetime]:
    """First and last event date in the generated history."""
    return EPOCH, EPOCH + timedelta(days=cfg.days)
