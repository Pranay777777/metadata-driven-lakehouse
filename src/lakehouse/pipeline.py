"""Run the whole platform end to end.

Everything up to now has been libraries with tests. This is the thing a
person actually runs, and the thing the README's quickstart points at:

    python -m lakehouse.seed --rows 1000000
    python -m lakehouse.pipeline --register
    python -m lakehouse.pipeline

It exists because without it there is no way to see the platform work.
The loaders were only ever driven by tests that built their own
`SourceObject` rows by hand, so a clean checkout had a control plane
with nothing in it and no command to fill it.

`--register` writes the catalog below into the control plane. It is
idempotent: run it as often as you like, it updates rather than
duplicates. The catalog describes the synthetic Olist-shaped dataset
the seed generator produces, and it is the only place in this codebase
that knows what `orders` is — which is the point. Every layer below
reads it from the control plane.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.credentials import database_url
from lakehouse.ingest.bronze import ParquetSource
from lakehouse.ingest.runner import run_pipeline
from lakehouse.logging import configure_logging
from lakehouse.metadata.enums import GoldRole, LoadStrategy, SourceKind
from lakehouse.metadata.models import (
    Base,
    GoldReference,
    SourceObject,
    SourceSystem,
)
from lakehouse.transform.gold import build_gold
from lakehouse.transform.silver import build_silver

SYSTEM_NAME = "seed"


@dataclass(frozen=True)
class CatalogEntry:
    """One object in the demo catalog."""

    name: str
    primary_key: str
    strategy: str = LoadStrategy.FULL
    incremental_column: str = "updated_at"
    scd2: bool = False
    gold_role: str | None = None
    load_order: int = 100
    references: dict[str, str] = field(default_factory=dict)
    """Fact column -> dimension object name."""


CATALOG: list[CatalogEntry] = [
    # Dimensions load first so the facts that reference them resolve.
    CatalogEntry(
        "customers",
        "customer_id",
        scd2=True,
        gold_role=GoldRole.DIMENSION,
        load_order=10,
    ),
    CatalogEntry("products", "product_id", gold_role=GoldRole.DIMENSION, load_order=11),
    CatalogEntry("sellers", "seller_id", gold_role=GoldRole.DIMENSION, load_order=12),
    # Orders arrive continuously, so they load incrementally on the
    # watermark rather than by full reload.
    CatalogEntry(
        "orders",
        "order_id",
        strategy=LoadStrategy.INCREMENTAL,
        gold_role=GoldRole.FACT,
        load_order=20,
        references={"customer_id": "customers"},
    ),
    CatalogEntry(
        "order_items",
        "order_item_id",
        strategy=LoadStrategy.INCREMENTAL,
        gold_role=GoldRole.FACT,
        load_order=21,
        references={"product_id": "products", "seller_id": "sellers"},
    ),
]


def register(session: Session) -> list[SourceObject]:
    """Write the catalog into the control plane, idempotently.

    Re-registering updates the existing rows rather than creating
    duplicates, so this is safe to run on every deploy — which is how a
    catalog stays true rather than drifting from the code.
    """
    system = session.scalars(
        select(SourceSystem).where(SourceSystem.name == SYSTEM_NAME)
    ).one_or_none()
    if system is None:
        system = SourceSystem(name=SYSTEM_NAME, kind=SourceKind.FILE)
        session.add(system)
        session.commit()

    objects: dict[str, SourceObject] = {}
    for entry in CATALOG:
        obj = session.scalars(
            select(SourceObject)
            .where(SourceObject.source_system_id == system.id)
            .where(SourceObject.object_name == entry.name)
        ).one_or_none()
        if obj is None:
            obj = SourceObject(
                source_system_id=system.id,
                schema_name="public",
                object_name=entry.name,
                target_path=f"bronze/{SYSTEM_NAME}/{entry.name}",
            )
            session.add(obj)
        obj.load_strategy = entry.strategy
        obj.primary_key_columns = entry.primary_key
        obj.incremental_column = entry.incremental_column
        obj.scd2_enabled = entry.scd2
        obj.gold_role = entry.gold_role
        obj.load_order = entry.load_order
        obj.active = True
        objects[entry.name] = obj
    session.commit()

    for entry in CATALOG:
        for column, dimension in entry.references.items():
            existing = session.scalars(
                select(GoldReference)
                .where(GoldReference.fact_object_id == objects[entry.name].id)
                .where(GoldReference.fact_column == column)
            ).one_or_none()
            if existing is None:
                session.add(
                    GoldReference(
                        fact_object_id=objects[entry.name].id,
                        dimension_object_id=objects[dimension].id,
                        fact_column=column,
                    )
                )
    session.commit()
    return list(objects.values())


def ordered_objects(session: Session) -> list[SourceObject]:
    """Active objects in load order — dimensions before facts."""
    return list(
        session.scalars(
            select(SourceObject)
            .where(SourceObject.active.is_(True))
            .order_by(SourceObject.load_order, SourceObject.object_name)
        )
    )


def run_all(session: Session, data_dir: Path, lake_root: Path) -> int:
    """Bronze, then Silver, then Gold. Returns the number of failures.

    Failures are counted rather than raised: the runner isolates a bad
    source by design, and a single broken object should not hide the
    fact that eleven others succeeded.
    """
    failures = 0
    bronze = run_pipeline(session, ParquetSource(data_dir), lake_root)
    print(f"bronze  run {bronze.run_id}")
    for outcome in bronze.outcomes:
        marker = "ok  " if outcome.status != "failed" else "FAIL"
        print(f"  {marker} {outcome.object_name:<14} {outcome.strategy}")
        failures += outcome.status == "failed"

    from lakehouse.ingest.bronze import start_pipeline_run

    silver_run = start_pipeline_run(session, "silver")
    print(f"silver  run {silver_run.run_id}")
    for obj in ordered_objects(session):
        try:
            result = build_silver(session, silver_run, obj, lake_root)
        except Exception as exc:
            failures += 1
            print(f"  FAIL {obj.object_name:<14} {type(exc).__name__}: {exc}")
        else:
            print(f"  ok   {obj.object_name:<14} {result.rows_written:>8,} rows")

    gold_run = start_pipeline_run(session, "gold")
    print(f"gold    run {gold_run.run_id}")
    # Dimensions before facts: a fact resolves surrogates by reading the
    # dimension that was just published.
    published = [o for o in ordered_objects(session) if o.gold_role]
    for obj in sorted(published, key=lambda o: o.gold_role != GoldRole.DIMENSION):
        try:
            result_gold = build_gold(session, gold_run, obj, lake_root)
        except Exception as exc:
            failures += 1
            print(f"  FAIL {obj.object_name:<14} {type(exc).__name__}: {exc}")
        else:
            print(
                f"  ok   {obj.object_name:<14} {result_gold.rows_written:>8,} rows"
                f"  ({result_gold.role})"
            )
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse.pipeline", description=__doc__)
    parser.add_argument(
        "--register", action="store_true", help="write the catalog into the control plane"
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data/generated"))
    parser.add_argument("--lake-root", type=Path, default=None)
    args = parser.parse_args(argv)

    settings = Settings()
    configure_logging(settings.log_level)
    lake_root = args.lake_root or Path(settings.lake_root)

    engine = create_engine(database_url(settings))
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        if args.register:
            objects = register(session)
            print(f"registered {len(objects)} object(s) in the control plane")
            return 0

        if not ordered_objects(session):
            print(
                "the control plane is empty — run with --register first",
                file=sys.stderr,
            )
            return 2
        if not args.data_dir.exists():
            print(
                f"no seed data at {args.data_dir} — run 'python -m lakehouse.seed' first",
                file=sys.stderr,
            )
            return 2

        failures = run_all(session, args.data_dir, lake_root)

    print(f"\nlake at {lake_root}")
    if failures:
        print(f"{failures} object(s) failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
