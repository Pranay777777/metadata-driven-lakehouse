"""Tests for the Dagster asset graph.

These run Dagster in-process against SQLite and a temporary lake, so
they need no daemon, no webserver and no Postgres.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from dagster import AssetsDefinition, materialize
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.config import Settings
from lakehouse.metadata.models import Base
from lakehouse.orchestration import LakehouseResource, build_definitions
from lakehouse.orchestration.definitions import (
    all_assets,
    bronze_key,
    gold_dependencies,
    gold_key,
    schedule,
    silver_key,
)
from lakehouse.pipeline import CATALOG, register
from lakehouse.seed.generator import SeedConfig, generate, write


@pytest.fixture
def seeded(tmp_path: Path) -> Path:
    data = tmp_path / "data"
    write(generate(SeedConfig(rows=2000, seed=42)), data)
    return data


@pytest.fixture
def resource(tmp_path: Path, seeded: Path, monkeypatch: pytest.MonkeyPatch) -> LakehouseResource:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'cp.db'}")
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    settings = Settings()

    engine: Engine = create_engine(settings.database_url)

    @event.listens_for(engine, "connect")
    def _fk(conn: object, _: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(engine)
    with Session(engine) as session:
        register(session)
    return LakehouseResource(settings=settings, data_dir=seeded)


@pytest.fixture
def assets() -> Iterator[list[AssetsDefinition]]:
    yield list(all_assets())


# --------------------------------------------------------------------------
# Shape of the graph
# --------------------------------------------------------------------------


def test_every_catalog_entry_produces_its_assets(assets: list[AssetsDefinition]) -> None:
    keys = {a.key for a in assets}
    for entry in CATALOG:
        assert bronze_key(entry.name) in keys
        assert silver_key(entry.name) in keys
        if entry.gold_role:
            assert gold_key(entry.name) in keys


def test_an_object_without_a_gold_role_has_no_gold_asset(assets: list[AssetsDefinition]) -> None:
    keys = {a.key for a in assets}
    for entry in CATALOG:
        if entry.gold_role is None:
            assert gold_key(entry.name) not in keys


def test_silver_depends_on_its_own_bronze() -> None:
    entry = next(e for e in CATALOG if e.name == "customers")
    silver = next(a for a in all_assets() if a.key == silver_key(entry.name))
    assert bronze_key("customers") in silver.asset_deps[silver_key("customers")]


def test_a_fact_waits_for_every_dimension_it_references() -> None:
    """Otherwise the fact resolves surrogates against a dimension that
    has not been published, and every key lands on the unknown member."""
    orders = next(e for e in CATALOG if e.name == "orders")
    deps = gold_dependencies(orders)

    assert silver_key("orders") in deps
    assert gold_key("customers") in deps


def test_order_items_waits_for_both_of_its_dimensions() -> None:
    items = next(e for e in CATALOG if e.name == "order_items")
    deps = gold_dependencies(items)
    assert gold_key("products") in deps
    assert gold_key("sellers") in deps


def test_a_dimension_waits_only_for_its_silver() -> None:
    customers = next(e for e in CATALOG if e.name == "customers")
    assert gold_dependencies(customers) == [silver_key("customers")]


def test_the_schedule_is_stopped_by_default() -> None:
    """A schedule that starts itself when someone opens the UI is a surprise."""
    assert schedule.default_status.value == "STOPPED"
    assert schedule.cron_schedule == "0 2 * * *"


def test_definitions_load_with_a_resource(resource: LakehouseResource) -> None:
    defs = build_definitions(resource)
    assert len(list(defs.assets or [])) == 15


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------


def test_the_whole_graph_materialises(
    resource: LakehouseResource, assets: list[AssetsDefinition]
) -> None:
    result = materialize(assets, resources={"lakehouse": resource})

    assert result.success
    assert len(result.get_asset_materialization_events()) == 15


def test_a_single_asset_can_be_rerun(resource: LakehouseResource) -> None:
    """Partial reruns are the reason for using an orchestrator at all."""
    everything = list(all_assets())
    materialize(everything, resources={"lakehouse": resource})

    just_bronze = [a for a in all_assets() if a.key == bronze_key("customers")]
    result = materialize(just_bronze, resources={"lakehouse": resource})
    assert result.success


def test_materialising_lands_all_three_layers(
    resource: LakehouseResource, assets: list[AssetsDefinition], tmp_path: Path
) -> None:
    materialize(assets, resources={"lakehouse": resource})

    lake = tmp_path / "lake"
    assert (lake / "bronze" / "seed" / "customers" / "_delta_log").exists()
    assert (lake / "silver" / "seed" / "customers" / "_delta_log").exists()
    assert (lake / "gold" / "dim_customers" / "_delta_log").exists()
    assert (lake / "gold" / "fact_orders" / "_delta_log").exists()


def test_an_unregistered_object_fails_with_a_useful_message(
    tmp_path: Path, seeded: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'empty.db'}")
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    settings = Settings()
    Base.metadata.create_all(create_engine(settings.database_url))

    empty = LakehouseResource(settings=settings, data_dir=seeded)
    silver = [a for a in all_assets() if a.key == silver_key("customers")]
    result = materialize(silver, resources={"lakehouse": empty}, raise_on_error=False)
    assert not result.success
