"""Tests for the end-to-end pipeline entry point."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.metadata.enums import GoldRole, LoadStrategy
from lakehouse.metadata.models import Base, GoldReference, SourceObject, SourceSystem
from lakehouse.pipeline import CATALOG, main, ordered_objects, register, run_all
from lakehouse.seed.generator import SeedConfig, generate, write


@pytest.fixture
def session() -> Iterator[Session]:
    eng: Engine = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _fk(conn: object, _: object) -> None:
        cur = conn.cursor()  # type: ignore[attr-defined]
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    with Session(eng) as s:
        yield s


@pytest.fixture
def seeded(tmp_path: Path) -> Path:
    data = tmp_path / "data"
    write(generate(SeedConfig(rows=2000, seed=42)), data)
    return data


# --------------------------------------------------------------------------
# Registration
# --------------------------------------------------------------------------


def test_registering_creates_the_whole_catalog(session: Session) -> None:
    register(session)

    assert session.query(SourceSystem).count() == 1
    assert session.query(SourceObject).count() == len(CATALOG)


def test_registering_twice_updates_rather_than_duplicates(session: Session) -> None:
    """Safe to run on every deploy, which is how a catalog stays true."""
    register(session)
    register(session)

    assert session.query(SourceObject).count() == len(CATALOG)
    assert session.query(GoldReference).count() == 3


def test_registration_reflects_edited_catalog_config(session: Session) -> None:
    register(session)
    obj = session.query(SourceObject).filter_by(object_name="customers").one()
    obj.scd2_enabled = False
    session.commit()

    register(session)
    session.refresh(obj)
    assert obj.scd2_enabled is True, "re-registering must restore the declared config"


def test_the_star_schema_edges_are_registered(session: Session) -> None:
    register(session)
    columns = {r.fact_column for r in session.query(GoldReference).all()}
    assert columns == {"customer_id", "product_id", "seller_id"}


def test_dimensions_are_ordered_before_facts(session: Session) -> None:
    register(session)
    roles = [o.gold_role for o in ordered_objects(session)]
    first_fact = roles.index(GoldRole.FACT)
    assert GoldRole.DIMENSION not in roles[first_fact:]


def test_orders_load_incrementally(session: Session) -> None:
    register(session)
    orders = session.query(SourceObject).filter_by(object_name="orders").one()
    assert orders.load_strategy == LoadStrategy.INCREMENTAL


# --------------------------------------------------------------------------
# Running
# --------------------------------------------------------------------------


def test_the_whole_pipeline_runs_clean(session: Session, seeded: Path, tmp_path: Path) -> None:
    register(session)
    failures = run_all(session, seeded, tmp_path / "lake")
    assert failures == 0


def test_every_layer_lands_on_disk(session: Session, seeded: Path, tmp_path: Path) -> None:
    register(session)
    lake = tmp_path / "lake"
    run_all(session, seeded, lake)

    assert (lake / "bronze" / "seed" / "customers" / "_delta_log").exists()
    assert (lake / "silver" / "seed" / "customers" / "_delta_log").exists()
    assert (lake / "gold" / "dim_customers" / "_delta_log").exists()
    assert (lake / "gold" / "fact_orders" / "_delta_log").exists()


def test_running_twice_is_safe(session: Session, seeded: Path, tmp_path: Path) -> None:
    """Re-running must not duplicate rows or fail on existing tables."""
    register(session)
    lake = tmp_path / "lake"
    assert run_all(session, seeded, lake) == 0
    assert run_all(session, seeded, lake) == 0


def test_a_broken_object_is_isolated_not_fatal(
    session: Session, seeded: Path, tmp_path: Path
) -> None:
    """One bad source must not stale the rest of the lake."""
    register(session)
    broken = session.query(SourceObject).filter_by(object_name="products").one()
    broken.primary_key_columns = "does_not_exist"
    session.commit()

    failures = run_all(session, seeded, tmp_path / "lake")
    assert failures > 0
    assert (tmp_path / "lake" / "silver" / "seed" / "customers").exists(), (
        "the healthy objects must still have loaded"
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def test_the_cli_registers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'cp.db'}")
    assert main(["--register"]) == 0


def test_the_cli_refuses_an_empty_control_plane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'cp.db'}")
    assert main(["--data-dir", str(tmp_path)]) == 2
    assert "--register" in capsys.readouterr().err


def test_the_cli_refuses_missing_seed_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'cp.db'}")
    main(["--register"])
    assert main(["--data-dir", str(tmp_path / "nope")]) == 2
    assert "lakehouse.seed" in capsys.readouterr().err


def test_the_cli_runs_end_to_end(
    tmp_path: Path, seeded: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'cp.db'}")
    main(["--register"])
    assert main(["--data-dir", str(seeded), "--lake-root", str(tmp_path / "lake")]) == 0
