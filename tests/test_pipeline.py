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


def test_every_run_is_closed_when_the_pipeline_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Before step 38 the Silver and Gold runs were opened and never closed.

    Every invocation left two rows 'running' forever, indistinguishable
    from a crashed process. The ops dashboard caught it on first render.
    """
    from sqlalchemy import select

    from lakehouse.metadata.enums import RunStatus
    from lakehouse.metadata.models import PipelineRun

    url = f"sqlite:///{tmp_path / 'control.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    data, lake = tmp_path / "data", tmp_path / "lake"
    write(generate(SeedConfig(rows=500)), data)

    assert main(["--register", "--data-dir", str(data), "--lake-root", str(lake)]) == 0
    assert main(["--data-dir", str(data), "--lake-root", str(lake)]) == 0

    with Session(create_engine(url)) as s:
        runs = list(s.scalars(select(PipelineRun)))
    assert {r.pipeline_name for r in runs} >= {"silver", "gold"}
    assert [r.pipeline_name for r in runs if r.status == RunStatus.RUNNING] == []
    assert all(r.ended_at is not None for r in runs)


def test_a_new_source_is_onboarded_by_configuration_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The claim in the README's first line, as a test.

    A sixth source joins the platform with one control-plane row and a
    file — no Python, no new pipeline. It is inserted with plain SQL, not
    through CATALOG, so nothing in the code knows it exists.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from sqlalchemy import select, text

    from lakehouse.metadata.enums import Layer, RunStatus
    from lakehouse.metadata.models import TaskRun
    from lakehouse.transform.silver import read_silver

    url = f"sqlite:///{tmp_path / 'control.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    data, lake = tmp_path / "data", tmp_path / "lake"
    write(generate(SeedConfig(rows=500)), data)
    assert main(["--register", "--data-dir", str(data), "--lake-root", str(lake)]) == 0

    # The whole onboarding: a file lands, and one row describes it.
    pq.write_table(
        pa.table(
            {
                "return_id": ["r1", "r2", "r2"],
                "order_id": ["o1", "o2", "o2"],
                "reason": ["damaged", "late", "late"],
                "updated_at": [1, 2, 2],
            }
        ),
        data / "returns.parquet",
    )
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO source_object (source_system_id, schema_name, object_name, "
                "target_path, load_strategy, primary_key_columns, incremental_column, "
                "active, load_order, cdc_delete_value, scd2_enabled, watermark_grace, "
                "created_at, updated_at) "
                "SELECT id, 'public', 'returns', 'bronze/seed/returns', 'full', 'return_id', "
                "'updated_at', 1, 30, 'D', 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP "
                "FROM source_system WHERE name = 'seed'"
            )
        )

    assert main(["--data-dir", str(data), "--lake-root", str(lake)]) == 0

    with Session(engine) as s:
        returns = s.scalars(select(SourceObject).where(SourceObject.object_name == "returns")).one()
        tasks = s.scalars(select(TaskRun).where(TaskRun.source_object_id == returns.id)).all()
        assert {t.layer for t in tasks} == {Layer.BRONZE, Layer.SILVER}
        assert all(t.status == RunStatus.SUCCEEDED for t in tasks)
        silver = read_silver(lake, returns)
    assert sorted(silver.column("return_id").to_pylist()) == ["r1", "r2"]  # deduplicated
