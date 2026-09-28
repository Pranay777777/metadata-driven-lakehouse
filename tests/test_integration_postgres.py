"""The platform against a real Postgres, not SQLite.

Every other test uses SQLite for speed, and SQLite is forgiving in ways
Postgres is not: it ignores most type errors, has no real booleans, and
treats `date()` differently. These tests run the same code against the
engine the compose stack and any real deployment use.

They need an admin URL to a Postgres server:

    LAKEHOUSE_TEST_POSTGRES_URL=postgresql+psycopg://app:app@localhost:5432/app

Each test creates its own throwaway database and drops it afterwards, so
pointing this at the compose stack never touches the `app` control plane.
Without the variable, every test here is skipped.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import DateTime, Engine, create_engine, literal, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from lakehouse import pipeline, privacy
from lakehouse.audit import close_abandoned, day, snapshot
from lakehouse.metadata.enums import RunStatus
from lakehouse.metadata.models import Base, PipelineRun, TaskRun
from lakehouse.migrate import BASELINE, alembic_config, current, ensure_schema, head
from lakehouse.seed.generator import SeedConfig, generate, write
from schema_shape import shape

ADMIN_URL = os.environ.get("LAKEHOUSE_TEST_POSTGRES_URL", "")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not ADMIN_URL, reason="LAKEHOUSE_TEST_POSTGRES_URL is not set"),
]


def _scratch_database() -> Iterator[str]:
    admin = create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT")
    name = f"lakehouse_test_{uuid.uuid4().hex[:10]}"
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    url = make_url(ADMIN_URL).set(database=name).render_as_string(hide_password=False)
    try:
        yield url
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture
def pg_url() -> Iterator[str]:
    yield from _scratch_database()


@pytest.fixture
def reference_url() -> Iterator[str]:
    yield from _scratch_database()


@pytest.fixture
def pg(pg_url: str) -> Iterator[Engine]:
    engine = create_engine(pg_url)
    yield engine
    engine.dispose()


# --- schema ---------------------------------------------------------------


def test_migrations_match_the_models_on_postgres(pg: Engine, reference_url: str) -> None:
    reference = create_engine(reference_url)
    Base.metadata.create_all(reference)
    ensure_schema(pg)
    try:
        assert shape(pg) == shape(reference)
    finally:
        reference.dispose()


def test_a_legacy_postgres_control_plane_is_adopted_with_its_history(pg: Engine) -> None:
    command.upgrade(alembic_config(pg), BASELINE)
    with pg.begin() as conn:
        conn.execute(text("DROP TABLE alembic_version"))
        conn.execute(
            text(
                "INSERT INTO pipeline_run (run_id, pipeline_name, status) "
                "VALUES ('kept', 'silver', 'succeeded')"
            )
        )

    assert ensure_schema(pg) == head()

    with pg.connect() as conn:
        assert conn.execute(text("SELECT run_id FROM pipeline_run")).scalar_one() == "kept"
        kind: str = conn.execute(
            text(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name = 'task_run' AND column_name = 'duration_seconds'"
            )
        ).scalar_one()
    assert kind == "double precision"


def test_downgrade_and_upgrade_round_trip_on_postgres(pg: Engine) -> None:
    ensure_schema(pg)
    command.downgrade(alembic_config(pg), "base")
    command.upgrade(alembic_config(pg), "head")
    assert current(pg) == head()


# --- the platform end to end ----------------------------------------------


@pytest.fixture
def populated(pg_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Seed, register, run, classify, rebuild — the demo, on Postgres."""
    monkeypatch.setenv("DATABASE_URL", pg_url)
    monkeypatch.delenv("DATABASE_URL_SECRET", raising=False)
    monkeypatch.setenv("MASKING_KEY", "integration-test-key")
    data, lake = tmp_path / "data", tmp_path / "lake"
    write(generate(SeedConfig(rows=3_000)), data)
    args = ["--data-dir", str(data), "--lake-root", str(lake)]
    assert pipeline.main(["--register", *args]) == 0
    assert pipeline.main(args) == 0
    assert privacy.main(["--apply", "--lake-root", str(lake)]) == 0
    assert pipeline.main(args) == 0
    return pg_url


def test_the_pipeline_runs_clean_on_postgres(populated: str) -> None:
    with Session(create_engine(populated)) as s:
        failed = s.scalars(select(TaskRun).where(TaskRun.status == RunStatus.FAILED)).all()
        open_runs = s.scalars(
            select(PipelineRun).where(PipelineRun.status == RunStatus.RUNNING)
        ).all()
    assert failed == []
    assert open_runs == []


def test_every_dashboard_view_computes_on_postgres(populated: str) -> None:
    with Session(create_engine(populated)) as s:
        snap = snapshot(s)
        assert close_abandoned(s, older_than=timedelta(0)) == []
    assert not snap.empty
    assert len(snap.runs) >= 3
    assert {v.layer for v in snap.volume} == {"bronze", "silver", "gold"}
    # Fractional durations reach the cost proxy — the reason for 0002.
    assert sum(v.compute_seconds for v in snap.volume) > 0
    assert snap.masked_columns == 4
    assert any(not c.reaches_gold for c in snap.privacy)
    assert all(f.last_loaded_at is not None for f in snap.freshness)


def test_day_buckets_timestamptz_on_postgres(pg: Engine) -> None:
    """date() on timestamptz uses the session time zone — UTC on the stack."""
    moment = literal(datetime(2026, 9, 27, 23, 30, tzinfo=UTC), DateTime(timezone=True))
    with pg.connect() as conn:
        conn.execute(text("SET TIME ZONE 'UTC'"))
        bucket = conn.execute(select(day(moment))).scalar_one()
    assert str(bucket)[:10] == "2026-09-27"
