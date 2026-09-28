"""Migrations: they build the schema the models describe, and adopt old databases."""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Engine, create_engine, inspect, text

from lakehouse.metadata.models import Base
from lakehouse.migrate import (
    BASELINE,
    alembic_config,
    current,
    ensure_schema,
    head,
    is_legacy,
    main,
)
from schema_shape import shape


@pytest.fixture
def engine(tmp_path: Path) -> Engine:
    return create_engine(f"sqlite:///{tmp_path / 'control.db'}")


def legacy(engine: Engine) -> None:
    """A database as `create_all` built it before migrations existed."""
    command.upgrade(alembic_config(engine), BASELINE)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE alembic_version"))
        conn.execute(
            text(
                "INSERT INTO pipeline_run (run_id, pipeline_name, status, started_at) "
                "VALUES ('kept', 'silver', 'succeeded', CURRENT_TIMESTAMP)"
            )
        )


def test_an_empty_database_is_built_to_head(engine: Engine) -> None:
    assert ensure_schema(engine) == head()
    assert current(engine) == head()


def test_migrations_build_exactly_what_the_models_describe(engine: Engine, tmp_path: Path) -> None:
    """The guard that would have caught step 36: models and migrations agree."""
    reference = create_engine(f"sqlite:///{tmp_path / 'reference.db'}")
    Base.metadata.create_all(reference)
    ensure_schema(engine)
    assert shape(engine) == shape(reference)


def test_no_model_change_is_missing_a_migration(engine: Engine) -> None:
    """Change a model without writing a migration, and this fails."""
    ensure_schema(engine)
    with engine.connect() as conn:
        context = MigrationContext.configure(conn, opts={"compare_type": True})
        assert compare_metadata(context, Base.metadata) == []


def test_running_it_again_changes_nothing(engine: Engine) -> None:
    ensure_schema(engine)
    before = shape(engine)
    assert ensure_schema(engine) == head()
    assert shape(engine) == before


def test_a_legacy_database_is_adopted_not_rebuilt(engine: Engine) -> None:
    legacy(engine)
    assert is_legacy(engine)

    assert ensure_schema(engine) == head()

    assert not is_legacy(engine)
    with engine.connect() as conn:
        kept: list[str] = list(conn.execute(text("SELECT run_id FROM pipeline_run")).scalars())
    assert kept == ["kept"]


def test_an_empty_database_is_not_mistaken_for_a_legacy_one(engine: Engine) -> None:
    assert not is_legacy(engine)


def test_duration_holds_fractions_after_migrating(engine: Engine) -> None:
    ensure_schema(engine)
    column = next(
        c for c in inspect(engine).get_columns("task_run") if c["name"] == "duration_seconds"
    )
    assert "FLOAT" in str(column["type"]).upper() or "REAL" in str(column["type"]).upper()


def test_every_migration_downgrades_and_upgrades_cleanly(engine: Engine) -> None:
    config = alembic_config(engine)
    ensure_schema(engine)
    command.downgrade(config, "base")
    assert [t for t in inspect(engine).get_table_names() if t != "alembic_version"] == []
    command.upgrade(config, "head")
    assert current(engine) == head()


def test_check_reports_pending_work(
    engine: Engine, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATABASE_URL", str(engine.url))

    assert main(["--check"]) == 1
    assert "empty" in capsys.readouterr().out

    assert main([]) == 0
    assert f"control plane at {head()}" in capsys.readouterr().out

    assert main(["--check"]) == 0


def test_check_names_a_legacy_database(
    engine: Engine, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    legacy(engine)
    monkeypatch.setenv("DATABASE_URL", str(engine.url))
    assert main(["--check"]) == 1
    assert "legacy" in capsys.readouterr().out
