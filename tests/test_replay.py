"""Tests for replay: watermark rewind and table restore."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.metadata.enums import LoadStrategy, WatermarkType
from lakehouse.metadata.models import Base, LoadWatermark, SourceObject, SourceSystem, TaskRun
from lakehouse.replay import (
    ReplayError,
    history,
    main,
    parse_moment,
    plan_rewind,
    render_watermark,
    restore,
    rewind,
    version_at,
)


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


def make_object(session: Session, name: str, strategy: str) -> SourceObject:
    system = session.query(SourceSystem).first()
    if system is None:
        system = SourceSystem(name="seed", kind="file")
        session.add(system)
        session.commit()
    obj = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name=name,
        target_path=f"bronze/seed/{name}",
        load_strategy=strategy,
        primary_key_columns="id",
        incremental_column="updated_at",
    )
    session.add(obj)
    session.commit()
    return obj


def with_watermark(session: Session, obj: SourceObject, value: str, wtype: str) -> None:
    session.add(LoadWatermark(source_object_id=obj.id, watermark_value=value, watermark_type=wtype))
    session.commit()


def versioned_table(path: Path) -> Path:
    """Two versions: v0 has 'old', v1 overwrites with 'new'."""
    write_deltalake(str(path), pa.table({"id": [1], "v": ["old"]}))
    write_deltalake(str(path), pa.table({"id": [1], "v": ["new"]}), mode="overwrite")
    return path


def values(path: Path) -> list[object]:
    return list(DeltaTable(str(path)).to_pyarrow_table().column("v").to_pylist())


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------


def test_a_bare_date_means_the_start_of_that_day() -> None:
    assert parse_moment("2026-03-01") == datetime(2026, 3, 1, tzinfo=UTC)


def test_an_iso_timestamp_is_accepted() -> None:
    assert parse_moment("2026-03-01T12:30:00+00:00").hour == 12


def test_a_naive_timestamp_is_taken_as_utc() -> None:
    assert parse_moment("2026-03-01T12:30:00").tzinfo == UTC


def test_nonsense_is_refused() -> None:
    with pytest.raises(ReplayError, match="not a date"):
        parse_moment("last tuesday")


# --------------------------------------------------------------------------
# Time travel
# --------------------------------------------------------------------------


def test_history_lists_every_version(tmp_path: Path) -> None:
    versions = history(versioned_table(tmp_path / "t"))
    assert [v.version for v in versions] == [0, 1]


def test_history_of_a_missing_table_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ReplayError, match="no Delta table"):
        history(tmp_path / "nope")


def test_version_at_picks_the_newest_version_not_after_the_moment(tmp_path: Path) -> None:
    path = versioned_table(tmp_path / "t")
    first = history(path)[0].timestamp
    assert version_at(path, first) == 0
    assert version_at(path, datetime.now(UTC)) == 1


def test_a_moment_before_the_table_existed_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ReplayError, match="did not exist yet"):
        version_at(versioned_table(tmp_path / "t"), datetime(2000, 1, 1, tzinfo=UTC))


def test_restore_defaults_to_a_dry_run(tmp_path: Path) -> None:
    path = versioned_table(tmp_path / "t")
    restore(path, history(path)[0].timestamp)
    assert values(path) == ["new"]


def test_restore_brings_back_the_old_data(tmp_path: Path) -> None:
    path = versioned_table(tmp_path / "t")
    target = restore(path, history(path)[0].timestamp, dry_run=False)
    assert target == 0
    assert values(path) == ["old"]


def test_a_restore_is_itself_reversible(tmp_path: Path) -> None:
    """Delta records a restore as a new commit, so it can be undone."""
    path = versioned_table(tmp_path / "t")
    restore(path, history(path)[0].timestamp, dry_run=False)
    assert history(path)[-1].version == 2

    DeltaTable(str(path)).restore(1)
    assert values(path) == ["new"]


# --------------------------------------------------------------------------
# Watermark rewind
# --------------------------------------------------------------------------


def test_an_integer_watermark_is_rendered_as_epoch_seconds() -> None:
    moment = datetime(2026, 3, 1, tzinfo=UTC)
    assert render_watermark(moment, WatermarkType.INTEGER) == str(int(moment.timestamp()))


def test_a_timestamp_watermark_is_rendered_as_iso() -> None:
    moment = datetime(2026, 3, 1, tzinfo=UTC)
    assert render_watermark(moment, WatermarkType.TIMESTAMP) == moment.isoformat()


def test_a_string_watermark_cannot_be_rewound_to_a_date() -> None:
    with pytest.raises(ReplayError, match="no ordering relationship"):
        render_watermark(datetime(2026, 3, 1, tzinfo=UTC), WatermarkType.STRING)


def test_a_full_load_source_has_nothing_to_rewind(session: Session) -> None:
    obj = make_object(session, "customers", LoadStrategy.FULL)
    with pytest.raises(ReplayError, match="no watermark to rewind"):
        plan_rewind(session, obj, datetime(2026, 3, 1, tzinfo=UTC))


def test_planning_changes_nothing(session: Session) -> None:
    obj = make_object(session, "orders", LoadStrategy.INCREMENTAL)
    with_watermark(session, obj, "2000000000", WatermarkType.INTEGER)

    plan = plan_rewind(session, obj, datetime(2026, 3, 1, tzinfo=UTC))

    assert plan.previous == "2000000000"
    assert session.get(LoadWatermark, obj.id).watermark_value == "2000000000"  # type: ignore[union-attr]


def test_rewinding_moves_the_watermark_back(session: Session) -> None:
    obj = make_object(session, "orders", LoadStrategy.INCREMENTAL)
    with_watermark(session, obj, "2000000000", WatermarkType.INTEGER)

    moment = datetime(2026, 3, 1, tzinfo=UTC)
    rewind(session, obj, moment)

    stored = session.get(LoadWatermark, obj.id)
    assert stored is not None
    assert stored.watermark_value == str(int(moment.timestamp()))


def test_a_rewind_is_recorded_in_the_audit_trail(session: Session) -> None:
    """The one sanctioned backwards move must never look like a bug."""
    obj = make_object(session, "orders", LoadStrategy.INCREMENTAL)
    with_watermark(session, obj, "2000000000", WatermarkType.INTEGER)

    rewind(session, obj, datetime(2026, 3, 1, tzinfo=UTC))

    task = session.query(TaskRun).filter(TaskRun.source_object_id == obj.id).one()
    assert task.watermark_from == "2000000000"
    assert task.watermark_to is not None
    assert "replay" in (task.error_message or "")


def test_rewinding_a_never_loaded_source_creates_a_watermark(session: Session) -> None:
    obj = make_object(session, "orders", LoadStrategy.INCREMENTAL)
    plan = rewind(session, obj, datetime(2026, 3, 1, tzinfo=UTC))

    assert plan.previous is None
    assert session.get(LoadWatermark, obj.id) is not None


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def test_the_cli_shows_history(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = versioned_table(tmp_path / "t")
    assert main(["history", "--path", str(path)]) == 0
    assert "v1" in capsys.readouterr().out


def test_the_cli_restore_is_a_dry_run_by_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = versioned_table(tmp_path / "t")
    main(["restore", "--path", str(path), "--to", datetime.now(UTC).isoformat()])
    out = capsys.readouterr().out
    assert "would restore" in out
    assert "dry run" in out


def test_the_cli_rewind_is_a_dry_run_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    url = f"sqlite:///{tmp_path / 'cp.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        obj = make_object(s, "orders", LoadStrategy.INCREMENTAL)
        with_watermark(s, obj, "2000000000", WatermarkType.INTEGER)

    assert main(["rewind", "--source", "orders", "--from", "2026-03-01"]) == 0
    assert "would rewind" in capsys.readouterr().out


def test_the_cli_applies_a_rewind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    url = f"sqlite:///{tmp_path / 'cp.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        obj = make_object(s, "orders", LoadStrategy.INCREMENTAL)
        with_watermark(s, obj, "2000000000", WatermarkType.INTEGER)

    main(["rewind", "--source", "orders", "--from", "2026-03-01", "--apply"])
    assert "re-reads everything" in capsys.readouterr().out


def test_the_cli_refuses_an_unknown_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    url = f"sqlite:///{tmp_path / 'cp.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    Base.metadata.create_all(create_engine(url))

    assert main(["rewind", "--source", "nope", "--from", "2026-03-01"]) == 1
    assert "refused" in capsys.readouterr().err
