"""Tests for the metadata control plane.

These assert that the *constraints* work, not merely that the tables exist.
A CHECK constraint nobody has tried to violate is documentation, not a
guarantee.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from lakehouse.metadata import (
    Base,
    ColumnMetadata,
    LoadWatermark,
    ObjectDependency,
    SourceObject,
    SourceSystem,
)
from lakehouse.metadata.enums import LoadStrategy, Sensitivity, SourceKind, WatermarkType


@pytest.fixture
def engine() -> Engine:
    """In-memory database with foreign keys enforced.

    SQLite ignores foreign keys unless asked, which would let the tests
    pass against a schema that fails on PostgreSQL.
    """
    eng = create_engine("sqlite://")

    @event.listens_for(eng, "connect")
    def _fk_on(dbapi_conn: object, _: object) -> None:
        cur = dbapi_conn.cursor()  # type: ignore[attr-defined]
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def session(engine: Engine) -> Iterator[Session]:
    with Session(engine) as s:
        yield s


@pytest.fixture
def system(session: Session) -> SourceSystem:
    sys_ = SourceSystem(name="olist", kind=SourceKind.JDBC, secret_name="olist-conn")
    session.add(sys_)
    session.commit()
    return sys_


def _object(system: SourceSystem, **kw: object) -> SourceObject:
    defaults: dict[str, object] = {
        "source_system_id": system.id,
        "schema_name": "public",
        "object_name": "orders",
        "target_path": "bronze/olist/orders",
        "load_strategy": LoadStrategy.FULL,
    }
    defaults.update(kw)
    return SourceObject(**defaults)


def test_full_load_needs_no_incremental_column(session: Session, system: SourceSystem) -> None:
    session.add(_object(system))
    session.commit()
    assert session.query(SourceObject).count() == 1


def test_incremental_without_column_is_rejected(session: Session, system: SourceSystem) -> None:
    """The constraint that stops a silently-broken incremental load."""
    session.add(_object(system, load_strategy=LoadStrategy.INCREMENTAL))
    with pytest.raises(IntegrityError):
        session.commit()


def test_incremental_with_column_is_accepted(session: Session, system: SourceSystem) -> None:
    session.add(
        _object(
            system,
            load_strategy=LoadStrategy.INCREMENTAL,
            incremental_column="updated_at",
        )
    )
    session.commit()
    assert session.query(SourceObject).count() == 1


def test_cdc_without_primary_key_is_rejected(session: Session, system: SourceSystem) -> None:
    session.add(_object(system, load_strategy=LoadStrategy.CDC))
    with pytest.raises(IntegrityError):
        session.commit()


def test_unknown_load_strategy_is_rejected(session: Session, system: SourceSystem) -> None:
    session.add(_object(system, load_strategy="sometimes"))
    with pytest.raises(IntegrityError):
        session.commit()


def test_duplicate_object_is_rejected(session: Session, system: SourceSystem) -> None:
    session.add(_object(system))
    session.commit()
    session.add(_object(system))
    with pytest.raises(IntegrityError):
        session.commit()


def test_unknown_source_kind_is_rejected(session: Session) -> None:
    session.add(SourceSystem(name="mystery", kind="carrier-pigeon"))
    with pytest.raises(IntegrityError):
        session.commit()


def test_self_dependency_is_rejected(session: Session, system: SourceSystem) -> None:
    obj = _object(system)
    session.add(obj)
    session.commit()
    session.add(ObjectDependency(source_object_id=obj.id, depends_on_id=obj.id))
    with pytest.raises(IntegrityError):
        session.commit()


def test_watermark_round_trip(session: Session, system: SourceSystem) -> None:
    obj = _object(system, load_strategy=LoadStrategy.INCREMENTAL, incremental_column="updated_at")
    session.add(obj)
    session.commit()

    session.add(
        LoadWatermark(
            source_object_id=obj.id,
            watermark_value="2026-09-01T00:00:00Z",
            watermark_type=WatermarkType.TIMESTAMP,
            committed_run_id="run-001",
        )
    )
    session.commit()

    stored = session.get(LoadWatermark, obj.id)
    assert stored is not None
    assert stored.watermark_value == "2026-09-01T00:00:00Z"
    assert stored.committed_run_id == "run-001"


def test_watermark_is_one_per_object(session: Session, system: SourceSystem) -> None:
    obj = _object(system, load_strategy=LoadStrategy.INCREMENTAL, incremental_column="updated_at")
    session.add(obj)
    session.commit()
    for _ in range(2):
        session.add(
            LoadWatermark(
                source_object_id=obj.id,
                watermark_value="1",
                watermark_type=WatermarkType.INTEGER,
            )
        )
    with pytest.raises(IntegrityError):
        session.commit()


def test_orphan_object_is_rejected(session: Session) -> None:
    """Foreign keys must be enforced, or config can reference nothing."""
    session.add(
        SourceObject(
            source_system_id=9999,
            schema_name="public",
            object_name="ghost",
            target_path="bronze/ghost",
            load_strategy=LoadStrategy.FULL,
        )
    )
    with pytest.raises(IntegrityError):
        session.commit()


def test_column_sensitivity_defaults_to_none(session: Session, system: SourceSystem) -> None:
    obj = _object(system)
    session.add(obj)
    session.commit()
    col = ColumnMetadata(source_object_id=obj.id, column_name="customer_email")
    session.add(col)
    session.commit()
    assert col.sensitivity == Sensitivity.NONE


def test_invalid_sensitivity_is_rejected(session: Session, system: SourceSystem) -> None:
    obj = _object(system)
    session.add(obj)
    session.commit()
    session.add(ColumnMetadata(source_object_id=obj.id, column_name="x", sensitivity="top-secret"))
    with pytest.raises(IntegrityError):
        session.commit()


def test_cascade_delete_removes_children(session: Session, system: SourceSystem) -> None:
    obj = _object(system)
    session.add(obj)
    session.commit()
    session.add(ColumnMetadata(source_object_id=obj.id, column_name="x"))
    session.commit()

    session.delete(obj)
    session.commit()
    assert session.query(ColumnMetadata).count() == 0
