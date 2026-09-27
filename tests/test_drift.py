"""Tests for schema drift detection and the evolution policy."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.drift import (
    DriftKind,
    SchemaDriftError,
    check_drift,
    compare,
    current_version,
    describe_schema,
    schema_hash,
)
from lakehouse.metadata.enums import LoadStrategy, RunStatus, SourceKind
from lakehouse.metadata.models import Base, SchemaVersion, SourceObject, SourceSystem, TaskRun


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
def obj(session: Session) -> SourceObject:
    system = SourceSystem(name="seed", kind=SourceKind.FILE)
    session.add(system)
    session.commit()
    o = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name="customers",
        target_path="bronze/seed/customers",
        load_strategy=LoadStrategy.FULL,
        primary_key_columns="customer_id",
    )
    session.add(o)
    session.commit()
    return o


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


BASE = pa.table({"customer_id": ["c1"], "city": ["rio"]})
WIDER = pa.table({"customer_id": ["c1"], "city": ["rio"], "state": ["RJ"]})
NARROWER = pa.table({"customer_id": ["c1"]})
RETYPED = pa.table({"customer_id": [1], "city": ["rio"]})


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------


def test_an_identical_schema_is_no_drift() -> None:
    assert compare(describe_schema(BASE), describe_schema(BASE)).kind == DriftKind.NONE


def test_a_new_column_is_additive() -> None:
    drift = compare(describe_schema(BASE), describe_schema(WIDER))
    assert drift.kind == DriftKind.ADDITIVE
    assert drift.added == ["state"]


def test_a_removed_column_is_breaking() -> None:
    drift = compare(describe_schema(BASE), describe_schema(NARROWER))
    assert drift.kind == DriftKind.BREAKING
    assert drift.removed == ["city"]


def test_a_retyped_column_is_breaking() -> None:
    drift = compare(describe_schema(BASE), describe_schema(RETYPED))
    assert drift.kind == DriftKind.BREAKING
    assert drift.retyped[0][0] == "customer_id"


def test_the_worst_finding_decides_the_outcome() -> None:
    """An addition alongside a removal is still breaking."""
    mixed = pa.table({"customer_id": ["c1"], "state": ["RJ"]})
    drift = compare(describe_schema(BASE), describe_schema(mixed))
    assert drift.kind == DriftKind.BREAKING
    assert drift.added == ["state"]
    assert drift.removed == ["city"]


def test_column_order_alone_is_not_drift() -> None:
    """Reordered columns mean nothing; churn here would cry wolf."""
    reordered = pa.table({"city": ["rio"], "customer_id": ["c1"]})
    assert schema_hash(describe_schema(BASE)) == schema_hash(describe_schema(reordered))


def test_describe_captures_nullability() -> None:
    assert describe_schema(BASE)[0]["nullable"] is True


def test_describe_reports_no_change_readably() -> None:
    assert compare(describe_schema(BASE), describe_schema(BASE)).describe() == "no change"


def test_describe_lists_every_finding() -> None:
    mixed = pa.table({"customer_id": [1], "state": ["RJ"]})
    text = compare(describe_schema(BASE), describe_schema(mixed)).describe()
    assert "added state" in text
    assert "removed city" in text
    assert "retyped customer_id" in text


# --------------------------------------------------------------------------
# Policy
# --------------------------------------------------------------------------


def test_the_first_sighting_records_version_one(session: Session, obj: SourceObject) -> None:
    """An unknown source is being onboarded, not misbehaving."""
    drift = check_drift(session, obj, BASE)

    assert drift.kind == DriftKind.NONE
    assert drift.version == 1
    recorded = current_version(session, obj)
    assert recorded is not None
    assert json.loads(recorded.schema_json)[0]["name"] == "customer_id"


def test_an_unchanged_schema_does_not_add_a_version(session: Session, obj: SourceObject) -> None:
    check_drift(session, obj, BASE)
    check_drift(session, obj, BASE)

    assert session.query(SchemaVersion).count() == 1


def test_an_additive_change_evolves_and_supersedes(session: Session, obj: SourceObject) -> None:
    check_drift(session, obj, BASE)
    drift = check_drift(session, obj, WIDER)

    assert drift.kind == DriftKind.ADDITIVE
    assert drift.version == 2
    assert session.query(SchemaVersion).count() == 2

    versions = session.query(SchemaVersion).order_by(SchemaVersion.version).all()
    assert [v.is_current for v in versions] == [False, True], "exactly one version is current"


def test_a_breaking_change_stops_the_load(session: Session, obj: SourceObject) -> None:
    check_drift(session, obj, BASE)
    with pytest.raises(SchemaDriftError, match="removed city"):
        check_drift(session, obj, NARROWER)


def test_a_refused_change_leaves_the_recorded_schema_alone(
    session: Session, obj: SourceObject
) -> None:
    """The next run must re-report the drift, not silently accept it."""
    check_drift(session, obj, BASE)
    with pytest.raises(SchemaDriftError):
        check_drift(session, obj, NARROWER)

    assert session.query(SchemaVersion).count() == 1
    recorded = current_version(session, obj)
    assert recorded is not None
    assert recorded.version == 1

    with pytest.raises(SchemaDriftError):
        check_drift(session, obj, NARROWER)


def test_the_error_names_the_object_and_the_change(session: Session, obj: SourceObject) -> None:
    check_drift(session, obj, BASE)
    with pytest.raises(SchemaDriftError) as caught:
        check_drift(session, obj, RETYPED)
    message = str(caught.value)
    assert "customers" in message
    assert "string -> int64" in message


# --------------------------------------------------------------------------
# Wired into ingestion
# --------------------------------------------------------------------------


def test_a_bronze_load_records_the_schema(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(BASE), tmp_path)

    recorded = current_version(session, obj)
    assert recorded is not None
    assert recorded.version == 1


def test_a_bronze_load_absorbs_an_additive_change(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(BASE), tmp_path)
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(WIDER), tmp_path)

    recorded = current_version(session, obj)
    assert recorded is not None
    assert recorded.version == 2


def test_a_breaking_change_fails_the_task_and_is_audited(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(BASE), tmp_path)

    run = start_pipeline_run(session, "bronze")
    with pytest.raises(SchemaDriftError):
        load_full(session, run, obj, Fixed(NARROWER), tmp_path)

    task = session.query(TaskRun).filter(TaskRun.run_id == run.run_id).one()
    assert task.status == RunStatus.FAILED
    assert "SchemaDriftError" in (task.error_message or "")


def test_two_objects_track_their_schemas_independently(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    other = SourceObject(
        source_system_id=obj.source_system_id,
        schema_name="public",
        object_name="orders",
        target_path="bronze/seed/orders",
        load_strategy=LoadStrategy.FULL,
        primary_key_columns="order_id",
    )
    session.add(other)
    session.commit()

    check_drift(session, obj, BASE)
    check_drift(session, other, pa.table({"order_id": ["o1"]}))
    check_drift(session, obj, WIDER)

    current = current_version(session, other)
    assert current is not None
    assert current.version == 1, "one object evolving must not touch another"
