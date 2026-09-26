"""Tests for CDC merge ingestion."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import read_bronze, start_pipeline_run
from lakehouse.ingest.cdc import collapse_changes, key_columns, load_cdc, split_deletes
from lakehouse.metadata.enums import LoadStrategy, RunStatus, SourceKind
from lakehouse.metadata.models import Base, SourceObject, SourceSystem, TaskRun


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
    system = SourceSystem(name="seed", kind=SourceKind.JDBC)
    session.add(system)
    session.commit()
    o = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name="customers",
        target_path="bronze/seed/customers",
        load_strategy=LoadStrategy.CDC,
        primary_key_columns="customer_id",
        incremental_column="seq",
        cdc_operation_column="op",
        cdc_delete_value="D",
    )
    session.add(o)
    session.commit()
    return o


class Batch:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def _changes(ids: list[str], cities: list[str], ops: list[str], seq: list[int]) -> pa.Table:
    return pa.table({"customer_id": ids, "city": cities, "op": ops, "seq": seq})


def _current(tmp_path: Path, obj: SourceObject) -> dict[str, str]:
    table = read_bronze(tmp_path, obj)
    return dict(
        zip(
            table.column("customer_id").to_pylist(),
            table.column("city").to_pylist(),
            strict=True,
        )
    )


def test_first_batch_inserts_everything(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run = start_pipeline_run(session, "cdc")
    batch = _changes(["c1", "c2"], ["rio", "recife"], ["I", "I"], [1, 1])
    result = load_cdc(session, run, obj, Batch(batch), tmp_path)

    assert result.inserted == 2
    assert _current(tmp_path, obj) == {"c1": "rio", "c2": "recife"}


def test_update_changes_the_row_in_place(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run1 = start_pipeline_run(session, "cdc")
    load_cdc(session, run1, obj, Batch(_changes(["c1"], ["rio"], ["I"], [1])), tmp_path)

    run2 = start_pipeline_run(session, "cdc")
    result = load_cdc(session, run2, obj, Batch(_changes(["c1"], ["manaus"], ["U"], [2])), tmp_path)

    assert result.updated == 1
    assert result.inserted == 0
    assert _current(tmp_path, obj) == {"c1": "manaus"}


def test_delete_removes_the_row(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    """A feed that signals a delete must not leave the row behind."""
    run1 = start_pipeline_run(session, "cdc")
    load_cdc(
        session,
        run1,
        obj,
        Batch(_changes(["c1", "c2"], ["rio", "recife"], ["I", "I"], [1, 1])),
        tmp_path,
    )

    run2 = start_pipeline_run(session, "cdc")
    result = load_cdc(session, run2, obj, Batch(_changes(["c2"], ["recife"], ["D"], [2])), tmp_path)

    assert result.deleted == 1
    assert _current(tmp_path, obj) == {"c1": "rio"}


def test_insert_update_delete_in_one_batch(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run1 = start_pipeline_run(session, "cdc")
    load_cdc(
        session,
        run1,
        obj,
        Batch(_changes(["c1", "c2"], ["rio", "recife"], ["I", "I"], [1, 1])),
        tmp_path,
    )

    run2 = start_pipeline_run(session, "cdc")
    mixed = _changes(
        ["c1", "c2", "c3"], ["curitiba", "recife", "salvador"], ["U", "D", "I"], [2, 2, 2]
    )
    result = load_cdc(session, run2, obj, Batch(mixed), tmp_path)

    assert (result.inserted, result.updated, result.deleted) == (1, 1, 1)
    assert _current(tmp_path, obj) == {"c1": "curitiba", "c3": "salvador"}


def test_multiple_changes_for_one_key_collapse_to_the_latest(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Three changes for one key in one batch must apply only the newest."""
    run1 = start_pipeline_run(session, "cdc")
    load_cdc(session, run1, obj, Batch(_changes(["c1"], ["rio"], ["I"], [1])), tmp_path)

    run2 = start_pipeline_run(session, "cdc")
    churn = _changes(["c1", "c1", "c1"], ["a", "b", "final"], ["U", "U", "U"], [2, 3, 4])
    result = load_cdc(session, run2, obj, Batch(churn), tmp_path)

    assert result.rows_read == 3
    assert result.rows_after_collapse == 1
    assert result.collapsed == 2
    assert _current(tmp_path, obj) == {"c1": "final"}


def test_insert_then_delete_in_one_batch_leaves_nothing(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Collapsing keeps the delete, so the row never appears."""
    run1 = start_pipeline_run(session, "cdc")
    load_cdc(session, run1, obj, Batch(_changes(["c1"], ["rio"], ["I"], [1])), tmp_path)

    run2 = start_pipeline_run(session, "cdc")
    load_cdc(
        session,
        run2,
        obj,
        Batch(_changes(["c2", "c2"], ["new", "new"], ["I", "D"], [2, 3])),
        tmp_path,
    )

    assert "c2" not in _current(tmp_path, obj)


def test_delete_for_unknown_key_is_harmless(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run = start_pipeline_run(session, "cdc")
    load_cdc(session, run, obj, Batch(_changes(["c9"], ["nowhere"], ["D"], [1])), tmp_path)
    assert read_bronze(tmp_path, obj).num_rows == 0


def test_replaying_the_same_batch_is_idempotent(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Redelivery is normal in CDC; it must not duplicate rows."""
    run1 = start_pipeline_run(session, "cdc")
    load_cdc(
        session,
        run1,
        obj,
        Batch(_changes(["c1", "c2"], ["rio", "recife"], ["I", "I"], [1, 1])),
        tmp_path,
    )
    batch = _changes(["c1", "c2"], ["rio", "recife"], ["U", "U"], [2, 2])
    run2 = start_pipeline_run(session, "cdc")
    load_cdc(session, run2, obj, Batch(batch), tmp_path)
    run3 = start_pipeline_run(session, "cdc")
    load_cdc(session, run3, obj, Batch(batch), tmp_path)

    assert read_bronze(tmp_path, obj).num_rows == 2


def test_composite_key_merge(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    obj.primary_key_columns = "customer_id, city"
    session.commit()
    assert key_columns(obj) == ["customer_id", "city"]

    run1 = start_pipeline_run(session, "cdc")
    load_cdc(
        session,
        run1,
        obj,
        Batch(_changes(["c1", "c1"], ["rio", "recife"], ["I", "I"], [1, 1])),
        tmp_path,
    )
    assert read_bronze(tmp_path, obj).num_rows == 2


def test_feed_without_operation_column_is_upsert_only(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    obj.cdc_operation_column = None
    session.commit()
    plain = pa.table({"customer_id": ["c1"], "city": ["rio"], "seq": [1]})

    run = start_pipeline_run(session, "cdc")
    result = load_cdc(session, run, obj, Batch(plain), tmp_path)
    assert result.inserted == 1
    assert result.deleted == 0


def test_missing_key_column_fails_clearly(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    run = start_pipeline_run(session, "cdc")
    bad = pa.table({"city": ["rio"], "op": ["I"], "seq": [1]})
    with pytest.raises(KeyError, match="customer_id"):
        load_cdc(session, run, obj, Batch(bad), tmp_path)
    assert session.query(TaskRun).one().status == RunStatus.FAILED


def test_wrong_strategy_is_refused(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    obj.load_strategy = LoadStrategy.FULL
    session.commit()
    run = start_pipeline_run(session, "cdc")
    with pytest.raises(ValueError, match="not 'cdc'"):
        load_cdc(session, run, obj, Batch(_changes(["c1"], ["rio"], ["I"], [1])), tmp_path)


def test_collapse_on_empty_batch() -> None:
    empty = _changes([], [], [], []).slice(0, 0)
    assert collapse_changes(empty, ["customer_id"], "seq").num_rows == 0


def test_split_deletes_partitions_the_batch(obj: SourceObject) -> None:
    batch = _changes(["a", "b"], ["x", "y"], ["U", "D"], [1, 1])
    upserts, deletes = split_deletes(batch, obj)
    assert upserts.num_rows == 1
    assert deletes.num_rows == 1


def test_audit_counts_collapsed_rows(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    run = start_pipeline_run(session, "cdc")
    load_cdc(
        session, run, obj, Batch(_changes(["c1", "c1"], ["a", "b"], ["I", "U"], [1, 2])), tmp_path
    )
    task = session.query(TaskRun).one()
    assert task.rows_read == 2
    assert task.rows_rejected == 1
