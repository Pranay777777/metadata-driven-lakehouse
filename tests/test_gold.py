"""Tests for the Gold star schema."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.enums import GoldRole, Layer, LoadStrategy, RunStatus, SourceKind
from lakehouse.metadata.models import (
    Base,
    GoldReference,
    SourceObject,
    SourceSystem,
    TaskRun,
)
from lakehouse.transform.gold import (
    UNKNOWN_KEY,
    build_fact,
    build_gold,
    gold_path,
    natural_keys,
    read_gold,
    surrogate,
    surrogate_column,
)
from lakehouse.transform.silver import build_silver


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
def system(session: Session) -> SourceSystem:
    s = SourceSystem(name="seed", kind=SourceKind.FILE)
    session.add(s)
    session.commit()
    return s


def make_object(
    session: Session,
    system: SourceSystem,
    name: str,
    *,
    role: str | None,
    keys: str,
    scd2: bool = False,
) -> SourceObject:
    obj = SourceObject(
        source_system_id=system.id,
        schema_name="public",
        object_name=name,
        target_path=f"bronze/seed/{name}",
        load_strategy=LoadStrategy.FULL,
        primary_key_columns=keys,
        incremental_column="updated_at",
        scd2_enabled=scd2,
        gold_role=role,
    )
    session.add(obj)
    session.commit()
    return obj


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def to_silver(session: Session, obj: SourceObject, table: pa.Table, tmp_path: Path) -> None:
    load_full(session, start_pipeline_run(session, "bronze"), obj, Fixed(table), tmp_path)
    build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)


@pytest.fixture
def customers(session: Session, system: SourceSystem) -> SourceObject:
    return make_object(session, system, "customers", role=GoldRole.DIMENSION, keys="customer_id")


@pytest.fixture
def orders(session: Session, system: SourceSystem) -> SourceObject:
    return make_object(session, system, "orders", role=GoldRole.FACT, keys="order_id")


def link(session: Session, fact: SourceObject, dim: SourceObject, column: str) -> None:
    session.add(
        GoldReference(fact_object_id=fact.id, dimension_object_id=dim.id, fact_column=column)
    )
    session.commit()


CUSTOMERS = pa.table(
    {"customer_id": ["c1", "c2"], "city": ["rio", "recife"], "updated_at": [100, 100]}
)


def orders_table(customer_ids: list[str], when: list[int]) -> pa.Table:
    return pa.table(
        {
            "order_id": [f"o{i}" for i in range(len(customer_ids))],
            "customer_id": customer_ids,
            "amount": [10.0] * len(customer_ids),
            "updated_at": when,
        }
    )


# --------------------------------------------------------------------------
# Paths and keys
# --------------------------------------------------------------------------


def test_gold_paths_are_prefixed_by_role(customers: SourceObject, orders: SourceObject) -> None:
    assert gold_path(customers) == "gold/dim_customers"
    assert gold_path(orders) == "gold/fact_orders"


def test_surrogates_are_deterministic_and_never_zero() -> None:
    assert surrogate(("c1",)) == surrogate(("c1",))
    assert surrogate(("c1",)) != surrogate(("c2",))
    assert surrogate(("c1",)) > UNKNOWN_KEY
    assert surrogate((None,)) != surrogate(("",)), "null must not collide with empty string"


def test_an_object_without_a_role_is_refused(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    staging = make_object(session, system, "staging", role=None, keys="id")
    with pytest.raises(ValueError, match="no gold_role"):
        build_gold(session, start_pipeline_run(session, "gold"), staging, tmp_path)


# --------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------


def test_a_dimension_gets_a_surrogate_and_an_unknown_member(
    session: Session, customers: SourceObject, tmp_path: Path
) -> None:
    to_silver(session, customers, CUSTOMERS, tmp_path)
    result = build_gold(session, start_pipeline_run(session, "gold"), customers, tmp_path)

    dim = read_gold(tmp_path, customers)
    assert result.rows_read == 2
    assert dim.num_rows == 3, "two members plus the unknown member"

    keys = dim.column("customers_key").to_pylist()
    assert UNKNOWN_KEY in keys
    assert len(set(keys)) == 3

    unknown = dim.filter(pa.compute.equal(dim.column("customers_key"), UNKNOWN_KEY))
    assert unknown.column("city").to_pylist() == [None]


def test_rebuilding_a_dimension_keeps_the_same_keys(
    session: Session, customers: SourceObject, tmp_path: Path
) -> None:
    """Gold is rebuilt every run, so surrogates must be reproducible."""
    to_silver(session, customers, CUSTOMERS, tmp_path)
    build_gold(session, start_pipeline_run(session, "gold"), customers, tmp_path)
    first = sorted(read_gold(tmp_path, customers).column("customers_key").to_pylist())

    to_silver(session, customers, CUSTOMERS, tmp_path)
    build_gold(session, start_pipeline_run(session, "gold"), customers, tmp_path)
    assert sorted(read_gold(tmp_path, customers).column("customers_key").to_pylist()) == first


def test_each_scd2_version_gets_its_own_surrogate(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    dim = make_object(
        session, system, "customers", role=GoldRole.DIMENSION, keys="customer_id", scd2=True
    )
    to_silver(
        session,
        dim,
        pa.table({"customer_id": ["c1"], "city": ["rio"], "updated_at": [100]}),
        tmp_path,
    )
    to_silver(
        session,
        dim,
        pa.table({"customer_id": ["c1"], "city": ["manaus"], "updated_at": [200]}),
        tmp_path,
    )
    build_gold(session, start_pipeline_run(session, "gold"), dim, tmp_path)

    published = read_gold(tmp_path, dim)
    keys = [k for k in published.column("customers_key").to_pylist() if k != UNKNOWN_KEY]
    assert len(keys) == 2
    assert len(set(keys)) == 2, "two versions of one member are two distinct rows"


def test_the_database_refuses_a_dimension_without_a_primary_key(
    session: Session, system: SourceSystem
) -> None:
    """A dimension with no natural key has nothing to hash a surrogate from."""
    obj = make_object(session, system, "customers", role=GoldRole.DIMENSION, keys="customer_id")
    obj.primary_key_columns = None
    with pytest.raises(IntegrityError, match="ck_dimension_needs_primary_key"):
        session.commit()
    session.rollback()


def test_natural_keys_refuses_an_unkeyed_object(session: Session, system: SourceSystem) -> None:
    obj = make_object(session, system, "staging", role=None, keys="id")
    obj.primary_key_columns = None
    session.commit()
    with pytest.raises(ValueError, match="needs primary_key_columns"):
        natural_keys(obj)


# --------------------------------------------------------------------------
# Facts
# --------------------------------------------------------------------------


def test_a_fact_swaps_the_natural_key_for_a_surrogate(
    session: Session, customers: SourceObject, orders: SourceObject, tmp_path: Path
) -> None:
    to_silver(session, customers, CUSTOMERS, tmp_path)
    to_silver(session, orders, orders_table(["c1", "c2"], [150, 150]), tmp_path)
    link(session, orders, customers, "customer_id")

    build_gold(session, start_pipeline_run(session, "gold"), customers, tmp_path)
    result = build_gold(session, start_pipeline_run(session, "gold"), orders, tmp_path)

    fact = read_gold(tmp_path, orders)
    assert "customer_id" not in fact.column_names
    assert "customers_key" in fact.column_names
    assert result.unresolved_total == 0
    assert set(fact.column("customers_key").to_pylist()) == {
        surrogate(("c1",)),
        surrogate(("c2",)),
    }


def test_an_unmatched_member_falls_back_to_the_unknown_key(
    session: Session, customers: SourceObject, orders: SourceObject, tmp_path: Path
) -> None:
    """The fact row survives and the gap is countable."""
    to_silver(session, customers, CUSTOMERS, tmp_path)
    to_silver(session, orders, orders_table(["c1", "c99"], [150, 150]), tmp_path)
    link(session, orders, customers, "customer_id")

    build_gold(session, start_pipeline_run(session, "gold"), customers, tmp_path)
    result = build_gold(session, start_pipeline_run(session, "gold"), orders, tmp_path)

    fact = read_gold(tmp_path, orders)
    assert fact.num_rows == 2, "the orphan row is kept, not dropped"
    assert UNKNOWN_KEY in fact.column("customers_key").to_pylist()
    assert result.unresolved == {"customer_id": 1}


def test_a_fact_joins_the_scd2_version_that_was_valid_at_event_time(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    dim = make_object(
        session, system, "customers", role=GoldRole.DIMENSION, keys="customer_id", scd2=True
    )
    to_silver(
        session,
        dim,
        pa.table({"customer_id": ["c1"], "city": ["rio"], "updated_at": [100]}),
        tmp_path,
    )
    to_silver(
        session,
        dim,
        pa.table({"customer_id": ["c1"], "city": ["manaus"], "updated_at": [300]}),
        tmp_path,
    )

    fact = make_object(session, system, "orders", role=GoldRole.FACT, keys="order_id")
    to_silver(session, fact, orders_table(["c1"], [200]), tmp_path)
    link(session, fact, dim, "customer_id")

    build_gold(session, start_pipeline_run(session, "gold"), dim, tmp_path)
    build_gold(session, start_pipeline_run(session, "gold"), fact, tmp_path)

    published = read_gold(tmp_path, fact)
    resolved = published.column("customers_key").to_pylist()[0]

    dimension = read_gold(tmp_path, dim)
    matched = dimension.filter(pa.compute.equal(dimension.column("customers_key"), resolved))
    assert matched.column("city").to_pylist() == ["rio"], (
        "an order placed at t=200 belongs to the version that was current then"
    )


def test_a_fact_older_than_every_dimension_version_is_unknown(
    session: Session, system: SourceSystem, tmp_path: Path
) -> None:
    dim = make_object(
        session, system, "customers", role=GoldRole.DIMENSION, keys="customer_id", scd2=True
    )
    to_silver(
        session,
        dim,
        pa.table({"customer_id": ["c1"], "city": ["rio"], "updated_at": [500]}),
        tmp_path,
    )

    fact = make_object(session, system, "orders", role=GoldRole.FACT, keys="order_id")
    to_silver(session, fact, orders_table(["c1"], [100]), tmp_path)
    link(session, fact, dim, "customer_id")

    build_gold(session, start_pipeline_run(session, "gold"), dim, tmp_path)
    result = build_gold(session, start_pipeline_run(session, "gold"), fact, tmp_path)
    assert result.unresolved == {"customer_id": 1}


def test_a_fact_with_no_references_is_published_unchanged(
    session: Session, orders: SourceObject, tmp_path: Path
) -> None:
    to_silver(session, orders, orders_table(["c1"], [100]), tmp_path)
    result = build_gold(session, start_pipeline_run(session, "gold"), orders, tmp_path)

    assert result.rows_written == 1
    assert "customer_id" in read_gold(tmp_path, orders).column_names


def test_a_missing_fact_column_names_itself(
    session: Session, customers: SourceObject, orders: SourceObject, tmp_path: Path
) -> None:
    to_silver(session, customers, CUSTOMERS, tmp_path)
    to_silver(session, orders, orders_table(["c1"], [100]), tmp_path)
    link(session, orders, customers, "buyer_id")

    build_gold(session, start_pipeline_run(session, "gold"), customers, tmp_path)
    with pytest.raises(KeyError, match="buyer_id"):
        build_fact(session, start_pipeline_run(session, "gold"), orders, tmp_path)


def test_the_gold_build_is_audited(
    session: Session, customers: SourceObject, tmp_path: Path
) -> None:
    to_silver(session, customers, CUSTOMERS, tmp_path)
    run = start_pipeline_run(session, "gold")
    build_gold(session, run, customers, tmp_path)

    task = session.query(TaskRun).filter(TaskRun.run_id == run.run_id).one()
    assert task.layer == Layer.GOLD
    assert task.status == RunStatus.SUCCEEDED
    assert task.rows_written == 3


def test_a_failed_gold_build_is_recorded(
    session: Session, customers: SourceObject, tmp_path: Path
) -> None:
    run = start_pipeline_run(session, "gold")
    with pytest.raises(Exception):
        build_gold(session, run, customers, tmp_path)

    task = session.query(TaskRun).filter(TaskRun.run_id == run.run_id).one()
    assert task.status == RunStatus.FAILED
    assert task.error_message is not None


def test_surrogate_column_names_follow_the_object(customers: SourceObject) -> None:
    assert surrogate_column(customers) == "customers_key"
