"""Tests for SCD Type 2 history in Silver."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session

from lakehouse.ingest.bronze import load_full, start_pipeline_run
from lakehouse.metadata.enums import LoadStrategy, SourceKind
from lakehouse.metadata.models import Base, SourceObject, SourceSystem
from lakehouse.transform.scd2 import (
    IS_CURRENT,
    IS_DELETED,
    ROW_HASH,
    VALID_FROM,
    VALID_TO,
    hashable_columns,
    represents_current_state,
)
from lakehouse.transform.silver import SilverResult, build_silver, read_silver


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
        incremental_column="updated_at",
        scd2_enabled=True,
    )
    session.add(o)
    session.commit()
    return o


class Fixed:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read(self, obj: SourceObject) -> pa.Table:
        return self.table


def cycle(session: Session, obj: SourceObject, table: pa.Table, tmp_path: Path) -> SilverResult:
    """One full Bronze load followed by one Silver build."""
    bronze_run = start_pipeline_run(session, "bronze")
    load_full(session, bronze_run, obj, Fixed(table), tmp_path)
    silver_run = start_pipeline_run(session, "silver")
    return build_silver(session, silver_run, obj, tmp_path)


def customers(rows: list[tuple[str, str, int]]) -> pa.Table:
    return pa.table(
        {
            "customer_id": [r[0] for r in rows],
            "city": [r[1] for r in rows],
            "updated_at": [r[2] for r in rows],
        }
    )


def history(tmp_path: Path, obj: SourceObject) -> pa.Table:
    return read_silver(tmp_path, obj).sort_by(
        [("customer_id", "ascending"), (VALID_FROM, "ascending")]
    )


def epochs(table: pa.Table, column: str) -> list[int | None]:
    """Timestamp column as epoch microseconds.

    Reading a timezone-aware timestamp as a Python datetime needs an IANA
    timezone database, which Windows does not ship, so the assertions
    below compare integers instead.
    """
    values: list[int | None] = table.column(column).cast(pa.int64()).to_pylist()
    return values


# --------------------------------------------------------------------------
# First load
# --------------------------------------------------------------------------


def test_first_load_opens_one_version_per_key(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    result = cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)

    table = history(tmp_path, obj)
    assert table.num_rows == 2
    assert table.column(IS_CURRENT).to_pylist() == [True, True]
    assert table.column(IS_DELETED).to_pylist() == [False, False]
    assert epochs(table, VALID_TO) == [None, None]
    assert result.scd2 is not None
    assert result.scd2.opened == 2
    assert result.scd2.closed == 0


def test_valid_from_uses_business_time(session: Session, obj: SourceObject, tmp_path: Path) -> None:
    """`updated_at` is an epoch column, so it dates the version."""
    cycle(session, obj, customers([("c1", "rio", 1_700_000_000)]), tmp_path)

    assert epochs(history(tmp_path, obj), VALID_FROM) == [1_700_000_000 * 1_000_000]


def test_valid_from_falls_back_to_processing_time(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """An integer surrogate key orders rows but cannot date them."""
    obj.incremental_column = "version_no"
    session.commit()
    table = pa.table({"customer_id": ["c1"], "city": ["rio"], "version_no": [7]})
    cycle(session, obj, table, tmp_path)

    opened = epochs(history(tmp_path, obj), VALID_FROM)[0]
    assert opened is not None
    assert opened > 1_767_225_600 * 1_000_000, "dated at processing time, not epoch 7"


# --------------------------------------------------------------------------
# Change detection
# --------------------------------------------------------------------------


def test_rerunning_an_unchanged_batch_writes_nothing(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Idempotency. Provenance changes every run and must not count."""
    batch = customers([("c1", "rio", 100), ("c2", "recife", 100)])
    cycle(session, obj, batch, tmp_path)
    result = cycle(session, obj, batch, tmp_path)

    assert history(tmp_path, obj).num_rows == 2
    assert result.scd2 is not None
    assert result.scd2.opened == 0
    assert result.scd2.unchanged == 2


def test_a_changed_attribute_closes_the_old_version(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)
    cycle(session, obj, customers([("c1", "manaus", 200)]), tmp_path)

    table = history(tmp_path, obj)
    assert table.num_rows == 2
    assert table.column("city").to_pylist() == ["rio", "manaus"]
    assert table.column(IS_CURRENT).to_pylist() == [False, True]

    closed_at = epochs(table, VALID_TO)[0]
    open_at = epochs(table, VALID_FROM)[1]
    assert closed_at == open_at, "intervals must abut, leaving no gap and no overlap"


def test_three_changes_leave_one_open_row_and_a_contiguous_chain(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    for city, when in (("rio", 100), ("manaus", 200), ("recife", 300)):
        cycle(session, obj, customers([("c1", city, when)]), tmp_path)

    table = history(tmp_path, obj)
    assert table.num_rows == 3
    assert table.column(IS_CURRENT).to_pylist() == [False, False, True]

    starts = epochs(table, VALID_FROM)
    ends = epochs(table, VALID_TO)
    assert ends[:-1] == starts[1:]
    assert ends[-1] is None


def test_a_new_key_is_added_without_touching_the_others(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)
    result = cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)

    assert result.scd2 is not None
    assert result.scd2.opened == 1
    assert result.scd2.closed == 0
    assert history(tmp_path, obj).num_rows == 2


def test_out_of_order_rows_are_refused_not_backdated(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "manaus", 200)]), tmp_path)
    result = cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)

    assert result.scd2 is not None
    assert result.scd2.late_skipped == 1
    assert result.scd2.opened == 0
    table = history(tmp_path, obj)
    assert table.num_rows == 1
    assert table.column("city").to_pylist() == ["manaus"]


def test_the_hash_ignores_keys_and_provenance() -> None:
    table = pa.table(
        {
            "customer_id": ["c1"],
            "city": ["rio"],
            "ingested_at": [1],
            "run_id": ["r1"],
            "source": ["customers"],
        }
    )
    assert hashable_columns(table, ["customer_id"]) == ["city"]


# --------------------------------------------------------------------------
# Deletes
# --------------------------------------------------------------------------


def test_a_vanished_key_is_tombstoned_for_a_full_reload_source(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)
    result = cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)

    assert result.scd2 is not None
    assert result.scd2.tombstoned == 1

    c2 = history(tmp_path, obj).filter(
        pa.compute.equal(history(tmp_path, obj).column("customer_id"), "c2")
    )
    assert c2.num_rows == 2
    assert c2.column(IS_DELETED).to_pylist() == [False, True]
    assert c2.column(IS_CURRENT).to_pylist() == [False, True], (
        "the tombstone stays current so late facts still find a dimension row"
    )


def test_an_incremental_source_never_infers_a_delete(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Bronze only grows for an incremental source, so absence means nothing.

    Bronze is loaded through the full-reload path to shrink the table on
    purpose; the policy under test is the one Silver applies, which is
    read from `load_strategy` at build time.
    """
    cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)

    bronze_run = start_pipeline_run(session, "bronze")
    load_full(session, bronze_run, obj, Fixed(customers([("c1", "rio", 100)])), tmp_path)
    obj.load_strategy = LoadStrategy.INCREMENTAL
    session.commit()
    result = build_silver(session, start_pipeline_run(session, "silver"), obj, tmp_path)

    assert result.scd2 is not None
    assert result.scd2.tombstoned == 0
    assert history(tmp_path, obj).num_rows == 2


def test_a_tombstone_is_not_re_tombstoned(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)
    cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)
    result = cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)

    assert result.scd2 is not None
    assert result.scd2.tombstoned == 0
    assert history(tmp_path, obj).num_rows == 3


def test_a_resurrected_key_reopens_after_the_tombstone(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)
    cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)
    cycle(session, obj, customers([("c1", "rio", 100), ("c2", "recife", 100)]), tmp_path)

    table = history(tmp_path, obj)
    c2 = table.filter(pa.compute.equal(table.column("customer_id"), "c2"))
    assert c2.num_rows == 3
    assert c2.column(IS_DELETED).to_pylist() == [False, True, False]
    assert c2.column(IS_CURRENT).to_pylist() == [False, False, True]

    starts = epochs(c2, VALID_FROM)
    assert starts[1] is not None and starts[2] is not None
    assert starts[1] < starts[2], "the reopened version must start after the tombstone"


def test_represents_current_state() -> None:
    assert represents_current_state(LoadStrategy.FULL)
    assert represents_current_state(LoadStrategy.CDC)
    assert not represents_current_state(LoadStrategy.INCREMENTAL)


# --------------------------------------------------------------------------
# Guards
# --------------------------------------------------------------------------


def test_a_schema_change_is_refused_rather_than_half_applied(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)

    widened = pa.table(
        {
            "customer_id": ["c1"],
            "city": ["rio"],
            "state": ["RJ"],
            "updated_at": [200],
        }
    )
    with pytest.raises(ValueError, match="does not match the SCD2 history table"):
        cycle(session, obj, widened, tmp_path)


def test_composite_keys_are_tracked_independently(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    obj.primary_key_columns = "customer_id,region"
    session.commit()

    def batch(city_a: str) -> pa.Table:
        return pa.table(
            {
                "customer_id": ["c1", "c1"],
                "region": ["north", "south"],
                "city": [city_a, "recife"],
                "updated_at": [100, 100],
            }
        )

    cycle(session, obj, batch("rio"), tmp_path)
    table = pa.table(
        {
            "customer_id": ["c1", "c1"],
            "region": ["north", "south"],
            "city": ["manaus", "recife"],
            "updated_at": [200, 100],
        }
    )
    result = cycle(session, obj, table, tmp_path)

    assert result.scd2 is not None
    assert result.scd2.opened == 1
    assert result.scd2.unchanged == 1


def test_the_snapshot_path_is_unaffected(
    session: Session, obj: SourceObject, tmp_path: Path
) -> None:
    """Turning the flag off leaves step 23's behaviour exactly as it was."""
    obj.scd2_enabled = False
    session.commit()

    result = cycle(session, obj, customers([("c1", "rio", 100)]), tmp_path)

    assert result.scd2 is None
    assert ROW_HASH not in read_silver(tmp_path, obj).column_names
