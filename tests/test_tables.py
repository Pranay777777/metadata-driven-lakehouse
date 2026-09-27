"""Tests for the shared Arrow helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from lakehouse.tables import (
    epoch_to_timestamp,
    latest_per_key,
    rename_snake_case,
    require_columns,
    to_snake_case,
    trim_strings,
)


def test_latest_per_key_keeps_the_newest() -> None:
    table = pa.table({"k": ["a", "a", "b"], "seq": [1, 5, 2], "v": ["old", "new", "only"]})
    out = latest_per_key(table, ["k"], "seq").sort_by("k")
    assert out.column("v").to_pylist() == ["new", "only"]


def test_latest_per_key_with_composite_key() -> None:
    table = pa.table({"a": ["x", "x"], "b": [1, 2], "seq": [1, 1], "v": ["p", "q"]})
    assert latest_per_key(table, ["a", "b"], "seq").num_rows == 2


def test_latest_per_key_on_empty_table() -> None:
    empty = pa.table({"k": [], "seq": []})
    assert latest_per_key(empty, ["k"], "seq").num_rows == 0


def test_latest_per_key_requires_its_columns() -> None:
    with pytest.raises(KeyError, match="seq"):
        latest_per_key(pa.table({"k": ["a"]}), ["k"], "seq")


def test_require_columns_names_all_missing_at_once() -> None:
    with pytest.raises(KeyError) as err:
        require_columns(pa.table({"a": [1]}), ["b", "c"], "widget")
    assert "b" in str(err.value)
    assert "c" in str(err.value)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("OrderID", "order_id"),
        ("Customer Name", "customer_name"),
        ("already_snake", "already_snake"),
        ("  Weird--Name  ", "weird_name"),
        ("UPPER", "upper"),
    ],
)
def test_to_snake_case(raw: str, expected: str) -> None:
    assert to_snake_case(raw) == expected


def test_rename_snake_case_renames_every_column() -> None:
    table = pa.table({"OrderID": [1], "Customer Name": ["x"]})
    assert rename_snake_case(table).column_names == ["order_id", "customer_name"]


def test_rename_refuses_to_silently_collide() -> None:
    """Two columns normalising to one name would drop data."""
    table = pa.table({"order_id": [1], "OrderID": [2]})
    with pytest.raises(ValueError, match="collide"):
        rename_snake_case(table)


def test_trim_strings_strips_padding() -> None:
    table = pa.table({"city": ["  rio  ", "recife"], "n": [1, 2]})
    assert trim_strings(table).column("city").to_pylist() == ["rio", "recife"]


def test_trim_strings_leaves_other_types_alone() -> None:
    table = pa.table({"n": [1, 2]})
    assert trim_strings(table).column("n").to_pylist() == [1, 2]


def test_epoch_to_timestamp_converts_and_tags_utc() -> None:
    table = pa.table({"updated_at": [0, 86_400]})
    out = epoch_to_timestamp(table, ["updated_at"])
    assert pa.types.is_timestamp(out.column("updated_at").type)
    assert out.column("updated_at").type.tz == "UTC"


def test_epoch_to_timestamp_ignores_unknown_and_non_integer_columns() -> None:
    table = pa.table({"name": ["x"], "updated_at": [0]})
    out = epoch_to_timestamp(table, ["name", "missing", "updated_at"])
    assert out.column("name").to_pylist() == ["x"]
