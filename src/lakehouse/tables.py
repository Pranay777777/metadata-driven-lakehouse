"""Arrow table helpers shared across layers.

`latest_per_key` is used in two places for two reasons that look the
same but are not: CDC collapses several changes for one key into the
newest before merging, and Silver removes redelivered or re-read
duplicates. Same operation, so it lives here once.
"""

from __future__ import annotations

import re

import pyarrow as pa
import pyarrow.compute as pc

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")


def require_columns(table: pa.Table, columns: list[str], context: str) -> None:
    """Raise a clear error naming every missing column at once.

    Reporting them one at a time turns a config mismatch into several
    round trips.
    """
    missing = [c for c in columns if c not in table.column_names]
    if missing:
        raise KeyError(f"{context} is missing required column(s): {', '.join(missing)}")


def latest_per_key(table: pa.Table, keys: list[str], sequence_column: str) -> pa.Table:
    """Keep one row per key — the one with the highest sequence value.

    Ties are broken by original position, so the result is deterministic
    for a given input even when the sequence column repeats.
    """
    require_columns(table, [*keys, sequence_column], "table")
    if table.num_rows == 0:
        return table

    ordered = table.sort_by([*[(k, "ascending") for k in keys], (sequence_column, "descending")])
    columns = [ordered.column(k).to_pylist() for k in keys]
    seen: set[tuple[object, ...]] = set()
    keep: list[int] = []
    for i in range(ordered.num_rows):
        composite = tuple(col[i] for col in columns)
        if composite not in seen:
            seen.add(composite)
            keep.append(i)
    return ordered.take(pa.array(keep))


def to_snake_case(name: str) -> str:
    """Normalise a column name.

    `OrderID` becomes `order_id`, `Customer Name` becomes
    `customer_name`. Sources disagree about casing; downstream queries
    should not have to.
    """
    spaced = _CAMEL_BOUNDARY.sub("_", name)
    cleaned = _NON_ALNUM.sub("_", spaced).strip("_")
    return cleaned.lower()


def rename_snake_case(table: pa.Table) -> pa.Table:
    """Apply `to_snake_case` to every column name.

    Raises:
        ValueError: if two columns normalise to the same name, which
            would silently drop one of them.
    """
    names = [to_snake_case(c) for c in table.column_names]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise ValueError(
            f"column names collide after normalisation: {', '.join(sorted(duplicates))}"
        )
    return table.rename_columns(names)


def trim_strings(table: pa.Table) -> pa.Table:
    """Strip leading and trailing whitespace from every string column.

    Trailing spaces are invisible and break joins and grouping, which
    makes them one of the more expensive kinds of dirty data.
    """
    for i, field in enumerate(table.schema):
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            table = table.set_column(i, field.name, pc.utf8_trim_whitespace(table.column(i)))
    return table


def epoch_to_timestamp(table: pa.Table, columns: list[str], unit: str = "s") -> pa.Table:
    """Convert integer epoch columns into UTC timestamps.

    Sources hand over epoch integers constantly. Leaving them as integers
    means every downstream query re-implements the conversion, usually
    without a timezone.
    """
    for name in columns:
        if name not in table.column_names:
            continue
        i = table.column_names.index(name)
        column = table.column(i)
        if not pa.types.is_integer(column.type):
            continue
        converted = column.cast(pa.timestamp(unit)).cast(pa.timestamp("us", tz="UTC"))
        table = table.set_column(i, name, converted)
    return table
