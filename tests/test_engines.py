"""Tests for the pluggable engines.

The Spark tests skip rather than fail when PySpark or a JVM is absent,
because a machine running only the default engine is a supported and
ordinary configuration.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from lakehouse.engines import ArrowEngine, EngineError, get_engine

pyspark = pytest.importorskip("pyspark", reason="the spark engine is an optional extra")

from lakehouse.engines.spark import SparkEngine, spark_available  # noqa: E402

requires_jvm = pytest.mark.skipif(
    not spark_available(), reason="no JVM available — Spark 4 needs Java 17 or later"
)


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkEngine]:
    engine = SparkEngine()
    yield engine
    engine.close()


def sample() -> pa.Table:
    """Two keys, several versions, one tie on the sequence column."""
    return pa.table(
        {
            "customer_id": ["c1", "c1", "c2", "c2", "c3"],
            "city": ["rio", "manaus", "recife", "recife", "belem"],
            "updated_at": [1, 2, 5, 5, 3],
        }
    )


def sorted_rows(table: pa.Table) -> list[tuple[object, ...]]:
    columns = [table.column(c).to_pylist() for c in sorted(table.column_names)]
    return sorted(tuple(col[i] for col in columns) for i in range(table.num_rows))


# --------------------------------------------------------------------------
# Selection
# --------------------------------------------------------------------------


def test_the_default_engine_is_arrow(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default path must never need a JVM."""
    monkeypatch.delenv("ENGINE", raising=False)
    assert get_engine().name == "arrow"


def test_an_engine_can_be_chosen_by_name() -> None:
    assert get_engine("arrow").name == "arrow"


def test_the_engine_comes_from_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENGINE", "arrow")
    assert get_engine().name == "arrow"


def test_an_unknown_engine_is_refused() -> None:
    with pytest.raises(EngineError, match="unknown engine"):
        get_engine("teradata")


def test_asking_for_arrow_does_not_import_pyspark(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lazy import: the default install has no pyspark at all."""
    import sys

    monkeypatch.setitem(sys.modules, "pyspark", None)
    assert get_engine("arrow").name == "arrow"


# --------------------------------------------------------------------------
# The Arrow engine
# --------------------------------------------------------------------------


def test_arrow_keeps_the_newest_row_per_key() -> None:
    result = ArrowEngine().latest_per_key(sample(), ["customer_id"], "updated_at")

    assert result.num_rows == 3
    cities = dict(
        zip(
            result.column("customer_id").to_pylist(),
            result.column("city").to_pylist(),
            strict=True,
        )
    )
    assert cities["c1"] == "manaus"


def test_arrow_closes_without_complaint() -> None:
    ArrowEngine().close()


# --------------------------------------------------------------------------
# The Spark engine
# --------------------------------------------------------------------------


@requires_jvm
def test_spark_keeps_the_newest_row_per_key(spark: SparkEngine) -> None:
    result = spark.latest_per_key(sample(), ["customer_id"], "updated_at")

    assert result.num_rows == 3
    cities = dict(
        zip(
            result.column("customer_id").to_pylist(),
            result.column("city").to_pylist(),
            strict=True,
        )
    )
    assert cities["c1"] == "manaus"


@requires_jvm
def test_the_two_engines_agree(spark: SparkEngine) -> None:
    """The contract. Two implementations are only useful if identical."""
    table = sample()
    assert sorted_rows(spark.latest_per_key(table, ["customer_id"], "updated_at")) == sorted_rows(
        ArrowEngine().latest_per_key(table, ["customer_id"], "updated_at")
    )


@requires_jvm
def test_the_two_engines_agree_on_composite_keys(spark: SparkEngine) -> None:
    table = pa.table(
        {
            "a": ["x", "x", "y"],
            "b": [1, 1, 2],
            "v": ["old", "new", "only"],
            "seq": [1, 2, 1],
        }
    )
    assert sorted_rows(spark.latest_per_key(table, ["a", "b"], "seq")) == sorted_rows(
        ArrowEngine().latest_per_key(table, ["a", "b"], "seq")
    )


@requires_jvm
def test_spark_preserves_column_order(spark: SparkEngine) -> None:
    """Downstream code positions columns by name; a reorder breaks it."""
    table = sample()
    result = spark.latest_per_key(table, ["customer_id"], "updated_at")
    assert result.column_names == list(table.column_names)


@requires_jvm
def test_spark_handles_an_empty_batch(spark: SparkEngine) -> None:
    empty = sample().slice(0, 0)
    assert spark.latest_per_key(empty, ["customer_id"], "updated_at").num_rows == 0


@requires_jvm
def test_spark_names_a_missing_column(spark: SparkEngine) -> None:
    with pytest.raises(EngineError, match="missing required column"):
        spark.latest_per_key(sample(), ["nope"], "updated_at")


@requires_jvm
def test_the_session_is_reused(spark: SparkEngine) -> None:
    """Starting a session costs seconds; doing it per call would dominate."""
    assert isinstance(spark, SparkEngine)
    first = spark.session
    assert spark.session is first
