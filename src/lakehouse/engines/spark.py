"""The Spark engine.

Same contract as the Arrow engine, executed on a Spark session. It
exists so the claim in ADR-003 — that Spark is a later option rather
than a rewrite — is demonstrably true rather than aspirational.

Two things are deliberate and worth reading before judging the design.

**Data crosses the boundary as Arrow, in both directions.** Spark 4
accepts a `pyarrow.Table` in `createDataFrame` and returns one from
`toArrow`, so no pandas, no temporary files and no Hadoop filesystem
are involved.

An earlier version round-tripped through Parquet on the theory that
files are how Spark really receives data. That was wrong twice over: it
bought nothing for an in-memory signature, and Spark's Parquet *writer*
goes through Hadoop's output committer, which needs `winutils.exe` on
Windows and fails without it. Removing the write removed a real
portability bug, not just a step.

**The in-memory signature is the honest limitation.** `latest_per_key`
takes and returns an Arrow table, so the result still has to fit on one
machine even though the computation did not. That is enough to prove
the seam and to benchmark the two engines against each other; it is not
enough to process more data than the driver can hold. Getting there
means the engine reading and writing Delta directly through
`delta-spark`, which is a bigger change and is named as such in
ADR-014 rather than glossed.

PySpark is imported lazily. Nothing about this module may make the
default path require a JVM.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from lakehouse.config import Settings
from lakehouse.engines.base import EngineError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

DEFAULT_MASTER = "local[*]"


def build_session(app_name: str = "lakehouse", master: str | None = None) -> SparkSession:
    """Create a local Spark session, or explain why it could not be.

    Raises:
        EngineError: if PySpark is not installed or no JVM is present.
            Both are ordinary on a machine that only runs the default
            engine, so the message says what to install rather than
            surfacing a stack trace from deep inside py4j.
    """
    settings = Settings()
    # Driver heap must be set before the JVM starts, and in local mode
    # `spark.driver.memory` in the builder is read too late to matter.
    # The environment variable is read by the launcher, so it works.
    os.environ.setdefault("SPARK_DRIVER_MEMORY", settings.spark_driver_memory)

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise EngineError(
            'the spark engine needs pyspark — install it with pip install -e ".[spark]"'
        ) from exc

    try:
        return (
            SparkSession.builder.appName(app_name)
            .master(master or settings.spark_master)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
            # Collecting a large result back through `toArrow` trips the
            # 1g default long before the driver heap runs out.
            .config("spark.driver.maxResultSize", settings.spark_driver_memory)
            .getOrCreate()
        )
    except Exception as exc:
        raise EngineError(
            "could not start Spark — this usually means no JVM is installed. "
            "Spark 4 needs Java 17 or later."
        ) from exc


class SparkEngine:
    """Engine backed by a Spark session.

    The session is created on first use and reused, because starting
    one costs seconds and creating a second one in the same process is
    not possible anyway.
    """

    name = "spark"

    def __init__(self, master: str | None = None) -> None:
        self.master = master
        self._session: SparkSession | None = None

    @property
    def session(self) -> SparkSession:
        if self._session is None:
            self._session = build_session(master=self.master)
            self._session.sparkContext.setLogLevel("ERROR")
        return self._session

    def latest_per_key(self, table: pa.Table, keys: list[str], sequence: str) -> pa.Table:
        """One row per key, newest by `sequence`, computed on Spark.

        Ties break on every remaining column in a fixed order, matching
        the Arrow engine, so the two agree on inputs where `sequence`
        alone does not decide.
        """
        missing = [c for c in [*keys, sequence] if c not in table.column_names]
        if missing:
            raise EngineError(f"missing required column(s): {', '.join(missing)}")
        if table.num_rows == 0:
            return table

        from pyspark.sql import Window

        # N812: `F` is the universal convention for this import across
        # every Spark codebase and every piece of Spark documentation.
        from pyspark.sql import functions as F  # noqa: N812

        frame = self.session.createDataFrame(table)
        tiebreak = [c for c in table.column_names if c not in {*keys, sequence}]
        ordering = [F.col(sequence).desc(), *[F.col(c).desc_nulls_last() for c in tiebreak]]
        window = Window.partitionBy(*keys).orderBy(*ordering)

        deduped = (
            frame.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        result: pa.Table = deduped.toArrow()
        # Downstream code positions columns by name, and a window
        # operation is not obliged to preserve input order.
        return result.select(list(table.column_names))

    def close(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None


def spark_available() -> bool:
    """Whether a Spark session could actually be started here.

    Used by tests to skip rather than fail on a machine with no JVM,
    which is the normal state of a machine running the default engine.
    """
    try:
        session: Any = build_session(app_name="lakehouse-probe")
    except EngineError:
        return False
    else:
        session.stop()
        return True
