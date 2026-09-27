"""The default engine: Arrow, in process, no JVM.

Everything the pipeline did before the engine seam existed. Kept as the
default because for anything that fits in memory it is faster than
Spark by the width of a JVM startup, and most things fit in memory.
"""

from __future__ import annotations

import pyarrow as pa

from lakehouse.tables import latest_per_key as arrow_latest_per_key


class ArrowEngine:
    """In-process engine backed by pyarrow compute."""

    name = "arrow"

    def latest_per_key(self, table: pa.Table, keys: list[str], sequence: str) -> pa.Table:
        return arrow_latest_per_key(table, keys, sequence)

    def close(self) -> None:
        """Nothing to release."""
        return
