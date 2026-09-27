"""Pluggable compute engines.

`get_engine` is the only thing the pipeline calls. Which engine it
returns is configuration, so switching is an environment variable
rather than a code change — which is the whole point of ADR-014.
"""

from __future__ import annotations

from lakehouse.config import Settings
from lakehouse.engines.arrow import ArrowEngine
from lakehouse.engines.base import Engine, EngineError

ENGINES = ("arrow", "spark")


def get_engine(name: str | None = None) -> Engine:
    """Build the configured engine.

    Spark is imported only when asked for, so the default path never
    touches PySpark or needs a JVM.
    """
    chosen = name or Settings().engine
    if chosen == "arrow":
        return ArrowEngine()
    if chosen == "spark":
        from lakehouse.engines.spark import SparkEngine

        return SparkEngine()
    raise EngineError(f"unknown engine '{chosen}' — expected one of {', '.join(ENGINES)}")


__all__ = ["ENGINES", "ArrowEngine", "Engine", "EngineError", "get_engine"]
