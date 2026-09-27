"""The engine seam.

ADR-003 chose Arrow and delta-rs as the default engine and deferred
Spark. Deferring it was right — Spark costs a JVM, a cluster and
minutes of startup to process data that fits in memory — but "we could
use Spark later" is only true if there is a place to put it.

This is that place: one interface, two implementations, chosen by
configuration. The operation behind it is `latest_per_key`, which is
the pipeline's heaviest transform and the one that stops fitting in
memory first.

Correctness is the contract. Both engines must return the same rows for
the same input, and a test asserts exactly that rather than trusting
two implementations to agree.
"""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class EngineError(Exception):
    """Raised when an engine cannot be built or used."""


class Engine(Protocol):
    """Computes the transforms that might outgrow a single machine."""

    name: str

    def latest_per_key(self, table: pa.Table, keys: list[str], sequence: str) -> pa.Table:
        """One row per key — the newest by `sequence`.

        Ties on `sequence` are broken deterministically so that two
        engines, and two runs of one engine, agree.
        """
        ...

    def close(self) -> None:
        """Release anything the engine holds open."""
        ...
