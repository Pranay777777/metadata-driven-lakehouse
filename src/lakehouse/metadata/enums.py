"""Controlled vocabularies for the metadata control plane.

These are stored as strings in the database rather than native enum types,
because Azure SQL and PostgreSQL disagree on enum support and migrating a
native enum is painful. A CHECK constraint gives the same safety and is
portable.
"""

from __future__ import annotations

from enum import StrEnum


class LoadStrategy(StrEnum):
    """How a source object is pulled into Bronze."""

    FULL = "full"
    """Truncate and reload. Correct for small dimensions and lookup tables."""

    INCREMENTAL = "incremental"
    """Pull rows above a stored watermark. Requires `incremental_column`."""

    CDC = "cdc"
    """Consume a change feed and MERGE. Requires `primary_key_columns`."""


class Layer(StrEnum):
    """Medallion layer."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class WatermarkType(StrEnum):
    """Type of the stored watermark value.

    Stored alongside the value because watermarks are persisted as text to
    keep one column portable across timestamp, integer and string keys.
    """

    TIMESTAMP = "timestamp"
    INTEGER = "integer"
    STRING = "string"


class RunStatus(StrEnum):
    """Lifecycle of a pipeline or task run."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"
    """Completed, but rows were rejected by a quarantine-severity rule."""


class Severity(StrEnum):
    """What a data-quality rule does when it fails.

    The three tiers exist because not every quality problem should stop a
    pipeline, and not every one should be ignored.
    """

    WARN = "warn"
    """Record the failure, load everything, carry on."""

    QUARANTINE = "quarantine"
    """Divert failing rows to the quarantine table, load the rest."""

    FAIL = "fail"
    """Abort the task. Nothing is written."""


class RuleType(StrEnum):
    """Supported data-quality checks."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    ALLOWED_VALUES = "allowed_values"
    REGEX = "regex"
    FRESHNESS = "freshness"
    ROW_COUNT = "row_count"
    CUSTOM_SQL = "custom_sql"


class GoldRole(StrEnum):
    """How an object is published into the Gold star schema.

    An object with no role is not published at all. Most Bronze objects
    are staging or reference data that no analyst should query.
    """

    FACT = "fact"
    """Measurements at a grain. Carries surrogate keys, not natural ones."""

    DIMENSION = "dimension"
    """Descriptive context, keyed by a surrogate so history can be joined."""


class Sensitivity(StrEnum):
    """PII classification, used to drive masking in Silver."""

    NONE = "none"
    INTERNAL = "internal"
    PII = "pii"
    """Hashed or tokenised on the way into Silver."""

    SENSITIVE_PII = "sensitive_pii"
    """Hashed, and excluded from Gold unless explicitly allow-listed."""


class MaskingStrategy(StrEnum):
    """What Silver does to a classified column's values.

    Which strategy a column gets is configuration, not code: the same
    masking runs for every source, and reclassifying a column is an
    UPDATE against `column_metadata`.
    """

    NONE = "none"
    """Leave the value alone. Correct for `internal` columns, which are
    quasi-identifiers worth labelling but not worth destroying."""

    HASH = "hash"
    """Replace with a keyed HMAC of the value. Equal inputs produce equal
    outputs, so masked columns still join and still deduplicate — the
    property that makes masking survivable in a warehouse."""

    REDACT = "redact"
    """Replace with null. The only strategy that works on any type, and
    the only one that removes the value outright."""

    PARTIAL = "partial"
    """Keep the last four characters, mask the rest. Enough to recognise
    a record in a support conversation, not enough to identify a person."""


class SourceKind(StrEnum):
    """Transport used to reach a source system."""

    JDBC = "jdbc"
    REST = "rest"
    FILE = "file"
    OBJECT_STORE = "object_store"
