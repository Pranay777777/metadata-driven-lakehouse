"""The metadata control plane.

Every table in this module is configuration or audit — none of it holds
business data. Onboarding a new source means inserting rows here, not
writing a new pipeline.

These models are the single source of truth for the schema. The DDL for
PostgreSQL (local stack) and Azure SQL (cloud) is generated from them by
`scripts/generate_ddl.py`, so the two dialects cannot drift apart.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from lakehouse.metadata.enums import (
    Layer,
    LoadStrategy,
    MaskingStrategy,
    RuleType,
    RunStatus,
    Sensitivity,
    Severity,
    SourceKind,
    WatermarkType,
)


def _check(column: str, enum: type[object]) -> CheckConstraint:
    """Build a portable CHECK constraint restricting a column to enum values."""
    members = ", ".join(f"'{m.value}'" for m in enum)  # type: ignore[attr-defined]
    return CheckConstraint(f"{column} IN ({members})", name=f"ck_{column}")


class Base(DeclarativeBase):
    """Declarative base for all control-plane tables."""


class TimestampMixin:
    """Audit columns present on every configuration table."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


class SourceSystem(TimestampMixin, Base):
    """A system data is pulled from.

    Credentials are never stored here — only the *name* of the secret to
    look up in Key Vault (or the local .env). The control plane must be
    safe to read by anyone who can query it.
    """

    __tablename__ = "source_system"
    __table_args__ = (_check("kind", SourceKind),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    secret_name: Mapped[str | None] = mapped_column(String(200))
    """Key Vault secret name holding the connection string. Never the value."""
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    objects: Mapped[list[SourceObject]] = relationship(
        back_populates="system", passive_deletes=True
    )


class SourceObject(TimestampMixin, Base):
    """One table, file or endpoint to ingest — the core config row.

    Adding a source to the platform is an INSERT into this table. Nothing
    else needs to change.
    """

    __tablename__ = "source_object"
    __table_args__ = (
        UniqueConstraint("source_system_id", "schema_name", "object_name", name="uq_source_object"),
        _check("load_strategy", LoadStrategy),
        CheckConstraint(
            "load_strategy <> 'incremental' OR incremental_column IS NOT NULL",
            name="ck_incremental_needs_column",
        ),
        CheckConstraint(
            "load_strategy <> 'cdc' OR primary_key_columns IS NOT NULL",
            name="ck_cdc_needs_primary_key",
        ),
        CheckConstraint(
            "gold_role IS NULL OR gold_role IN ('fact', 'dimension')",
            name="ck_gold_role",
        ),
        CheckConstraint(
            "gold_role <> 'dimension' OR primary_key_columns IS NOT NULL",
            name="ck_dimension_needs_primary_key",
        ),
        Index("ix_source_object_active_order", "active", "load_order"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_system_id: Mapped[int] = mapped_column(
        ForeignKey("source_system.id", ondelete="CASCADE"), nullable=False
    )
    schema_name: Mapped[str] = mapped_column(String(100), nullable=False)
    object_name: Mapped[str] = mapped_column(String(200), nullable=False)
    target_path: Mapped[str] = mapped_column(String(400), nullable=False)
    """Relative path under the lake root, e.g. 'bronze/olist/orders'."""

    load_strategy: Mapped[str] = mapped_column(String(20), nullable=False)
    incremental_column: Mapped[str | None] = mapped_column(String(100))
    primary_key_columns: Mapped[str | None] = mapped_column(String(400))
    """Comma-separated. Required for CDC merges and SCD2 in Silver."""

    cdc_operation_column: Mapped[str | None] = mapped_column(String(100))
    """Column in the change feed holding the operation code. Null means the
    feed carries only upserts and never signals a delete."""

    cdc_delete_value: Mapped[str] = mapped_column(String(20), default="D", nullable=False)
    """Value of `cdc_operation_column` that means "this row was deleted".
    Feeds disagree — 'D', 'delete', '3' — so it is configuration."""

    watermark_grace: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    """How far below the stored watermark to re-read, so late-arriving rows
    are not missed. Expressed in the watermark's own units: seconds for a
    timestamp watermark, raw units for an integer key. Zero disables it."""

    load_order: Mapped[int] = mapped_column(Integer, default=100, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    scd2_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    """Track history in Silver with valid_from / valid_to / is_current."""

    gold_role: Mapped[str | None] = mapped_column(String(20))
    """Published to Gold as a fact or a dimension. NULL means not published —
    most objects are staging or reference data no analyst should query."""

    freshness_sla_minutes: Mapped[int | None] = mapped_column(Integer)
    """Alert if the newest row is older than this. NULL disables the check."""

    owner: Mapped[str | None] = mapped_column(String(200))
    """Who to contact when this source breaks. Unowned data rots."""

    system: Mapped[SourceSystem] = relationship(back_populates="objects")
    columns: Mapped[list[ColumnMetadata]] = relationship(
        back_populates="source_object", passive_deletes=True
    )
    rules: Mapped[list[DataQualityRule]] = relationship(
        back_populates="source_object", passive_deletes=True
    )
    watermark: Mapped[LoadWatermark | None] = relationship(
        back_populates="source_object", uselist=False, passive_deletes=True
    )


class GoldReference(TimestampMixin, Base):
    """One edge of the star: a fact column that points at a dimension.

    This is what keeps the Gold builder generic. Without it, resolving
    `customer_id` to `dim_customer` would have to be hardcoded, and the
    premise that nothing in the pipeline knows what `orders` is would
    stop being true at exactly the layer analysts look at.
    """

    __tablename__ = "gold_reference"
    __table_args__ = (
        UniqueConstraint("fact_object_id", "fact_column", name="uq_gold_reference"),
        CheckConstraint("fact_object_id <> dimension_object_id", name="ck_no_self_reference"),
        Index("ix_gold_reference_fact", "fact_object_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fact_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    dimension_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    fact_column: Mapped[str] = mapped_column(String(200), nullable=False)
    """Natural-key column on the fact, replaced by the dimension's surrogate."""


class ColumnMetadata(TimestampMixin, Base):
    """Per-column configuration, chiefly PII handling.

    Masking is driven from here rather than hardcoded in transformation
    code, so classifying a new column does not require a code change.
    """

    __tablename__ = "column_metadata"
    __table_args__ = (
        UniqueConstraint("source_object_id", "column_name", name="uq_column_metadata"),
        _check("sensitivity", Sensitivity),
        _check("masking_strategy", MaskingStrategy),
        CheckConstraint(
            "sensitivity <> 'none' OR masking_strategy = 'none'",
            name="ck_unclassified_is_unmasked",
        ),
        Index("ix_column_metadata_object", "source_object_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    column_name: Mapped[str] = mapped_column(String(200), nullable=False)
    sensitivity: Mapped[str] = mapped_column(String(20), default=Sensitivity.NONE, nullable=False)

    masking_strategy: Mapped[str] = mapped_column(
        String(20), default=MaskingStrategy.NONE, nullable=False
    )
    """How Silver masks this column. Independent of `sensitivity` because
    the classification is a statement about the data and the strategy is a
    decision about what to do with it — two columns can both be PII and
    still need different treatment."""

    allow_in_gold: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    """Allow-list for `sensitive_pii`. Those columns are dropped on the way
    into Gold unless someone has deliberately said otherwise, so the
    default for the star schema is that the most sensitive data is simply
    not there."""

    business_description: Mapped[str | None] = mapped_column(Text)

    source_object: Mapped[SourceObject] = relationship(back_populates="columns")


class DataQualityRule(TimestampMixin, Base):
    """A quality expectation, with an explicit consequence when it fails."""

    __tablename__ = "dq_rule"
    __table_args__ = (
        _check("rule_type", RuleType),
        _check("severity", Severity),
        Index("ix_dq_rule_object", "source_object_id", "active"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    rule_type: Mapped[str] = mapped_column(String(30), nullable=False)
    column_name: Mapped[str | None] = mapped_column(String(200))
    expression: Mapped[str | None] = mapped_column(Text)
    """Rule parameters as JSON, or raw SQL for CUSTOM_SQL."""
    severity: Mapped[str] = mapped_column(String(20), default=Severity.WARN, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    source_object: Mapped[SourceObject] = relationship(back_populates="rules")


class ObjectDependency(Base):
    """Edge in the load DAG: `source_object_id` depends on `depends_on_id`."""

    __tablename__ = "object_dependency"
    __table_args__ = (
        UniqueConstraint("source_object_id", "depends_on_id", name="uq_object_dependency"),
        CheckConstraint("source_object_id <> depends_on_id", name="ck_no_self_dependency"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    depends_on_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------


class LoadWatermark(Base):
    """Persisted high-water mark for incremental loads.

    Without this table, `incremental_column` is decorative: a pipeline
    restarted after a failure has no way to know where it stopped. Stored
    as text with an accompanying type so one column serves timestamp,
    integer and string keys.
    """

    __tablename__ = "load_watermark"
    __table_args__ = (_check("watermark_type", WatermarkType),)

    source_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), primary_key=True
    )
    watermark_value: Mapped[str] = mapped_column(String(100), nullable=False)
    watermark_type: Mapped[str] = mapped_column(String(20), nullable=False)
    committed_run_id: Mapped[str | None] = mapped_column(String(64))
    """Run that advanced this watermark. Only set after a successful write,
    which is what makes a re-run idempotent rather than lossy."""
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    source_object: Mapped[SourceObject] = relationship(back_populates="watermark")


class SchemaVersion(Base):
    """Snapshot of a source's schema, used to detect drift.

    Each ingest compares the incoming schema against the current version.
    New nullable columns evolve automatically; a removed or retyped column
    is a breaking change and stops the load.
    """

    __tablename__ = "schema_version"
    __table_args__ = (
        UniqueConstraint("source_object_id", "version", name="uq_schema_version"),
        Index("ix_schema_version_current", "source_object_id", "is_current"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_json: Mapped[str] = mapped_column(Text, nullable=False)
    """Serialised list of {name, type, nullable}."""
    schema_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    """Hash of schema_json, so drift is one string comparison."""
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


# --------------------------------------------------------------------------
# Audit
# --------------------------------------------------------------------------


class PipelineRun(Base):
    """One invocation of the pipeline, across all objects."""

    __tablename__ = "pipeline_run"
    __table_args__ = (_check("status", RunStatus),)

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(200), nullable=False)
    triggered_by: Mapped[str | None] = mapped_column(String(200))
    status: Mapped[str] = mapped_column(String(20), default=RunStatus.RUNNING, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    tasks: Mapped[list[TaskRun]] = relationship(back_populates="run", passive_deletes=True)


class TaskRun(Base):
    """One object moving through one layer.

    Row counts are split three ways because "rows copied" alone cannot
    distinguish a clean load from one that silently dropped half its input.
    """

    __tablename__ = "task_run"
    __table_args__ = (
        _check("status", RunStatus),
        _check("layer", Layer),
        Index("ix_task_run_object_time", "source_object_id", "started_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("pipeline_run.run_id", ondelete="CASCADE"), nullable=False
    )
    source_object_id: Mapped[int] = mapped_column(
        ForeignKey("source_object.id", ondelete="CASCADE"), nullable=False
    )
    layer: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default=RunStatus.RUNNING, nullable=False)

    rows_read: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rows_written: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rows_rejected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    watermark_from: Mapped[str | None] = mapped_column(String(100))
    watermark_to: Mapped[str | None] = mapped_column(String(100))
    """The window this task covered — what makes a replay reproducible."""

    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column(Float)
    """Wall-clock seconds, fractional. It was an integer until step 39, which
    floored every sub-second task to zero and made the dashboard's
    compute-time figure read 0 at demo scale (migration 0002)."""

    run: Mapped[PipelineRun] = relationship(back_populates="tasks")
    dq_results: Mapped[list[DataQualityResult]] = relationship(
        back_populates="task_run", passive_deletes=True
    )


class DataQualityResult(Base):
    """Outcome of one rule against one task run."""

    __tablename__ = "dq_result"
    __table_args__ = (Index("ix_dq_result_task", "task_run_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_run_id: Mapped[int] = mapped_column(
        ForeignKey("task_run.id", ondelete="CASCADE"), nullable=False
    )
    dq_rule_id: Mapped[int] = mapped_column(
        ForeignKey("dq_rule.id", ondelete="CASCADE"), nullable=False
    )
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    failed_row_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    detail: Mapped[str | None] = mapped_column(Text)
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    task_run: Mapped[TaskRun] = relationship(back_populates="dq_results")
