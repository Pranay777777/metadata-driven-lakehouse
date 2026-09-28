# ruff: noqa: E501 — generated DDL; SQL strings do not wrap
"""baseline control plane

The schema exactly as `Base.metadata.create_all` built it at step 38,
verified table by table (columns, types, nullability, CHECK, UNIQUE,
indexes, foreign keys) on SQLite and Postgres. A database built by
create_all before migrations existed is stamped at this revision by
`lakehouse.migrate.ensure_schema`, then upgraded.

Revision ID: 0001
Revises:
Create Date: 2026-09-28 03:21:29.627416
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipeline_run",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("pipeline_name", sa.String(length=200), nullable=False),
        sa.Column("triggered_by", sa.String(length=200), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'skipped', 'quarantined')",
            name="ck_status",
        ),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_table(
        "source_system",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("secret_name", sa.String(length=200), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.CheckConstraint("kind IN ('jdbc', 'rest', 'file', 'object_store')", name="ck_kind"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "source_object",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_system_id", sa.Integer(), nullable=False),
        sa.Column("schema_name", sa.String(length=100), nullable=False),
        sa.Column("object_name", sa.String(length=200), nullable=False),
        sa.Column("target_path", sa.String(length=400), nullable=False),
        sa.Column("load_strategy", sa.String(length=20), nullable=False),
        sa.Column("incremental_column", sa.String(length=100), nullable=True),
        sa.Column("primary_key_columns", sa.String(length=400), nullable=True),
        sa.Column("cdc_operation_column", sa.String(length=100), nullable=True),
        sa.Column("cdc_delete_value", sa.String(length=20), nullable=False),
        sa.Column("watermark_grace", sa.Integer(), nullable=False),
        sa.Column("load_order", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("scd2_enabled", sa.Boolean(), nullable=False),
        sa.Column("gold_role", sa.String(length=20), nullable=True),
        sa.Column("freshness_sla_minutes", sa.Integer(), nullable=True),
        sa.Column("owner", sa.String(length=200), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "gold_role <> 'dimension' OR primary_key_columns IS NOT NULL",
            name="ck_dimension_needs_primary_key",
        ),
        sa.CheckConstraint(
            "gold_role IS NULL OR gold_role IN ('fact', 'dimension')", name="ck_gold_role"
        ),
        sa.CheckConstraint(
            "load_strategy <> 'cdc' OR primary_key_columns IS NOT NULL",
            name="ck_cdc_needs_primary_key",
        ),
        sa.CheckConstraint(
            "load_strategy <> 'incremental' OR incremental_column IS NOT NULL",
            name="ck_incremental_needs_column",
        ),
        sa.CheckConstraint(
            "load_strategy IN ('full', 'incremental', 'cdc')", name="ck_load_strategy"
        ),
        sa.ForeignKeyConstraint(["source_system_id"], ["source_system.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_system_id", "schema_name", "object_name", name="uq_source_object"
        ),
    )
    with op.batch_alter_table("source_object", schema=None) as batch_op:
        batch_op.create_index(
            "ix_source_object_active_order", ["active", "load_order"], unique=False
        )

    op.create_table(
        "column_metadata",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_object_id", sa.Integer(), nullable=False),
        sa.Column("column_name", sa.String(length=200), nullable=False),
        sa.Column("sensitivity", sa.String(length=20), nullable=False),
        sa.Column("masking_strategy", sa.String(length=20), nullable=False),
        sa.Column("allow_in_gold", sa.Boolean(), nullable=False),
        sa.Column("business_description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "masking_strategy IN ('none', 'hash', 'redact', 'partial')", name="ck_masking_strategy"
        ),
        sa.CheckConstraint(
            "sensitivity <> 'none' OR masking_strategy = 'none'", name="ck_unclassified_is_unmasked"
        ),
        sa.CheckConstraint(
            "sensitivity IN ('none', 'internal', 'pii', 'sensitive_pii')", name="ck_sensitivity"
        ),
        sa.ForeignKeyConstraint(["source_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_object_id", "column_name", name="uq_column_metadata"),
    )
    with op.batch_alter_table("column_metadata", schema=None) as batch_op:
        batch_op.create_index("ix_column_metadata_object", ["source_object_id"], unique=False)

    op.create_table(
        "dq_rule",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_object_id", sa.Integer(), nullable=False),
        sa.Column("rule_type", sa.String(length=30), nullable=False),
        sa.Column("column_name", sa.String(length=200), nullable=True),
        sa.Column("expression", sa.Text(), nullable=True),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "rule_type IN ('not_null', 'unique', 'range', 'allowed_values', 'regex', 'freshness', 'row_count', 'custom_sql')",
            name="ck_rule_type",
        ),
        sa.CheckConstraint("severity IN ('warn', 'quarantine', 'fail')", name="ck_severity"),
        sa.ForeignKeyConstraint(["source_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("dq_rule", schema=None) as batch_op:
        batch_op.create_index("ix_dq_rule_object", ["source_object_id", "active"], unique=False)

    op.create_table(
        "gold_reference",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("fact_object_id", sa.Integer(), nullable=False),
        sa.Column("dimension_object_id", sa.Integer(), nullable=False),
        sa.Column("fact_column", sa.String(length=200), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.CheckConstraint("fact_object_id <> dimension_object_id", name="ck_no_self_reference"),
        sa.ForeignKeyConstraint(["dimension_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["fact_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("fact_object_id", "fact_column", name="uq_gold_reference"),
    )
    with op.batch_alter_table("gold_reference", schema=None) as batch_op:
        batch_op.create_index("ix_gold_reference_fact", ["fact_object_id"], unique=False)

    op.create_table(
        "load_watermark",
        sa.Column("source_object_id", sa.Integer(), nullable=False),
        sa.Column("watermark_value", sa.String(length=100), nullable=False),
        sa.Column("watermark_type", sa.String(length=20), nullable=False),
        sa.Column("committed_run_id", sa.String(length=64), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "watermark_type IN ('timestamp', 'integer', 'string')", name="ck_watermark_type"
        ),
        sa.ForeignKeyConstraint(["source_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("source_object_id"),
    )
    op.create_table(
        "object_dependency",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_object_id", sa.Integer(), nullable=False),
        sa.Column("depends_on_id", sa.Integer(), nullable=False),
        sa.CheckConstraint("source_object_id <> depends_on_id", name="ck_no_self_dependency"),
        sa.ForeignKeyConstraint(["depends_on_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_object_id", "depends_on_id", name="uq_object_dependency"),
    )
    op.create_table(
        "schema_version",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_object_id", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("schema_json", sa.Text(), nullable=False),
        sa.Column("schema_hash", sa.String(length=64), nullable=False),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["source_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_object_id", "version", name="uq_schema_version"),
    )
    with op.batch_alter_table("schema_version", schema=None) as batch_op:
        batch_op.create_index(
            "ix_schema_version_current", ["source_object_id", "is_current"], unique=False
        )

    op.create_table(
        "task_run",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("source_object_id", sa.Integer(), nullable=False),
        sa.Column("layer", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("rows_read", sa.Integer(), nullable=False),
        sa.Column("rows_written", sa.Integer(), nullable=False),
        sa.Column("rows_rejected", sa.Integer(), nullable=False),
        sa.Column("watermark_from", sa.String(length=100), nullable=True),
        sa.Column("watermark_to", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Integer(), nullable=True),
        sa.CheckConstraint("layer IN ('bronze', 'silver', 'gold')", name="ck_layer"),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'skipped', 'quarantined')",
            name="ck_status",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["pipeline_run.run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_object_id"], ["source_object.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("task_run", schema=None) as batch_op:
        batch_op.create_index(
            "ix_task_run_object_time", ["source_object_id", "started_at"], unique=False
        )

    op.create_table(
        "dq_result",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_run_id", sa.Integer(), nullable=False),
        sa.Column("dq_rule_id", sa.Integer(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("failed_row_count", sa.Integer(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column(
            "evaluated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(CURRENT_TIMESTAMP)"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["dq_rule_id"], ["dq_rule.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["task_run_id"], ["task_run.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("dq_result", schema=None) as batch_op:
        batch_op.create_index("ix_dq_result_task", ["task_run_id"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("dq_result", schema=None) as batch_op:
        batch_op.drop_index("ix_dq_result_task")

    op.drop_table("dq_result")
    with op.batch_alter_table("task_run", schema=None) as batch_op:
        batch_op.drop_index("ix_task_run_object_time")

    op.drop_table("task_run")
    with op.batch_alter_table("schema_version", schema=None) as batch_op:
        batch_op.drop_index("ix_schema_version_current")

    op.drop_table("schema_version")
    op.drop_table("object_dependency")
    op.drop_table("load_watermark")
    with op.batch_alter_table("gold_reference", schema=None) as batch_op:
        batch_op.drop_index("ix_gold_reference_fact")

    op.drop_table("gold_reference")
    with op.batch_alter_table("dq_rule", schema=None) as batch_op:
        batch_op.drop_index("ix_dq_rule_object")

    op.drop_table("dq_rule")
    with op.batch_alter_table("column_metadata", schema=None) as batch_op:
        batch_op.drop_index("ix_column_metadata_object")

    op.drop_table("column_metadata")
    with op.batch_alter_table("source_object", schema=None) as batch_op:
        batch_op.drop_index("ix_source_object_active_order")

    op.drop_table("source_object")
    op.drop_table("source_system")
    op.drop_table("pipeline_run")
