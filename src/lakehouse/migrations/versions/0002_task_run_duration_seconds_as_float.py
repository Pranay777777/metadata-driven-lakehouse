"""task_run duration_seconds as float

Loaders stored int(elapsed), so every sub-second task recorded 0 and the
dashboard's compute-time figure read zero at demo scale. Widening to a
float loses nothing; narrowing back on downgrade truncates, as it did
before.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-28 03:22:08.324909
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("task_run", schema=None) as batch_op:
        batch_op.alter_column(
            "duration_seconds", existing_type=sa.INTEGER(), type_=sa.Float(), existing_nullable=True
        )


def downgrade() -> None:
    with op.batch_alter_table("task_run", schema=None) as batch_op:
        batch_op.alter_column(
            "duration_seconds", existing_type=sa.Float(), type_=sa.INTEGER(), existing_nullable=True
        )
