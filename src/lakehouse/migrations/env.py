"""Alembic environment: the models are the target, credentials give the URL."""

from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine, pool

from lakehouse.config import Settings
from lakehouse.credentials import database_url
from lakehouse.metadata.models import Base

target_metadata = Base.metadata


def _url() -> str:
    # A URL set on the config wins — that is how tests and ensure_schema
    # point Alembic at a specific database. Otherwise resolve it the same
    # way the pipeline does, so DATABASE_URL_SECRET is honoured.
    configured = context.config.get_main_option("sqlalchemy.url")
    return configured or database_url(Settings())


def run_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_online() -> None:
    connection = context.config.attributes.get("connection")
    if connection is not None:
        _run(connection)
        return
    engine = create_engine(_url(), poolclass=pool.NullPool)
    with engine.connect() as conn:
        _run(conn)


def _run(connection: object) -> None:
    context.configure(
        connection=connection,  # type: ignore[arg-type]
        target_metadata=target_metadata,
        # SQLite cannot ALTER most things; batch mode rebuilds the table.
        render_as_batch=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_offline()
else:
    run_online()
