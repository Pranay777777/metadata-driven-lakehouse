"""Bring a control plane to the current schema (ADR-020).

`ensure_schema` replaces the `Base.metadata.create_all` calls the CLIs
used to make. `create_all` only ever creates missing tables — it never
adds a column to one that exists — which is how step 36's two new
`column_metadata` columns broke every Silver task on an existing stack.

Three starting points, one outcome:

- **Empty database** — every migration runs from the baseline.
- **Versioned database** — pending migrations run; none is a no-op.
- **Legacy database** — tables exist, but no `alembic_version`, because
  it was built by `create_all` before migrations existed. It is stamped
  at the baseline and then upgraded. The baseline is the schema as of
  step 38, which every stack that followed the steps already has.

    python -m lakehouse.migrate            # upgrade to head
    python -m lakehouse.migrate --check    # exit 1 if anything is pending
"""

from __future__ import annotations

import argparse
import logging
from importlib.resources import files

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, create_engine, inspect

from lakehouse.config import Settings
from lakehouse.credentials import database_url
from lakehouse.logging import configure_logging

logger = logging.getLogger(__name__)

BASELINE = "0001"
"""The revision a legacy `create_all` database is presumed to match."""


def alembic_config(engine: Engine) -> Config:
    """An Alembic config that needs no alembic.ini on disk.

    The CLIs run from anywhere, including the Docker image, so the script
    location comes from the installed package rather than a relative path.
    """
    config = Config()
    config.set_main_option("script_location", str(files("lakehouse") / "migrations"))
    config.set_main_option(
        "sqlalchemy.url", engine.url.render_as_string(hide_password=False).replace("%", "%%")
    )
    return config


def head() -> str:
    script = ScriptDirectory.from_config(alembic_config(create_engine("sqlite://")))
    revision = script.get_current_head()
    assert revision is not None
    return revision


def current(engine: Engine) -> str | None:
    with engine.connect() as conn:
        return MigrationContext.configure(conn).get_current_revision()


def is_legacy(engine: Engine) -> bool:
    """Tables built by create_all, with no record of any migration."""
    tables = set(inspect(engine).get_table_names())
    return "source_object" in tables and "alembic_version" not in tables


def ensure_schema(engine: Engine) -> str:
    """Upgrade `engine` to head, adopting a legacy database first.

    Returns the revision the database ended at.
    """
    config = alembic_config(engine)
    if is_legacy(engine):
        logger.info("control plane predates migrations — stamping it at baseline %s", BASELINE)
        command.stamp(config, BASELINE)
    before = current(engine)
    command.upgrade(config, "head")
    after = current(engine)
    if before != after:
        logger.info("control plane migrated %s → %s", before or "empty", after)
    assert after is not None
    return after


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse.migrate", description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="report pending migrations without applying them"
    )
    args = parser.parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    engine = create_engine(database_url(settings))

    target = head()
    if args.check:
        at = current(engine)
        where = at or ("legacy (pre-migration)" if is_legacy(engine) else "empty")
        print(f"database at {where}, head is {target}")
        return 0 if at == target else 1

    print(f"control plane at {ensure_schema(engine)} (head {target})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
