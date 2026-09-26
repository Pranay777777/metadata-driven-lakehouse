"""Generate reference DDL from the SQLAlchemy models.

The models are the single source of truth. This script renders them for
each target dialect so the checked-in SQL cannot drift from the code, and
so the schema is readable by someone who does not want to read Python.

    python scripts/generate_ddl.py
"""

from __future__ import annotations

import pathlib
import sys

from sqlalchemy import create_mock_engine
from sqlalchemy.schema import CreateIndex, CreateTable, SchemaItem

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from lakehouse.metadata import Base

DIALECTS = {
    "postgresql": "postgresql://",
    "mssql": "mssql+pyodbc://",
}

HEADER = """\
-- ---------------------------------------------------------------------
-- Metadata control plane — {dialect}
--
-- GENERATED FILE. Do not edit.
-- Source of truth: src/lakehouse/metadata/models.py
-- Regenerate with: python scripts/generate_ddl.py
-- ---------------------------------------------------------------------

"""


def render(dialect: str, url: str) -> str:
    """Render CREATE TABLE and CREATE INDEX statements for one dialect."""
    statements: list[str] = []

    def collect(sql: SchemaItem, *_: object, **__: object) -> None:
        statements.append(str(sql).strip().rstrip(";") + ";")

    engine = create_mock_engine(url, collect)
    for table in Base.metadata.sorted_tables:
        collect(CreateTable(table).compile(dialect=engine.dialect))
        for index in table.indexes:
            collect(CreateIndex(index).compile(dialect=engine.dialect))

    return HEADER.format(dialect=dialect) + "\n\n".join(statements) + "\n"


def main() -> None:
    out_dir = pathlib.Path(__file__).resolve().parents[1] / "sql" / "reference"
    out_dir.mkdir(parents=True, exist_ok=True)
    for dialect, url in DIALECTS.items():
        path = out_dir / f"control_plane.{dialect}.sql"
        path.write_text(render(dialect, url), encoding="utf-8")
        print(f"wrote {path.relative_to(path.parents[2])}")


if __name__ == "__main__":
    main()
