"""The dashboard, rendered headlessly with Streamlit's own test harness.

These run the real app file against a real control plane. The numbers
are tested in `test_audit_views.py`; these prove the page renders them
without raising, and that it degrades sensibly when there is nothing to
show.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Skip on the symbol itself, not on `import streamlit`: an uninstall can
# leave empty streamlit/ directories behind that still import, as
# namespace packages, all the way down to streamlit.testing.v1.
try:
    from streamlit.testing.v1 import AppTest
except ImportError:
    pytest.skip("streamlit is not installed", allow_module_level=True)

from lakehouse import pipeline, privacy
from lakehouse.seed.generator import SeedConfig, generate, write

APP = Path(__file__).resolve().parents[1] / "src" / "lakehouse" / "dashboard" / "app.py"
TIMEOUT = 60


def _texts(app: AppTest) -> str:
    """Everything the page printed, as one searchable string."""
    parts = [m.value for m in app.markdown] + [c.value for c in app.caption]
    parts += [i.value for i in app.info] + [w.value for w in app.warning]
    parts += [t.value for t in app.title] + [s.value for s in app.subheader]
    return "\n".join(str(p) for p in parts)


@pytest.fixture
def control_plane(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    url = f"sqlite:///{tmp_path / 'control.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.delenv("DATABASE_URL_SECRET", raising=False)
    return url


def test_an_empty_control_plane_says_what_to_run(control_plane: str) -> None:
    from sqlalchemy import create_engine

    from lakehouse.metadata.models import Base

    Base.metadata.create_all(create_engine(control_plane))

    app = AppTest.from_file(str(APP), default_timeout=TIMEOUT).run()

    assert not app.exception
    assert "control plane is empty" in _texts(app)
    assert len(app.metric) == 0


def test_a_populated_lake_renders_every_section(
    control_plane: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    write(generate(SeedConfig(rows=2_000)), data)
    lake = tmp_path / "lake"
    assert pipeline.main(["--register", "--data-dir", str(data), "--lake-root", str(lake)]) == 0
    assert pipeline.main(["--data-dir", str(data), "--lake-root", str(lake)]) == 0
    assert privacy.main(["--apply", "--lake-root", str(lake)]) == 0

    app = AppTest.from_file(str(APP), default_timeout=TIMEOUT).run()

    assert not app.exception
    text = _texts(app)
    for section in (
        "Freshness",
        "Volume and compute",
        "Data quality",
        "Gold integrity",
        "Privacy posture",
        "Recent runs",
    ):
        assert section in text
    assert [m.label for m in app.metric] == [
        "Last run",
        "Stale runs",
        "Freshness breaches",
        "Rows quarantined",
        "Unknown-member rows",
    ]
    assert app.metric[0].value == "succeeded"
    assert len(app.expander) >= 2  # bronze-silver-gold run plus the scan
    assert "cost proxy" in text


def test_the_page_never_shows_the_database_password() -> None:
    from lakehouse.dashboard.app import describe

    shown = describe("postgresql+psycopg://app:hunter2@db.internal:5432/control")

    assert "hunter2" not in shown
    assert "***" in shown
    assert "db.internal" in shown


def test_the_page_names_the_control_plane_it_read(control_plane: str) -> None:
    from sqlalchemy import create_engine

    from lakehouse.metadata.models import Base

    Base.metadata.create_all(create_engine(control_plane))

    app = AppTest.from_file(str(APP), default_timeout=TIMEOUT).run()

    assert "control.db" in _texts(app)
