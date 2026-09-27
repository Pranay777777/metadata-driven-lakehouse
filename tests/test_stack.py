"""Tests for the local stack health checker."""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from lakehouse.config import Settings
from lakehouse.stack import Check, build_checks, main, run_checks


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv("APP_ENV", "ci")
    return Settings()


# --------------------------------------------------------------------------
# Composition
# --------------------------------------------------------------------------


def test_every_stack_service_is_checked(settings: Settings) -> None:
    names = [c.name for c in build_checks(settings)]
    assert names == ["postgres", "marquez", "marquez-web"]


def test_the_database_check_reads_the_configured_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.internal:6543/app")
    check = build_checks(Settings())[0]
    assert check.target == "db.internal:6543"


def test_a_url_without_a_port_falls_back_to_5432(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.internal/app")
    assert build_checks(Settings())[0].target == "db.internal:5432"


def test_trailing_slashes_do_not_double_up(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENLINEAGE_URL", "http://localhost:5000/")
    marquez = next(c for c in build_checks(Settings()) if c.name == "marquez")
    assert marquez.target == "http://localhost:5000"


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------


def test_all_failures_are_reported_not_just_the_first() -> None:
    """A stack with three problems should report three, not one."""

    def down() -> str:
        raise OSError("refused")

    checks = [Check(f"svc{i}", "host", down) for i in range(3)]
    healthy, lines = run_checks(checks)

    assert healthy is False
    assert sum("DOWN" in line for line in lines) == 3


def test_a_healthy_stack_reports_clean() -> None:
    checks = [Check("svc", "host", lambda: "fine")]
    healthy, lines = run_checks(checks)

    assert healthy is True
    assert "ok" in lines[0]


def test_a_mixed_stack_is_unhealthy() -> None:
    def down() -> str:
        raise TimeoutError("slow")

    healthy, _ = run_checks([Check("up", "host", lambda: "fine"), Check("down", "host", down)])
    assert healthy is False


# --------------------------------------------------------------------------
# Real probes against a throwaway server
# --------------------------------------------------------------------------


class _Quiet(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *_: object) -> None:
        return


@pytest.fixture
def http_server() -> Iterator[int]:
    server = HTTPServer(("127.0.0.1", 0), _Quiet)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield int(server.server_address[1])
    server.shutdown()
    server.server_close()


def test_an_http_probe_succeeds_against_a_live_server(
    monkeypatch: pytest.MonkeyPatch, http_server: int
) -> None:
    monkeypatch.setenv("OPENLINEAGE_URL", f"http://127.0.0.1:{http_server}")
    marquez = next(c for c in build_checks(Settings()) if c.name == "marquez")
    assert "200" in marquez.probe()


def test_a_tcp_probe_succeeds_against_a_live_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    monkeypatch.setenv("DATABASE_URL", f"postgresql://u:p@127.0.0.1:{port}/app")
    try:
        assert build_checks(Settings())[0].probe() == "accepting connections"
    finally:
        listener.close()


def test_the_cli_fails_when_nothing_is_running(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Port 1 is reserved and never listening."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@127.0.0.1:1/app")
    monkeypatch.setenv("OPENLINEAGE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("MARQUEZ_WEB_URL", "http://127.0.0.1:1")

    assert main(["--timeout", "0.5"]) == 1
    assert "DOWN" in capsys.readouterr().out


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


def test_lineage_is_off_by_default(settings: Settings) -> None:
    """CI and the test suite must never need Marquez running."""
    assert settings.openlineage_enabled is False


def test_the_database_url_names_its_driver(settings: Settings) -> None:
    """A bare postgresql:// lets SQLAlchemy choose between psycopg2 and
    psycopg 3, and the choice has changed between versions."""
    assert settings.database_url.startswith("postgresql+psycopg://")


def test_stack_defaults_match_the_compose_file(settings: Settings) -> None:
    assert settings.openlineage_url.endswith(":5000")
    assert settings.marquez_web_url.endswith(":3000")


def test_object_storage_is_not_checked(settings: Settings) -> None:
    """The compose stack ships no object storage; see ADR-011."""
    assert "minio" not in [c.name for c in build_checks(settings)]
