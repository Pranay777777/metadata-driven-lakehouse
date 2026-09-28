"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from lakehouse.config import Settings


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Settings isolated from the developer's own .env.

    Environment variables take precedence over the .env file in
    pydantic-settings, so setting them here makes the test deterministic
    regardless of what is on the machine.
    """
    monkeypatch.setenv("APP_ENV", "ci")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    return Settings()


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: needs a real Postgres — set LAKEHOUSE_TEST_POSTGRES_URL (ADR-020)",
    )
