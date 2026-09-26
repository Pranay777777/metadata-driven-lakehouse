"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from app.config import Settings


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
