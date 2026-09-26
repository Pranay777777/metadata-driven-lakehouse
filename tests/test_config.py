"""Tests for application settings."""

from __future__ import annotations

from app.config import Settings, get_settings


def test_settings_defaults(settings: Settings) -> None:
    assert settings.app_env == "ci"
    assert settings.log_level == "DEBUG"


def test_get_settings_is_cached() -> None:
    assert get_settings() is get_settings()
