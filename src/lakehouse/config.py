"""Typed application settings, loaded from the environment."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Every configurable value lives here — never read os.environ directly."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: Literal["local", "ci", "staging", "prod"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    database_url: str = "postgresql://app:app@localhost:5432/app"

    lake_root: str = "./lake"
    """Where Delta tables are written. A local path today; see ADR-011."""

    # --- object storage ---------------------------------------------------
    # Nothing reads these yet: the lake writes to local disk and the
    # compose stack ships no object storage (ADR-011). They exist so the
    # settings surface is stable when step 33 moves the lake off disk.
    s3_endpoint_url: str = ""
    s3_bucket: str = "lakehouse"
    s3_access_key_id: str = ""
    s3_secret_access_key: str = ""
    """Empty by default. Step 37 resolves real credentials by name."""

    openlineage_url: str = "http://localhost:5000"
    openlineage_namespace: str = "lakehouse"
    openlineage_enabled: bool = False
    """Off by default so the test suite and CI never need Marquez."""

    marquez_web_url: str = "http://localhost:3000"


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings instance."""
    return Settings()
