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
    database_url: str = "postgresql+psycopg://app:app@localhost:5432/app"
    """The driver is named explicitly. A bare `postgresql://` leaves
    SQLAlchemy to pick between psycopg2 and psycopg 3, and which one it
    picks has changed between versions — an ambiguity that surfaces as
    ModuleNotFoundError on a machine that has the other one."""

    engine: Literal["arrow", "spark"] = "arrow"
    """Compute engine for the heavy transforms. Arrow needs no JVM and is
    faster for anything that fits in memory, which is most things."""

    spark_driver_memory: str = "2g"
    """Heap for the Spark driver. The default 1g is not enough to collect
    a couple of million rows back, which is what `toArrow` does — the
    failure is `TaskResultLost`, which does not name memory at all."""

    spark_master: str = "local[*]"

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

    masking_key: str = ""
    """Key for the HMAC that masks classified columns. Empty means the
    built-in development key, which is published in this repository and
    therefore offers no protection at all — `lakehouse.privacy` warns
    when it is in use. Step 37 resolves the real key by secret name; the
    control plane never holds the value (design rule 6)."""

    openlineage_url: str = "http://localhost:5000"
    openlineage_namespace: str = "lakehouse"
    openlineage_enabled: bool = False
    """Off by default so the test suite and CI never need Marquez."""

    marquez_web_url: str = "http://localhost:3000"


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings instance."""
    return Settings()
