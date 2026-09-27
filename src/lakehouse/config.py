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
    """The local compose stack's control plane. The credential in it exists
    only inside a container on the developer's machine, which is the one
    place a connection string in configuration is acceptable. Everywhere
    else set `database_url_secret` and this value is ignored.

    The driver is named explicitly. A bare `postgresql://` leaves
    SQLAlchemy to pick between psycopg2 and psycopg 3, and which one it
    picks has changed between versions — an ambiguity that surfaces as
    ModuleNotFoundError on a machine that has the other one."""

    database_url_secret: str = ""
    """Name of the secret holding the control-plane URL. When set, the URL
    is resolved through the secret store (ADR-018)."""

    # --- secrets ------------------------------------------------------------
    # Only names live in configuration. Values are resolved at the moment
    # they are needed, by `lakehouse.credentials` (design rule 6).
    secrets_backend: Literal["env", "keyvault"] = "env"
    """Where named secrets are resolved. `env` reads the environment and
    `.env`; `keyvault` reads Azure Key Vault via managed identity."""

    key_vault_url: str = ""
    """e.g. https://my-vault.vault.azure.net — required for `keyvault`."""

    masking_key_secret: str = "masking-key"  # noqa: S105 — a secret's name, not its value
    """Name of the secret holding the PII masking key (ADR-017). With the
    env backend that is the `MASKING_KEY` variable."""

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
    # compose stack ships no object storage (ADR-011).
    s3_endpoint_url: str = ""
    s3_bucket: str = "lakehouse"
    s3_credentials_secret: str = ""
    """Name of the secret holding object-storage credentials. The access
    key and secret used to be two settings holding the values themselves,
    which is exactly what design rule 6 forbids."""

    openlineage_url: str = "http://localhost:5000"
    openlineage_namespace: str = "lakehouse"
    openlineage_enabled: bool = False
    """Off by default so the test suite and CI never need Marquez."""

    marquez_web_url: str = "http://localhost:3000"


@lru_cache
def get_settings() -> Settings:
    """Return the cached settings instance."""
    return Settings()
