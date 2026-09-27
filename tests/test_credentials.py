"""Tests for resolving secrets by name."""

from __future__ import annotations

import builtins
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from lakehouse.config import Settings
from lakehouse.credentials import (
    EnvSecretStore,
    KeyVaultSecretStore,
    SecretNotFoundError,
    clear_stores,
    connection_string,
    database_url,
    env_var_name,
    get_store,
    resolve,
)


@pytest.fixture(autouse=True)
def _fresh_stores() -> Iterator[None]:
    clear_stores()
    yield
    clear_stores()


class FakeVault:
    """Stands in for azure.keyvault.secrets.SecretClient."""

    def __init__(self, **secrets: str) -> None:
        self.secrets = secrets
        self.calls = 0

    def get_secret(self, name: str) -> Any:
        self.calls += 1
        if name not in self.secrets:
            raise _not_found()(f"secret {name} not found")
        return SimpleNamespace(value=self.secrets[name])


def _not_found() -> type[Exception]:
    try:
        from azure.core.exceptions import ResourceNotFoundError
    except ImportError:
        return KeyError
    return ResourceNotFoundError  # type: ignore[no-any-return, unused-ignore]


# --- names ----------------------------------------------------------------


def test_a_secret_name_maps_to_an_environment_variable() -> None:
    assert env_var_name("masking-key") == "MASKING_KEY"
    assert env_var_name(" control-plane-url ") == "CONTROL_PLANE_URL"


# --- env backend ----------------------------------------------------------


def test_env_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_DB", "postgresql://u:p@h/db")
    value = EnvSecretStore(env_file=None).get("source-db")
    assert value is not None
    assert value.get_secret_value() == "postgresql://u:p@h/db"


def test_env_falls_back_to_the_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOURCE_DB", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("SOURCE_DB=from-file\n")
    value = EnvSecretStore(env_file).get("source-db")
    assert value is not None
    assert value.get_secret_value() == "from-file"


def test_the_environment_wins_over_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("SOURCE_DB=from-file\n")
    monkeypatch.setenv("SOURCE_DB", "from-env")
    value = EnvSecretStore(env_file).get("source-db")
    assert value is not None
    assert value.get_secret_value() == "from-env"


def test_a_missing_env_file_is_not_an_error(tmp_path: Path) -> None:
    assert EnvSecretStore(tmp_path / "absent.env").get("nothing-here") is None


def test_an_empty_value_counts_as_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BLANK_SECRET", "")
    assert EnvSecretStore(env_file=None).get("blank-secret") is None


# --- keyvault backend -----------------------------------------------------


def test_key_vault_returns_the_secret() -> None:
    store = KeyVaultSecretStore("https://v.vault.azure.net", client=FakeVault(**{"db-url": "x"}))
    value = store.get("db-url")
    assert value is not None
    assert value.get_secret_value() == "x"


def test_key_vault_reports_a_missing_secret_as_none() -> None:
    store = KeyVaultSecretStore("https://v.vault.azure.net", client=FakeVault())
    assert store.get("absent") is None


def test_key_vault_is_asked_once_per_name() -> None:
    vault = FakeVault(**{"db-url": "x"})
    store = KeyVaultSecretStore("https://v.vault.azure.net", client=vault)
    store.get("db-url")
    store.get("db-url")
    store.get("absent")
    store.get("absent")
    assert vault.calls == 2


def test_key_vault_needs_a_url() -> None:
    with pytest.raises(ValueError, match="key_vault_url"):
        KeyVaultSecretStore("")


def test_key_vault_builds_a_real_client_when_the_sdk_is_present() -> None:
    pytest.importorskip("azure.keyvault.secrets")
    store = KeyVaultSecretStore("https://example.vault.azure.net")
    # Constructing the client authenticates lazily — nothing is called yet.
    assert store.vault_url == "https://example.vault.azure.net"


def test_a_missing_sdk_says_how_to_install_it(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def no_azure(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("azure"):
            raise ImportError(f"No module named '{name}'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_azure)
    with pytest.raises(ImportError, match=r"\[azure\]"):
        KeyVaultSecretStore("https://v.vault.azure.net")


# --- selection and resolution ---------------------------------------------


def test_env_is_the_default_backend() -> None:
    assert get_store(Settings()).backend == "env"


def test_the_store_is_built_once() -> None:
    assert get_store(Settings()) is get_store(Settings())


def test_keyvault_is_selected_by_settings() -> None:
    pytest.importorskip("azure.keyvault.secrets")
    settings = Settings(secrets_backend="keyvault", key_vault_url="https://v.vault.azure.net")
    assert get_store(settings).backend == "keyvault"


class Held:
    def __init__(self, backend: str = "env", **secrets: str) -> None:
        self.backend = backend
        self.secrets = secrets

    def get(self, name: str) -> SecretStr | None:
        value = self.secrets.get(name)
        return SecretStr(value) if value else None


def test_resolve_returns_a_redacted_value() -> None:
    value = resolve("token", Held(token="s3cr3t"))
    assert "s3cr3t" not in repr(value)
    assert "s3cr3t" not in f"{value}"
    assert value.get_secret_value() == "s3cr3t"


def test_a_missing_env_secret_names_the_variable() -> None:
    with pytest.raises(SecretNotFoundError, match="environment variable SOURCE_DB"):
        resolve("source-db", Held())


def test_a_missing_vault_secret_names_the_vault_secret() -> None:
    with pytest.raises(SecretNotFoundError, match="Key Vault secret 'source-db'"):
        resolve("source-db", Held(backend="keyvault"))


# --- consumers ------------------------------------------------------------


def test_the_local_database_url_is_used_when_no_secret_is_named() -> None:
    assert database_url(Settings(database_url="sqlite://")) == "sqlite://"


def test_a_named_database_secret_overrides_the_setting() -> None:
    settings = Settings(database_url="sqlite://", database_url_secret="control-plane-url")
    store = Held(**{"control-plane-url": "postgresql+psycopg://real/db"})
    assert database_url(settings, store) == "postgresql+psycopg://real/db"


def test_a_named_database_secret_that_is_missing_fails_loudly() -> None:
    settings = Settings(database_url_secret="control-plane-url")
    with pytest.raises(SecretNotFoundError, match="control-plane-url"):
        database_url(settings, Held())


def test_a_source_connection_string_is_resolved_by_name() -> None:
    value = connection_string("crm-db", Held(**{"crm-db": "Server=x;Password=y"}))
    assert value.get_secret_value() == "Server=x;Password=y"


def test_a_source_without_a_secret_name_is_refused() -> None:
    with pytest.raises(SecretNotFoundError, match="secret_name"):
        connection_string(None, Held())


def test_no_setting_holds_a_secret_value() -> None:
    """Design rule 6, enforced: credentials are named, never stored.

    The one permitted exception is the local compose URL, whose
    credential exists only inside a container on the developer's machine.
    """
    suspicious = ("password", "access_key", "secret_access", "token", "api_key")
    offenders = [
        name
        for name in Settings.model_fields
        if any(word in name for word in suspicious) and not name.endswith("_secret")
    ]
    assert offenders == []
    named = [name for name in Settings.model_fields if name.endswith("_secret")]
    assert set(named) >= {"database_url_secret", "masking_key_secret", "s3_credentials_secret"}
