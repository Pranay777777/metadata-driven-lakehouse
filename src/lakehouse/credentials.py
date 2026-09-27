"""Credentials are resolved by name, never stored.

Design rule 6 has been in the docstrings since ADR-002: the control plane
holds the *name* of a secret, never its value. Until now nothing resolved
those names, so the rule was a promise rather than a mechanism. This
module is the mechanism.

Two backends, one interface:

**env** — the default, for a laptop. A secret named `masking-key` is read
from the environment variable `MASKING_KEY`, falling back to the `.env`
file, which is the same precedence pydantic-settings uses everywhere
else. This is the one module allowed to read the environment directly:
secret names are data in the control plane, so they cannot be declared as
fields on `Settings` ahead of time.

**keyvault** — Azure Key Vault through `DefaultAzureCredential`. On Azure
that is the managed identity, so no credential exists anywhere to leak;
on a laptop it falls through to `az login`. The SDK is an optional extra
and is imported only when this backend is chosen, so the default install
never needs it — the same rule the Spark engine follows.

Values come back as `SecretStr`, whose repr is `'**********'`. A secret
that reaches a log line, an exception message or a Dagster event through
an f-string has been redacted before anyone decided whether to print it.

The backends differ in what a *missing* secret means, and deliberately:
`env` lets callers fall back to a development default, `keyvault` does
not. A laptop without a masking key should warn; a production run
without one should stop.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol

from dotenv import dotenv_values
from pydantic import SecretStr

from lakehouse.config import Settings


class SecretNotFoundError(LookupError):
    """A named secret is not in the configured store.

    The message names the secret and where it looked. It never includes a
    value, because there is none — and because error messages end up in
    `task_run.error_message`, which anyone with read access can see.
    """


class SecretStore(Protocol):
    """Somewhere secrets can be looked up by name."""

    backend: str

    def get(self, name: str) -> SecretStr | None:
        """The secret's value, or None if the store has no such secret."""
        ...


def env_var_name(secret_name: str) -> str:
    """`masking-key` → `MASKING_KEY`.

    Key Vault names allow only letters, digits and dashes; environment
    variables conventionally use upper case and underscores. Mapping one
    onto the other means the same secret name works in both backends,
    so the control plane never needs to know which one is in use.
    """
    return secret_name.strip().upper().replace("-", "_")


class EnvSecretStore:
    """Secrets from the process environment, then the `.env` file."""

    backend = "env"

    def __init__(self, env_file: Path | None = Path(".env")) -> None:
        self._file: dict[str, str | None] = (
            dotenv_values(env_file) if env_file is not None and env_file.exists() else {}
        )

    def get(self, name: str) -> SecretStr | None:
        key = env_var_name(name)
        value = os.environ.get(key) or self._file.get(key)
        return SecretStr(value) if value else None


class KeyVaultSecretStore:
    """Secrets from Azure Key Vault.

    Values are cached for the life of the store. A pipeline run resolves
    the same few names many times, and Key Vault throttles.
    """

    backend = "keyvault"

    def __init__(self, vault_url: str, client: Any | None = None) -> None:
        if not vault_url and client is None:
            raise ValueError("secrets_backend is 'keyvault' but key_vault_url is empty")
        self.vault_url = vault_url
        self._client = client if client is not None else _key_vault_client(vault_url)
        self._not_found = _not_found_error()
        self._cache: dict[str, SecretStr | None] = {}

    def get(self, name: str) -> SecretStr | None:
        if name not in self._cache:
            try:
                value = self._client.get_secret(name).value
            except self._not_found:
                value = None
            self._cache[name] = SecretStr(value) if value else None
        return self._cache[name]


_AZURE_HINT = (
    "the 'keyvault' secrets backend needs the Azure SDK — install it with pip install -e '.[azure]'"
)


def _key_vault_client(vault_url: str) -> Any:
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError as exc:
        raise ImportError(_AZURE_HINT) from exc
    return SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())


def _not_found_error() -> type[Exception]:
    """The SDK's not-found exception, or a stand-in when it is absent.

    Tests inject a fake client without installing the SDK; the stand-in
    lets them raise a not-found the store will recognise.
    """
    try:
        from azure.core.exceptions import ResourceNotFoundError
    except ImportError:
        return KeyError
    return ResourceNotFoundError  # type: ignore[no-any-return, unused-ignore]


_STORES: dict[tuple[str, str], SecretStore] = {}


def _build_store(settings: Settings) -> SecretStore:
    if settings.secrets_backend == "keyvault":
        return KeyVaultSecretStore(settings.key_vault_url)
    return EnvSecretStore()


def get_store(settings: Settings | None = None) -> SecretStore:
    """The store the settings select, built once per backend and vault.

    Built once because a Key Vault client authenticates on first use, and
    re-authenticating for every lookup is how a pipeline gets throttled.
    """
    resolved = settings or Settings()
    key = (resolved.secrets_backend, resolved.key_vault_url)
    if key not in _STORES:
        _STORES[key] = _build_store(resolved)
    return _STORES[key]


def clear_stores() -> None:
    """Forget built stores — for tests, and after changing settings."""
    _STORES.clear()


def resolve(name: str, store: SecretStore | None = None) -> SecretStr:
    """Look a secret up by name, or fail with a message that says where.

    Raises:
        SecretNotFoundError: the store has no secret by that name.
    """
    source = store or get_store()
    value = source.get(name)
    if value is None:
        where = (
            f"environment variable {env_var_name(name)}"
            if source.backend == "env"
            else f"Key Vault secret '{name}'"
        )
        raise SecretNotFoundError(f"secret '{name}' is not set — expected {where}")
    return value


def database_url(settings: Settings | None = None, store: SecretStore | None = None) -> str:
    """The control-plane URL, resolved from a secret when one is named.

    Locally `database_url` is the compose stack's own credential, which
    exists nowhere but a container on the developer's machine. Anywhere
    else, `database_url_secret` names the secret that holds the real one,
    and the value in settings is ignored.
    """
    resolved = settings or Settings()
    if not resolved.database_url_secret:
        return resolved.database_url
    return resolve(resolved.database_url_secret, store or get_store(resolved)).get_secret_value()


def connection_string(secret_name: str | None, store: SecretStore | None = None) -> SecretStr:
    """Resolve a `source_system.secret_name` into its connection string.

    The control plane stores the name; this is the only place it becomes
    a value, and it stays wrapped until the moment a driver needs it.
    """
    if not secret_name:
        raise SecretNotFoundError(
            "source system has no secret_name — a source that needs credentials must name one"
        )
    return resolve(secret_name, store)
