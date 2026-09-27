# ADR-018: Secrets are resolved by name, from env or Key Vault

- **Status:** accepted
- **Date:** 2026-09-27

## Context

Design rule 6 — credentials are never stored in the control plane, only
the name of the secret — has been in the docstrings since ADR-002.
`source_system.secret_name` exists to hold those names. Nothing resolved
them, so the rule was a promise, not a mechanism.

Worse, `Settings` itself broke the rule three times: `masking_key`,
`s3_access_key_id` and `s3_secret_access_key` were settings holding
secret *values*. ADR-017 had just added the first of those, with a note
that this step would fix it.

## Decision

### One interface, two backends

`lakehouse.credentials` resolves a secret by name through a `SecretStore`.

**`env`** (default) maps a name onto an environment variable —
`masking-key` becomes `MASKING_KEY` — reading the process environment
first and `.env` second, the same precedence pydantic-settings uses.
This is the one module allowed to read the environment directly: secret
names are data in the control plane, so they cannot be declared as
`Settings` fields in advance.

**`keyvault`** reads Azure Key Vault through `DefaultAzureCredential`.
On Azure that is the managed identity, so no credential exists anywhere
to leak; on a laptop it falls through to `az login`.

The name mapping means the same secret name works in both backends, so
the control plane never needs to know which one is in use. Key Vault
names allow only letters, digits and dashes, which is why names are
dashed rather than underscored.

### Settings hold names, never values

`masking_key` became `masking_key_secret`; the two S3 values became
`s3_credentials_secret`; `database_url_secret` was added. A test walks
`Settings.model_fields` and fails if any field that looks like a
credential is not a `_secret` name, so the rule is now enforced rather
than remembered.

Every place that built a database engine from `settings.database_url`
— six of them — now calls `credentials.database_url()`, which uses the
secret when one is named.

### Values are `SecretStr`

A resolved secret's repr is `'**********'`. A value that reaches a log
line, an exception or a `task_run.error_message` through an f-string has
been redacted before anyone decided whether to print it. Error messages
name the secret and where it was looked for, never a value.

### A missing secret means different things in each backend

With `env`, a missing masking key falls back to the published
development key with a warning, so a fresh clone runs. With `keyvault`
it is a hard failure. A production run masking with a public key looks
like protection and is not; stopping is the honest outcome.

### The SDK is optional

`azure-identity` and `azure-keyvault-secrets` are the `[azure]` extra,
imported only when the `keyvault` backend is chosen, and the error when
they are missing says how to install them. The default install never
needs them — the same rule the Spark engine follows (design rule 9).
mypy passes with and without the SDK installed.

## Consequences

**One connection string remains in configuration, deliberately.** The
default `database_url` holds the compose stack's `app:app` credential.
It exists only inside a container on the developer's machine, and
removing it would mean a fresh clone cannot run without first writing a
`.env`. Anywhere that is not a laptop sets `database_url_secret` and the
default is never read. "Zero connection strings in code" is true for
everything that reaches a real system.

**Nothing consumes a source's `secret_name` yet.** The demo catalog's
only source reads Parquet from disk. `connection_string()` resolves a
`source_system.secret_name` and is tested, but its first real caller
arrives with the first JDBC or REST source.

**Key Vault is tested against a fake client.** No test reaches Azure.
The fake mirrors the SDK's surface — `get_secret(name).value` and
`ResourceNotFoundError` — but a change in the SDK would not be caught
here. A smoke test against a real vault needs a subscription the CI does
not have.

**Values are cached per store.** A rotated secret is picked up by the
next process, not the running one. For batch pipelines that is the right
trade; a long-lived service would need a TTL.

**gitleaks was re-run** over every ref (32 commits) and the working tree
as part of this step: no findings. The Makefile and CI now use
`gitleaks git --log-opts="--all"` rather than the deprecated `detect`,
which scanned only the checked-out branch.
