# Security Policy

## Reporting a vulnerability

Please open a [private security advisory](../../security/advisories/new) rather
than a public issue. Expect an initial response within 72 hours.

## What this project does

- Secrets are read from the environment via typed settings; none are committed.
  `.env` is gitignored and `.env.example` documents the required keys.
- `gitleaks` runs over the **full git history** on every CI run, not just the
  working tree, using the allowlist in `.gitleaks.toml`.
- `pip-audit` fails the build on known-vulnerable dependencies.
- Dependabot opens weekly dependency PRs.
- The container runs as an unprivileged user (uid 10001), never root.
- Secret scanning push protection is enabled on the repository.

## If a credential is ever exposed

1. **Rotate it first.** History rewriting does not undo exposure.
2. Verify it is real — scanners produce false positives.
3. Rewrite history with `git filter-repo`, then force-push.
4. Ask GitHub Support to purge cached views of the affected commits.
