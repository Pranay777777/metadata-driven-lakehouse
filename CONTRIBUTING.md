# Contributing

## Setup

```bash
make install      # editable install with dev extras, plus git hooks
make up           # start the local stack
make test
```

## Before opening a pull request

```bash
make format lint typecheck test security
```

CI runs the same gates, so a green local run means a green PR.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `perf:`, `ci:`.

Small, focused commits. The git history is part of the project's documentation.

## Architecture decisions

Anything expensive to reverse gets an ADR in `docs/adr/`, copied from
`0000-template.md`.
