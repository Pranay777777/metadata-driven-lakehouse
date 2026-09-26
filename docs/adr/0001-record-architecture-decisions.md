# ADR-001: Record architecture decisions

- **Status:** accepted
- **Date:** 2026-09-26

## Context

Design choices get made once and forgotten. Six months later nobody remembers
why the project uses one queue over another, so the reasoning is reconstructed
badly or the decision is reversed by accident.

## Options considered

1. **Architecture Decision Records** — one short markdown file per decision,
   versioned next to the code.
2. **A wiki page** — easy to write, drifts from the code, no review.
3. **Nothing** — rely on commit messages and memory.

## Decision

Use ADRs in `docs/adr/`, numbered sequentially, following `0000-template.md`.
Any decision that would be expensive to reverse gets one.

## Consequences

**Gained:** the reasoning survives; reviewers can challenge the decision rather
than the code; new readers understand the shape of the system quickly.

**Given up:** roughly twenty minutes per significant decision.

**Revisit when:** ADRs stop being written, which means the format is too heavy.
