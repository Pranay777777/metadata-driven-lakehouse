# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] — 2026-09-28

The first complete release: every layer, the controls around them, and
the evidence that they work. Release notes: [docs/releases/v1.0.0.md](docs/releases/v1.0.0.md).

### Added
- **Metadata control plane** — eleven constrained SQLAlchemy models, DDL
  generated for Postgres and Azure SQL (ADR-002), with Alembic migrations
  that adopt pre-migration databases (ADR-020).
- **Bronze** — full, incremental (watermark + late-arrival grace window) and
  CDC loads; schema-drift detection with additive evolution (ADR-004, 007).
- **Silver** — conformance, deduplication, SCD Type 2 with tombstones (ADR-005).
- **Gold** — config-driven star schema with surrogate keys and unknown
  members (ADR-006).
- **Data quality** — YAML data contracts compiled into rules with warn /
  quarantine / fail severities; quarantine as a table (ADR-008, 009, 010).
- **PII** — scanner that proposes classifications; keyed-HMAC, partial and
  redact masking in Silver; sensitive columns withheld from Gold (ADR-017).
- **Secrets by name** — env or Azure Key Vault via managed identity (ADR-018).
- **Lineage** — column-level OpenLineage events to Marquez (ADR-012).
- **Orchestration** — Dagster assets generated from the catalog (ADR-013).
- **Engines** — Arrow + delta-rs by default, Spark behind the same
  interface, tested to agree (ADR-003, 014).
- **Operations** — compaction and Z-ordering (ADR-015), replay (ADR-016),
  Streamlit ops dashboard over the audit trail (ADR-019).
- **Evidence** — 499 tests at 96% coverage; Postgres integration tests;
  CI on Ubuntu 3.11/3.12 and Windows 3.12; reproducible benchmarks and
  demo recording in `scripts/`.

### Fixed
- Silver and Gold runs were never closed, leaving every run `running`;
  `audit --close-abandoned` repairs existing control planes.
- Sub-second task durations were stored as 0 (migration 0002).
- mypy was pinned to Python 3.11 and failed in 3.12 environments.

[Unreleased]: https://github.com/Pranay777777/metadata-driven-lakehouse/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Pranay777777/metadata-driven-lakehouse/releases/tag/v1.0.0
