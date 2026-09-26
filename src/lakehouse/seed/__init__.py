"""Synthetic source data, so the platform runs with no cloud account."""

from lakehouse.seed.generator import SeedConfig, generate, summarise, write

__all__ = ["SeedConfig", "generate", "summarise", "write"]
