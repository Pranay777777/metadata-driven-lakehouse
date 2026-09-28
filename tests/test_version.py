"""The package and its metadata report the same version."""

from __future__ import annotations

import tomllib
from pathlib import Path

import lakehouse


def test_the_version_is_declared_once_in_spirit() -> None:
    """pyproject.toml and __version__ must agree, or a release lies about itself."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    declared = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["version"]
    assert lakehouse.__version__ == declared
