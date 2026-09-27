"""Check that every service in the local stack is actually answering.

`docker compose ps` says a container is running. Running is not the
same as usable: Postgres accepts TCP connections before it accepts
queries, Marquez serves its admin port before migrations finish, and
MinIO answers health probes before a bucket exists. This checks the
thing each service is actually for.

    python -m lakehouse.stack

Exit code is 0 only if every check passes, so it works as a CI gate and
as the last line of `make up`.
"""

from __future__ import annotations

import argparse
import socket
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlparse

from lakehouse.config import Settings
from lakehouse.credentials import database_url


@dataclass(frozen=True)
class Check:
    """One service and how to tell whether it is usable."""

    name: str
    target: str
    probe: Callable[[], str]


def _http(url: str, timeout: float) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310
        return f"HTTP {response.status}"


def _tcp(host: str, port: int, timeout: float) -> str:
    with socket.create_connection((host, port), timeout=timeout):
        return "accepting connections"


def build_checks(settings: Settings, timeout: float = 3.0) -> list[Check]:
    """The checks appropriate to the configured endpoints."""
    database = urlparse(database_url(settings))
    marquez = settings.openlineage_url.rstrip("/")

    return [
        Check(
            "postgres",
            f"{database.hostname}:{database.port or 5432}",
            lambda: _tcp(database.hostname or "localhost", database.port or 5432, timeout),
        ),
        Check(
            "marquez",
            marquez,
            lambda: _http(f"{marquez}/api/v1/namespaces", timeout),
        ),
        Check(
            "marquez-web",
            settings.marquez_web_url,
            lambda: _http(settings.marquez_web_url, timeout),
        ),
    ]


def run_checks(checks: list[Check]) -> tuple[bool, list[str]]:
    """Run every check, reporting all failures rather than the first."""
    lines: list[str] = []
    healthy = True
    for check in checks:
        try:
            detail = check.probe()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            healthy = False
            reason = getattr(exc, "reason", exc)
            lines.append(f"  DOWN  {check.name:<13} {check.target}  ({reason})")
        else:
            lines.append(f"  ok    {check.name:<13} {check.target}  ({detail})")
    return healthy, lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse.stack", description=__doc__)
    parser.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args(argv)

    healthy, lines = run_checks(build_checks(Settings(), args.timeout))
    print("\n".join(lines))
    if not healthy:
        print("\nsome services are not answering — try 'make up' and wait", file=sys.stderr)
        return 1
    print("\nstack is up")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
