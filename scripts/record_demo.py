"""Record the README's demo GIF from a real run.

    python scripts/record_demo.py docs/images/demo.cast
    agg --speed 1.9 --idle-time-limit 1.5 --font-size 15 --theme monokai \\
        docs/images/demo.cast docs/images/demo.gif

Every line in the recording is the platform's genuine output, captured as
it runs; only the typing of each command is simulated. `agg` is
asciinema's GIF renderer (https://github.com/asciinema/agg). The run uses
a scratch directory and a SQLite control plane, so it needs no stack.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

WIDTH, HEIGHT = 150, 30
TYPING_DELAY = 0.035
MAX_LINE_DELAY = 0.08


class Recording:
    def __init__(self, workdir: Path) -> None:
        self.workdir = workdir
        self.events: list[tuple[float, str, str]] = []
        self.clock = 0.0
        self.env = dict(
            os.environ,
            DATABASE_URL=f"sqlite:///{workdir / 'control.db'}",
            LAKE_ROOT="lake",
            MASKING_KEY="demo",
            LOG_LEVEL="WARNING",
            PYTHONUNBUFFERED="1",
        )

    def emit(self, text: str, gap: float) -> None:
        self.clock += gap
        self.events.append((round(self.clock, 3), "o", text))

    def run(
        self, *args: str, keep: Callable[[str], bool] | None = None, pause: float = 1.2
    ) -> None:
        shown = "python -m " + " ".join(args)
        self.emit("\x1b[1;32m$\x1b[0m ", 0.4)
        for char in shown:
            self.emit(char, TYPING_DELAY)
        self.emit("\r\n", 0.3)

        started = time.perf_counter()
        # Fixed internal module names, never user input.
        proc = subprocess.run(  # noqa: S603
            [sys.executable, "-m", *args],
            env=self.env,
            cwd=self.workdir,
            capture_output=True,
            text=True,
            check=True,
        )
        elapsed = time.perf_counter() - started
        # JSON log lines are for machines; the demo shows what a person reads.
        lines = [x for x in (proc.stdout + proc.stderr).splitlines() if not x.startswith("{")]
        if keep is not None:
            lines = [x for x in lines if keep(x)]
        delay = min(elapsed / max(len(lines), 1), MAX_LINE_DELAY)
        for line in lines:
            self.emit(line + "\r\n", delay)
        self.emit("", pause)

    def save(self, path: Path) -> None:
        header = {"version": 2, "width": WIDTH, "height": HEIGHT, "title": "lakehouse"}
        with path.open("w", encoding="utf-8") as out:
            out.write(json.dumps(header) + "\n")
            for event in self.events:
                out.write(json.dumps(list(event)) + "\n")


def seed_summary(line: str) -> bool:
    return line.startswith(("customers", "orders", "order_items", "total", "written"))


def masked_columns(line: str) -> bool:
    return "→" in line or line.startswith("wrote")


def main() -> int:
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/images/demo.cast")
    workdir = Path(tempfile.mkdtemp(prefix="lakehouse-demo-"))
    try:
        rec = Recording(workdir)
        rec.run("lakehouse.seed", "--rows", "50000", keep=seed_summary)
        rec.run("lakehouse.pipeline", "--register")
        rec.run("lakehouse.pipeline", pause=1.8)
        rec.run(
            "lakehouse.privacy", "--apply", "--object", "customers", keep=masked_columns, pause=3.0
        )
        rec.save(target)
        print(f"{len(rec.events)} events, {rec.clock:.1f}s -> {target}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
