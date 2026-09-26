"""Tests for structured logging."""

from __future__ import annotations

import json
import logging

from app.logging import JsonFormatter


def test_formatter_emits_valid_json() -> None:
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello %s",
        args=("world",),
        exc_info=None,
    )
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "hello world"
    assert payload["level"] == "INFO"
