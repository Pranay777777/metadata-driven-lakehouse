"""Entry point: python -m app"""

from __future__ import annotations

import logging

from app.config import get_settings
from app.logging import configure_logging


def main() -> None:
    """Start the application."""
    settings = get_settings()
    configure_logging(settings.log_level)
    logging.getLogger(__name__).info("started in %s", settings.app_env)


if __name__ == "__main__":
    main()
