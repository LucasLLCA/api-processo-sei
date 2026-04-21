"""Standard logging configuration for pipeline scripts.

Matches the format previously duplicated in every script
(`%(asctime)s [%(levelname)s] %(message)s`).
"""

from __future__ import annotations

import logging

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(name: str | None = None, level: int | str = logging.INFO) -> logging.Logger:
    """Configure the root logger once and return a child logger.

    Safe to call more than once: re-invocations only update the level.
    """
    if isinstance(level, str):
        level = logging.getLevelName(level.upper())

    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format=_LOG_FORMAT, datefmt=_DATE_FORMAT)
    else:
        root.setLevel(level)

    return logging.getLogger(name) if name else root
