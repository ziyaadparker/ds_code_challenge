"""Shared logging configuration.

Every task script calls :func:`get_logger` instead of configuring logging
itself, so all scripts write to both the console and a per-script timestamped
log file under ``logs/`` with a consistent format. This is what the README's
"include logging" requirements (tasks 1 and 2) refer to.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from src.common.config import LOG_DIR

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a logger named ``name`` that logs to console + logs/<name>.log.

    Safe to call multiple times for the same name (e.g. in tests) - handlers
    are only attached once.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = LOG_DIR / f"{name}_{timestamp}.log"
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    logger.info("Logging to %s", log_path)
    return logger
