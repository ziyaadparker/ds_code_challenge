"""A tiny timing context manager used across all task scripts.

The README explicitly asks each data-engineering task to "log the time
taken to perform the operations described". Centralising this in one place
keeps every script's timing log line consistently formatted.
"""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def timer(step_name: str, logger: logging.Logger) -> Iterator[None]:
    """Log how long the wrapped block took, in seconds (3 d.p.)."""
    start = time.perf_counter()
    logger.info("STARTING  %s", step_name)
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info("COMPLETED %s in %.3fs", step_name, elapsed)
