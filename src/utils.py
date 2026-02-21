"""Shared utilities used across the pipeline."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


def utc_now() -> datetime:
    """Return a timezone-aware ``datetime`` representing the current UTC time."""

    return datetime.now(timezone.utc)


def setup_logging(verbose: bool = False) -> None:
    """Configure basic logging for CLI scripts."""

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@contextmanager
def log_exceptions(logger: logging.Logger, context: str) -> Iterator[None]:
    """Context manager to log unexpected exceptions before re-raising."""

    try:
        yield
    except Exception:  # pragma: no cover - strict re-raise after logging
        logger.exception("Unhandled exception while %s", context)
        raise

