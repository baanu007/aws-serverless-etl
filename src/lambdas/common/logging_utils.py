"""Shared logging helpers for Lambda handlers.

Uses the standard library `logging` module with a JSON-friendly format so
CloudWatch Logs Insights queries stay simple. If `aws-lambda-powertools`
is installed in the runtime, callers may swap in `Logger` from there;
this module purposely avoids importing it to keep cold starts cheap.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Mapping


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger.

    Log level is taken from the LOG_LEVEL env var (defaults to INFO).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, level_name, logging.INFO))
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, event: Mapping[str, Any]) -> None:
    """Log a Lambda event safely (truncate large bodies)."""
    try:
        payload = json.dumps(event, default=str)
    except (TypeError, ValueError):
        payload = repr(event)
    if len(payload) > 4096:
        payload = payload[:4096] + "...[truncated]"
    logger.info("event=%s", payload)
