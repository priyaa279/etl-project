from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import UTC, datetime
from typing import Any, TextIO

LOGGER_NAME = "metadata_etl"
_URI_CREDENTIALS = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)[^\s/@:]+:[^\s/@]+@", re.IGNORECASE)
_SENSITIVE_KEYS = ("password", "secret", "token", "credential", "dsn")
_LOGGER = logging.getLogger(LOGGER_NAME)
_LOGGER.addHandler(logging.NullHandler())
_LOGGER.propagate = False


def redact_text(value: object) -> str:
    """Remove credentials from a value before it reaches logs or operational metadata."""
    text = str(value)
    text = _URI_CREDENTIALS.sub(r"\g<scheme>***:***@", text)
    for name, secret in os.environ.items():
        if secret and any(marker in name.lower() for marker in _SENSITIVE_KEYS):
            text = text.replace(secret, "***")
    return text


def _safe_context(values: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if any(marker in key.lower() for marker in _SENSITIVE_KEYS):
            safe[key] = "***"
        elif isinstance(value, str):
            safe[key] = redact_text(value)
        else:
            safe[key] = value
    return safe


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "message": redact_text(record.getMessage()),
        }
        payload.update(_safe_context(getattr(record, "etl_context", {})))
        return json.dumps(payload, default=str, sort_keys=True)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        context = _safe_context(getattr(record, "etl_context", {}))
        details = " ".join(f"{key}={value}" for key, value in sorted(context.items()))
        suffix = f" {details}" if details else ""
        return f"{record.levelname} {redact_text(record.getMessage())}{suffix}"


def configure_logging(
    *,
    json_output: bool | None = None,
    level: str | None = None,
    stream: TextIO | None = None,
) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel((level or os.getenv("ETL_LOG_LEVEL", "INFO")).upper())
    handler = logging.StreamHandler(stream or sys.stderr)
    if json_output is None:
        json_output = os.getenv("ETL_LOG_FORMAT", "json").lower() == "json"
    handler.setFormatter(JsonFormatter() if json_output else TextFormatter())
    logger.addHandler(handler)
    return logger


def emit_event(event: str, *, level: int = logging.INFO, **context: Any) -> None:
    logging.getLogger(LOGGER_NAME).log(
        level,
        event,
        extra={"etl_context": {"event": event, **context}},
    )
