from __future__ import annotations

import hashlib
import json
from typing import Any


def _stable_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict | list | tuple):
        return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))
    return str(value)


def protect_value(value: Any, policy: str) -> str | None:
    if value is None or policy == "none":
        return None
    text = _stable_text(value)
    if policy == "full":
        return text
    if policy == "hashed":
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return f"sha256:{digest}"
    if policy == "masked":
        if len(text) <= 2:
            return "*" * len(text)
        return text[0] + ("*" * (len(text) - 2)) + text[-1]
    raise AssertionError(f"Unhandled quarantine policy: {policy}")
