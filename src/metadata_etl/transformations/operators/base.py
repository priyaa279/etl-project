from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any

from metadata_etl.errors import ConfigError

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FORBIDDEN_SQL = re.compile(
    r"(;|--|/\*|\*/|\b(attach|copy|create|delete|drop|insert|install|load|pragma|update)\b)",
    re.IGNORECASE,
)
SUPPORTED_TYPES = {"string", "integer", "decimal", "date", "timestamp", "boolean"}
Schema = dict[str, str]


def require_mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must be a YAML mapping")
    return value


def require_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path} must be a non-empty string")
    return value.strip()


def require_identifier(value: Any, path: str) -> str:
    name = require_string(value, path)
    if not IDENTIFIER.fullmatch(name):
        raise ConfigError(f"{path} must be a safe SQL identifier; got {name!r}")
    return name


def require_column(value: Any, path: str, schema: Schema) -> str:
    column = require_identifier(value, path)
    if column not in schema:
        raise ConfigError(f"{path} references unknown column {column!r}")
    return column


def require_datatype(value: Any, path: str) -> str:
    datatype = require_string(value, path).lower()
    if datatype not in SUPPORTED_TYPES:
        raise ConfigError(f"{path} must be one of {sorted(SUPPORTED_TYPES)}")
    return datatype


def validate_expression(value: Any, path: str) -> str:
    expression = require_string(value, path)
    if FORBIDDEN_SQL.search(expression):
        raise ConfigError(f"{path} contains a forbidden SQL token")
    return expression


def require_scalar(value: Any, path: str) -> Any:
    if isinstance(value, dict | list | tuple | set):
        raise ConfigError(f"{path} must be a scalar value or null")
    return value


class TransformationOperator(ABC):
    name: str

    @abstractmethod
    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        """Validate and normalize config, updating the known output schema when needed."""

    @abstractmethod
    def compile(self, spec: dict[str, Any], input_relation: str) -> str:
        """Compile this operator into a SELECT against input_relation."""
