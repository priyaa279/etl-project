from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from metadata_etl.errors import ConfigError

DUCKDB_TYPES = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "decimal": "DECIMAL(38, 6)",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "boolean": "BOOLEAN",
}


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | Decimal):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigError("Transformation literals cannot be NaN or infinite")
        return repr(value)
    if isinstance(value, datetime | date):
        value = value.isoformat()
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ConfigError(f"Unsupported transformation literal: {value!r}")


def cast_expression(expression: str, datatype: str, date_format: str | None = None) -> str:
    if datatype == "date" and date_format:
        return f"CAST(strptime({expression}, {sql_literal(date_format)}) AS DATE)"
    if datatype == "timestamp" and date_format:
        return f"CAST(strptime({expression}, {sql_literal(date_format)}) AS TIMESTAMP)"
    return f"CAST({expression} AS {DUCKDB_TYPES[datatype]})"
