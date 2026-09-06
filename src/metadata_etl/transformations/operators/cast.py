from __future__ import annotations

from typing import Any

from metadata_etl.errors import ConfigError
from metadata_etl.transformations.operators.base import (
    Schema,
    TransformationOperator,
    require_column,
    require_datatype,
    require_string,
)
from metadata_etl.transformations.sql import cast_expression, quote_identifier


class CastOperator(TransformationOperator):
    name = "cast"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        column = require_column(spec.get("column"), f"{path}.column", schema)
        datatype = require_datatype(spec.get("datatype"), f"{path}.datatype")
        date_format = spec.get("format")
        if date_format is not None:
            date_format = require_string(date_format, f"{path}.format")
        if datatype == "date" and schema[column] == "string" and not date_format:
            raise ConfigError(f"{path}.format is required when casting a string to date")
        values.update(column=column, datatype=datatype, format=date_format)
        schema[column] = datatype
        return values

    def compile(self, spec: dict[str, Any], input_relation: str) -> str:
        column = quote_identifier(spec["column"])
        expression = cast_expression(column, spec["datatype"], spec.get("format"))
        return f"SELECT * REPLACE ({expression} AS {column}) FROM {input_relation}"
