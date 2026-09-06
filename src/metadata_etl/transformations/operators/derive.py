from __future__ import annotations

from typing import Any

from metadata_etl.errors import ConfigError
from metadata_etl.transformations.operators.base import (
    Schema,
    TransformationOperator,
    require_datatype,
    require_identifier,
    require_string,
    validate_expression,
)
from metadata_etl.transformations.sql import cast_expression, quote_identifier


class DeriveOperator(TransformationOperator):
    name = "derive"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        target = require_identifier(spec.get("target_column"), f"{path}.target_column")
        if target in schema:
            raise ConfigError(f"{path}.target_column already exists: {target}")
        datatype = require_datatype(spec.get("datatype"), f"{path}.datatype")
        date_format = spec.get("format")
        if date_format is not None:
            date_format = require_string(date_format, f"{path}.format")
        values.update(
            target_column=target,
            datatype=datatype,
            expression=validate_expression(spec.get("expression"), f"{path}.expression"),
            format=date_format,
        )
        schema[target] = datatype
        return values

    def compile(self, spec: dict[str, Any], input_relation: str) -> str:
        target = quote_identifier(spec["target_column"])
        expression = cast_expression(
            f"({spec['expression']})", spec["datatype"], spec.get("format")
        )
        return f"SELECT *, {expression} AS {target} FROM {input_relation}"
