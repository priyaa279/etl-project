from __future__ import annotations

from typing import Any

from metadata_etl.errors import ConfigError
from metadata_etl.transformations.operators.base import (
    Schema,
    TransformationOperator,
    require_column,
    require_mapping,
)
from metadata_etl.transformations.sql import quote_identifier


class DeduplicateOperator(TransformationOperator):
    name = "deduplicate"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        keys_value = spec.get("keys")
        if not isinstance(keys_value, list) or not keys_value:
            raise ConfigError(f"{path}.keys must be a non-empty list")
        keys = [
            require_column(value, f"{path}.keys[{index}]", schema)
            for index, value in enumerate(keys_value)
        ]
        if len(set(keys)) != len(keys):
            raise ConfigError(f"{path}.keys cannot contain duplicate columns")

        order_value = require_mapping(spec.get("order_by"), f"{path}.order_by")
        if not order_value:
            raise ConfigError(f"{path}.order_by must contain at least one column")
        order_by: dict[str, str] = {}
        for column_value, direction_value in order_value.items():
            column = require_column(column_value, f"{path}.order_by column", schema)
            if not isinstance(direction_value, str) or direction_value.lower() not in {
                "asc",
                "desc",
            }:
                raise ConfigError(f"{path}.order_by.{column} must be asc or desc")
            order_by[column] = direction_value.lower()
        values.update(keys=keys, order_by=order_by)
        return values

    def compile(self, spec: dict[str, Any], input_relation: str) -> str:
        keys = ", ".join(quote_identifier(column) for column in spec["keys"])
        order_by = ", ".join(
            f"{quote_identifier(column)} {direction.upper()}"
            for column, direction in spec["order_by"].items()
        )
        marker = quote_identifier(f"__etl_row_number_{spec['id'].lower()}")
        return (
            f"SELECT * EXCLUDE ({marker}) FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {order_by}) AS {marker} "
            f"FROM {input_relation}"
            f") WHERE {marker} = 1"
        )
