from __future__ import annotations

from typing import Any

from metadata_etl.transformations.operators.base import (
    Schema,
    TransformationOperator,
    validate_expression,
)


class FilterOperator(TransformationOperator):
    name = "filter"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        values["condition"] = validate_expression(spec.get("condition"), f"{path}.condition")
        return values

    def compile(self, spec: dict[str, Any], input_relation: str) -> str:
        return f"SELECT * FROM {input_relation} WHERE {spec['condition']}"
