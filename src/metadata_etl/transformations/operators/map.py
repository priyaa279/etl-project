from __future__ import annotations

from typing import Any

from metadata_etl.errors import ConfigError
from metadata_etl.transformations.operators.base import (
    Schema,
    TransformationOperator,
    require_column,
    require_datatype,
    require_mapping,
    require_scalar,
)
from metadata_etl.transformations.sql import (
    DUCKDB_TYPES,
    cast_expression,
    quote_identifier,
    sql_literal,
)


class MapOperator(TransformationOperator):
    name = "map"

    def validate(self, spec: dict[str, Any], path: str, schema: Schema) -> dict[str, Any]:
        values = dict(spec)
        column = require_column(spec.get("column"), f"{path}.column", schema)
        mappings = require_mapping(spec.get("mappings"), f"{path}.mappings")
        if not mappings:
            raise ConfigError(f"{path}.mappings must contain at least one mapping")
        for source, target in mappings.items():
            require_scalar(source, f"{path}.mappings key")
            require_scalar(target, f"{path}.mappings[{source!r}]")
        datatype = spec.get("datatype", schema[column])
        datatype = require_datatype(datatype, f"{path}.datatype")
        if "default" in spec:
            require_scalar(spec["default"], f"{path}.default")
        values.update(
            column=column,
            mappings=dict(mappings),
            datatype=datatype,
            source_datatype=schema[column],
        )
        schema[column] = datatype
        return values

    def compile(self, spec: dict[str, Any], input_relation: str) -> str:
        column = quote_identifier(spec["column"])
        source_type = DUCKDB_TYPES[spec["source_datatype"]]
        target_type = spec["datatype"]
        clauses: list[str] = []
        for source, target in spec["mappings"].items():
            if source is None:
                predicate = f"{column} IS NULL"
            else:
                predicate = f"{column} = CAST({sql_literal(source)} AS {source_type})"
            result = cast_expression(sql_literal(target), target_type)
            clauses.append(f"WHEN {predicate} THEN {result}")
        if "default" in spec:
            fallback = cast_expression(sql_literal(spec["default"]), target_type)
        else:
            fallback = cast_expression(column, target_type)
        expression = "CASE " + " ".join(clauses) + f" ELSE {fallback} END"
        return f"SELECT * REPLACE ({expression} AS {column}) FROM {input_relation}"
