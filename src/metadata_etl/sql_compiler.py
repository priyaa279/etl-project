from __future__ import annotations

from pathlib import Path

from metadata_etl.config import ColumnConfig, ETLConfig
from metadata_etl.errors import ConfigError
from metadata_etl.transformations.registry import default_registry
from metadata_etl.transformations.sql import (
    DUCKDB_TYPES,
    cast_expression,
    quote_identifier,
    sql_literal,
)


def csv_relation(path: Path, delimiter: str) -> str:
    normalized_path = path.as_posix()
    return (
        "read_csv("
        f"{sql_literal(normalized_path)}, "
        "header = true, all_varchar = true, strict_mode = true, "
        f"delim = {sql_literal(delimiter)}"
        ")"
    )


def _normalized_source_expression(column: ColumnConfig, config: ETLConfig) -> str:
    expression = quote_identifier(column.source)
    if config.trim_strings or config.null_tokens:
        expression = f"CAST({expression} AS VARCHAR)"
    if config.trim_strings:
        expression = f"trim({expression})"
    if config.null_tokens:
        token_values = ", ".join(sql_literal(token) for token in config.null_tokens)
        expression = f"CASE WHEN {expression} IN ({token_values}) THEN NULL ELSE {expression} END"
    expression = cast_expression(expression, column.datatype, column.date_format)
    return f"{expression} AS {quote_identifier(column.name)}"


def compile_canonical_sql(
    config: ETLConfig,
    *,
    source_path: Path | None = None,
    source_relation: str | None = None,
    watermark_value: object | None = None,
    watermark_to: object | None = None,
) -> str:
    """Compile source normalization and the optional incremental boundary."""
    if source_relation is not None:
        relation = source_relation
    elif config.source_type == "csv":
        path = source_path or config.source_path
        if path is None:
            raise ConfigError("CSV source path is required")
        relation = csv_relation(path, config.delimiter)
    elif config.source_type == "parquet":
        path = source_path or config.source_path
        if path is None:
            raise ConfigError("Parquet source path is required")
        relation = f"read_parquet({sql_literal(path.as_posix())})"
    else:
        raise ConfigError(
            f"source_relation is required when compiling a {config.source_type} source"
        )
    projections = ",\n        ".join(
        _normalized_source_expression(column, config) for column in config.columns
    )
    query = f"SELECT\n        {projections}\n    FROM {relation}"
    predicates: list[str] = []
    if config.watermark is not None and watermark_value is not None:
        column = quote_identifier(config.watermark.column)
        threshold = (
            f"CAST({sql_literal(watermark_value)} AS {DUCKDB_TYPES[config.watermark.datatype]})"
        )
        predicates.append(f"{column} > {threshold}")
    if config.watermark is not None and watermark_to is not None:
        column = quote_identifier(config.watermark.column)
        threshold = (
            f"CAST({sql_literal(watermark_to)} AS {DUCKDB_TYPES[config.watermark.datatype]})"
        )
        predicates.append(f"{column} <= {threshold}")
    if predicates:
        query = f"SELECT * FROM (\n    {query}\n) WHERE {' AND '.join(predicates)}"
    return query


def compile_transform_sql(
    config: ETLConfig,
    *,
    source_path: Path | None = None,
    source_relation: str | None = None,
    watermark_value: object | None = None,
    watermark_to: object | None = None,
) -> str:
    """Compile the approved canonical schema and transforms into one DuckDB query."""
    canonical = compile_canonical_sql(
        config,
        source_path=source_path,
        source_relation=source_relation,
        watermark_value=watermark_value,
        watermark_to=watermark_to,
    )
    ctes = [f"canonical AS (\n    {canonical}\n)"]
    previous = "canonical"
    registry = default_registry()

    for index, transform in enumerate(config.transformations, start=1):
        cte_name = f"step_{index}_{transform.id.lower()}"
        statement = registry.compile(transform.type, transform.values, quote_identifier(previous))
        ctes.append(f"{quote_identifier(cte_name)} AS (\n    {statement}\n)")
        previous = cte_name

    return "WITH\n" + ",\n".join(ctes) + f"\nSELECT * FROM {quote_identifier(previous)}"
