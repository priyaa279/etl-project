from __future__ import annotations

from pathlib import Path

from metadata_etl.config import ColumnConfig, ETLConfig
from metadata_etl.transformations.registry import default_registry
from metadata_etl.transformations.sql import cast_expression, quote_identifier, sql_literal


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
    if config.trim_strings:
        expression = f"trim({expression})"
    if config.null_tokens:
        token_values = ", ".join(sql_literal(token) for token in config.null_tokens)
        expression = f"CASE WHEN {expression} IN ({token_values}) THEN NULL ELSE {expression} END"
    expression = cast_expression(expression, column.datatype, column.date_format)
    return f"{expression} AS {quote_identifier(column.name)}"


def compile_transform_sql(config: ETLConfig, *, source_path: Path | None = None) -> str:
    """Compile the approved canonical schema and transforms into one DuckDB query."""
    relation = csv_relation(source_path or config.source_path, config.delimiter)
    projections = ",\n        ".join(
        _normalized_source_expression(column, config) for column in config.columns
    )
    ctes = [f"canonical AS (\n    SELECT\n        {projections}\n    FROM {relation}\n)"]
    previous = "canonical"
    registry = default_registry()

    for index, transform in enumerate(config.transformations, start=1):
        cte_name = f"step_{index}_{transform.id.lower()}"
        statement = registry.compile(transform.type, transform.values, quote_identifier(previous))
        ctes.append(f"{quote_identifier(cte_name)} AS (\n    {statement}\n)")
        previous = cte_name

    return "WITH\n" + ",\n".join(ctes) + f"\nSELECT * FROM {quote_identifier(previous)}"
