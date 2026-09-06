from __future__ import annotations

from pathlib import Path

from metadata_etl.config import ColumnConfig, ETLConfig

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


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
    if column.datatype == "date":
        expression = (
            f"CAST(strptime({expression}, {sql_literal(column.date_format or '')}) AS DATE)"
        )
    else:
        expression = f"CAST({expression} AS {DUCKDB_TYPES[column.datatype]})"
    return f"{expression} AS {quote_identifier(column.name)}"


def compile_transform_sql(config: ETLConfig, *, source_path: Path | None = None) -> str:
    """Compile the approved canonical schema and transforms into one DuckDB query."""
    relation = csv_relation(source_path or config.source_path, config.delimiter)
    projections = ",\n        ".join(
        _normalized_source_expression(column, config) for column in config.columns
    )
    ctes = [f"canonical AS (\n    SELECT\n        {projections}\n    FROM {relation}\n)"]
    previous = "canonical"

    for index, transform in enumerate(config.transformations, start=1):
        cte_name = f"step_{index}_{transform.id.lower()}"
        if transform.type == "filter":
            statement = (
                f"SELECT * FROM {quote_identifier(previous)} WHERE {transform.values['condition']}"
            )
        elif transform.type == "derive":
            datatype = DUCKDB_TYPES[transform.values["datatype"]]
            target = quote_identifier(transform.values["target_column"])
            statement = (
                f"SELECT *, CAST(({transform.values['expression']}) AS {datatype}) AS {target} "
                f"FROM {quote_identifier(previous)}"
            )
        else:  # protected by config validation
            raise AssertionError(f"Unhandled transformation type: {transform.type}")
        ctes.append(f"{quote_identifier(cte_name)} AS (\n    {statement}\n)")
        previous = cte_name

    return "WITH\n" + ",\n".join(ctes) + f"\nSELECT * FROM {quote_identifier(previous)}"
