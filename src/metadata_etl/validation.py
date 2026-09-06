from __future__ import annotations

import duckdb

from metadata_etl.config import ETLConfig
from metadata_etl.errors import ConfigError
from metadata_etl.sql_compiler import compile_transform_sql


def validate_plan(config: ETLConfig) -> None:
    """Bind the compiled plan to the source schema without loading PostgreSQL."""
    try:
        with duckdb.connect(":memory:") as connection:
            connection.execute(f"DESCRIBE ({compile_transform_sql(config)})").fetchall()
    except duckdb.Error as exc:
        raise ConfigError(f"Transformation plan is not executable: {exc}") from exc
