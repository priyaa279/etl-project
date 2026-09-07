from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

from metadata_etl.config import ETLConfig
from metadata_etl.connectors import default_connector_registry
from metadata_etl.errors import ConfigError
from metadata_etl.sql_compiler import compile_transform_sql


def validate_plan(config: ETLConfig) -> None:
    """Bind the compiled plan to the source schema without loading PostgreSQL."""
    try:
        with tempfile.TemporaryDirectory(prefix=".etl-validate-", dir=config.path.parent) as temp:
            extracted = (
                default_connector_registry()
                .get(config.source_type)
                .extract(config, "VALIDATE", Path(temp))
            )
            with duckdb.connect(":memory:") as connection:
                query = compile_transform_sql(config, source_relation=extracted.relation_sql)
                connection.execute(f"DESCRIBE ({query})").fetchall()
    except duckdb.Error as exc:
        raise ConfigError(f"Transformation plan is not executable: {exc}") from exc
