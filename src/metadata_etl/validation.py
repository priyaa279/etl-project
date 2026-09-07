from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

import duckdb

from metadata_etl.config import ETLConfig, with_source_override
from metadata_etl.connectors import default_connector_registry
from metadata_etl.errors import ConfigError
from metadata_etl.schema import (
    DriftResult,
    SchemaFingerprint,
    canonical_schema_fingerprint,
    detect_schema_drift,
)
from metadata_etl.sql_compiler import compile_canonical_sql, compile_transform_sql


@dataclass(frozen=True)
class SourcePreflight:
    raw_schema: SchemaFingerprint
    canonical_schema: SchemaFingerprint
    drift: DriftResult
    canonical_compatible: bool


def validate_plan(config: ETLConfig, *, source_override: str | Path | None = None) -> None:
    """Bind the compiled plan to the source schema without loading PostgreSQL."""
    try:
        if source_override is not None:
            config = with_source_override(config, source_override)
        with tempfile.TemporaryDirectory(prefix=".etl-validate-") as temp:
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


def preflight_source(
    config: ETLConfig,
    source_override: str | Path,
    *,
    previous_raw: SchemaFingerprint | None,
    previous_canonical: SchemaFingerprint | None,
) -> SourcePreflight:
    """Inspect one source using normal framework components without durable ETL side effects."""
    runtime_config = with_source_override(config, source_override)
    with tempfile.TemporaryDirectory(prefix=".etl-preflight-") as temp:
        extracted = (
            default_connector_registry()
            .get(runtime_config.source_type)
            .extract(runtime_config, "PREFLIGHT", Path(temp))
        )
        canonical = canonical_schema_fingerprint(runtime_config.columns)
        drift = detect_schema_drift(
            previous_raw,
            extracted.raw_schema,
            previous_canonical,
            canonical,
            runtime_config.schema_drift,
        )
        try:
            with duckdb.connect(":memory:") as connection:
                canonical_query = compile_canonical_sql(
                    runtime_config, source_relation=extracted.relation_sql
                )
                transform_query = compile_transform_sql(
                    runtime_config, source_relation=extracted.relation_sql
                )
                connection.execute(f"DESCRIBE ({canonical_query})").fetchall()
                connection.execute(f"DESCRIBE ({transform_query})").fetchall()
        except duckdb.Error as exc:
            if drift.failed:
                return SourcePreflight(extracted.raw_schema, canonical, drift, False)
            raise ConfigError(
                f"Uploaded source is incompatible with the approved plan: {exc}"
            ) from exc
    return SourcePreflight(extracted.raw_schema, canonical, drift, True)
