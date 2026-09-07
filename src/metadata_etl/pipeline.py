from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from metadata_etl.config import ETLConfig, load_config
from metadata_etl.errors import ETLError, ExtractionError, SchemaDriftError
from metadata_etl.postgres import LoadMetrics, PostgresStore
from metadata_etl.quality.engine import evaluate_contracts
from metadata_etl.schema import (
    SchemaFingerprint,
    canonical_schema_fingerprint,
    detect_schema_drift,
    raw_csv_schema_fingerprint,
)
from metadata_etl.source import RawArtifact, preserve_raw_copy
from metadata_etl.sql_compiler import compile_canonical_sql, compile_transform_sql
from metadata_etl.transformations.sql import quote_identifier


@dataclass(frozen=True)
class RunResult:
    run_id: str
    dataset: str
    status: str
    config_hash: str
    git_commit_sha: str
    raw_path: str
    raw_sha256: str
    rows_extracted: int
    rows_transformed: int
    rows_contract_passed: int
    rows_quarantined: int
    rows_loaded: int
    rows_inserted: int
    rows_updated: int
    raw_schema_hash: str
    canonical_schema_hash: str
    drift_status: str
    watermark_before: str | None
    watermark_after: str | None
    duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _create_run_id() -> str:
    now = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"RUN_{now}_{uuid.uuid4().hex[:8].upper()}"


def _git_sha(config_path: Path) -> str:
    git = shutil.which("git")
    if not git:
        return "UNAVAILABLE"
    try:
        result = subprocess.run(
            [git, "-C", str(config_path.parent), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return "UNAVAILABLE"
    return result.stdout.strip() or "UNAVAILABLE"


def _extract_and_transform(
    config: ETLConfig, raw: RawArtifact, watermark_value: object | None = None
) -> tuple[int, list[tuple[str, Any]], list[tuple[Any, ...]], str | None]:
    canonical_query = compile_canonical_sql(
        config, source_path=raw.path, watermark_value=watermark_value
    )
    query = compile_transform_sql(config, source_path=raw.path, watermark_value=watermark_value)
    try:
        with duckdb.connect(":memory:") as connection:
            if config.watermark is not None:
                watermark_column = quote_identifier(config.watermark.column)
                extracted_row = connection.execute(
                    f"SELECT count(*), max({watermark_column}) FROM ({canonical_query})"
                ).fetchone()
            else:
                extracted_row = connection.execute(
                    f"SELECT count(*) FROM ({canonical_query})"
                ).fetchone()
            cursor = connection.execute(query)
            description = cursor.description
            rows = cursor.fetchall()
    except duckdb.Error as exc:
        raise ExtractionError(f"CSV normalization or transformation failed: {exc}") from exc

    if extracted_row is None or description is None:
        raise ExtractionError("CSV query did not return a result")
    output_columns = [(item[0], item[1]) for item in description]
    positions = {name: index for index, (name, _) in enumerate(output_columns)}
    contract_not_null_columns = {
        contract.values["column"] for contract in config.contracts if contract.type == "not_null"
    }
    for column in config.columns:
        if not column.nullable and column.name not in contract_not_null_columns:
            null_count = sum(row[positions[column.name]] is None for row in rows)
            if null_count:
                raise ExtractionError(
                    f"Column {column.name!r} is not nullable but contains {null_count} null value(s)"
                )
    candidate_watermark = _watermark_text(extracted_row[1]) if len(extracted_row) > 1 else None
    return int(extracted_row[0]), output_columns, rows, candidate_watermark


def _watermark_text(value: Any | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime | date):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _load(
    store: PostgresStore,
    config: ETLConfig,
    output_columns: list[tuple[str, Any]],
    rows: list[tuple[Any, ...]],
    run_id: str,
    candidate_watermark: str | None,
) -> LoadMetrics:
    common = {
        "destination_schema": config.destination_schema,
        "staging_table": config.staging_table,
        "target_table": config.target_table,
        "columns": output_columns,
        "rows": rows,
        "run_id": run_id,
    }
    if config.load_strategy == "full":
        return store.publish_full(**common)
    if config.load_strategy == "incremental":
        assert config.watermark is not None
        return store.publish_incremental(
            **common,
            dataset=config.dataset,
            watermark_column=config.watermark.column,
            watermark_after=candidate_watermark,
            updated_at=datetime.now(UTC),
        )
    return store.publish_upsert(**common, keys=config.load_keys)


def run_pipeline(config_path: str | Path) -> RunResult:
    """Execute an approved CSV configuration through the generic ETL runtime."""
    config = load_config(config_path)
    run_id = _create_run_id()
    git_sha = _git_sha(config.path)
    started_at = datetime.now(UTC)
    timer = time.perf_counter()
    raw: RawArtifact | None = None
    rows_extracted: int | None = None
    rows_transformed: int | None = None
    rows_contract_passed: int | None = None
    rows_quarantined: int | None = None
    raw_schema: SchemaFingerprint | None = None
    canonical_schema: SchemaFingerprint | None = None
    drift_status: str | None = None
    watermark_before: str | None = None
    watermark_after: str | None = None

    with PostgresStore(config.dsn) as store:
        store.ensure_metadata_tables()
        store.start_run(
            run_id=run_id,
            dataset=config.dataset,
            schema_version=config.schema_version,
            config_hash=config.config_hash,
            git_sha=git_sha,
            started_at=started_at,
        )
        try:
            raw = preserve_raw_copy(config.source_path, config.raw_root, config.dataset, run_id)
            raw_schema = raw_csv_schema_fingerprint(raw.path, config.delimiter)
            canonical_schema = canonical_schema_fingerprint(config.columns)
            previous_raw_json, previous_canonical_json = store.get_previous_successful_schemas(
                config.dataset
            )
            previous_raw = (
                SchemaFingerprint.from_json(previous_raw_json) if previous_raw_json else None
            )
            previous_canonical = (
                SchemaFingerprint.from_json(previous_canonical_json)
                if previous_canonical_json
                else None
            )
            drift = detect_schema_drift(
                previous_raw,
                raw_schema,
                previous_canonical,
                canonical_schema,
                config.schema_drift,
            )
            drift_status = drift.status
            store.record_schema_drift(
                run_id=run_id,
                dataset=config.dataset,
                old_raw_hash=previous_raw.hash if previous_raw else raw_schema.hash,
                new_raw_hash=raw_schema.hash,
                old_canonical_hash=(
                    previous_canonical.hash if previous_canonical else canonical_schema.hash
                ),
                new_canonical_hash=canonical_schema.hash,
                events=drift.events,
                detected_at=datetime.now(UTC),
            )
            if drift.failed:
                failed_types = sorted(
                    {event.drift_type for event in drift.events if event.policy == "fail"}
                )
                raise SchemaDriftError(
                    f"Schema drift violates configured policy: {', '.join(failed_types)}"
                )

            extraction_watermark: object | None = None
            if config.watermark is not None:
                watermark_before = store.get_watermark(config.dataset, config.watermark.column)
                extraction_watermark = (
                    watermark_before
                    if watermark_before is not None
                    else config.watermark.initial_value
                )
            rows_extracted, output_columns, rows, candidate_watermark = _extract_and_transform(
                config, raw, extraction_watermark
            )
            rows_transformed = len(rows)
            quality = evaluate_contracts(
                config.contracts,
                [name for name, _ in output_columns],
                rows,
                {column.name: column.quarantine_value for column in config.columns},
            )
            rows_contract_passed = quality.rows_contract_passed
            rows_quarantined = quality.rows_quarantined
            store.record_quality_results(
                run_id=run_id,
                dataset=config.dataset,
                summaries=quality.summaries,
                quarantine_records=quality.quarantine_records,
            )
            load_metrics = _load(
                store,
                config,
                output_columns,
                quality.valid_rows,
                run_id,
                candidate_watermark,
            )
            rows_loaded = load_metrics.rows_loaded
            rows_inserted = load_metrics.rows_inserted
            rows_updated = load_metrics.rows_updated
            if config.watermark is not None:
                watermark_after = candidate_watermark or watermark_before
        except Exception as exc:
            store.finish_run(
                run_id=run_id,
                status="FAILED",
                finished_at=datetime.now(UTC),
                duration_seconds=time.perf_counter() - timer,
                raw_path=str(raw.path) if raw else None,
                raw_sha256=raw.sha256 if raw else None,
                rows_extracted=rows_extracted,
                rows_transformed=rows_transformed,
                rows_contract_passed=rows_contract_passed,
                rows_quarantined=rows_quarantined,
                raw_schema_hash=raw_schema.hash if raw_schema else None,
                canonical_schema_hash=canonical_schema.hash if canonical_schema else None,
                raw_schema_json=raw_schema.json if raw_schema else None,
                canonical_schema_json=canonical_schema.json if canonical_schema else None,
                drift_status=drift_status,
                watermark_before=watermark_before,
                watermark_after=watermark_before,
                error_message=str(exc)[:4000],
            )
            if isinstance(exc, ETLError):
                raise
            raise ETLError(f"Pipeline failed: {exc}") from exc

        duration = time.perf_counter() - timer
        store.finish_run(
            run_id=run_id,
            status="SUCCEEDED",
            finished_at=datetime.now(UTC),
            duration_seconds=duration,
            raw_path=str(raw.path),
            raw_sha256=raw.sha256,
            rows_extracted=rows_extracted,
            rows_transformed=rows_transformed,
            rows_contract_passed=rows_contract_passed,
            rows_quarantined=rows_quarantined,
            rows_loaded=rows_loaded,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            raw_schema_hash=raw_schema.hash,
            canonical_schema_hash=canonical_schema.hash,
            raw_schema_json=raw_schema.json,
            canonical_schema_json=canonical_schema.json,
            drift_status=drift_status,
            watermark_before=watermark_before,
            watermark_after=watermark_after,
        )

    return RunResult(
        run_id=run_id,
        dataset=config.dataset,
        status="SUCCEEDED",
        config_hash=config.config_hash,
        git_commit_sha=git_sha,
        raw_path=str(raw.path),
        raw_sha256=raw.sha256,
        rows_extracted=rows_extracted,
        rows_transformed=rows_transformed,
        rows_contract_passed=rows_contract_passed,
        rows_quarantined=rows_quarantined,
        rows_loaded=rows_loaded,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        raw_schema_hash=raw_schema.hash,
        canonical_schema_hash=canonical_schema.hash,
        drift_status=drift_status,
        watermark_before=watermark_before,
        watermark_after=watermark_after,
        duration_seconds=duration,
    )
