from __future__ import annotations

import logging
import os
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
from metadata_etl.connectors import ExtractedSource, default_connector_registry
from metadata_etl.errors import ETLError, ExtractionError, SchemaDriftError
from metadata_etl.postgres import LoadMetrics, PostgresStore
from metadata_etl.quality.engine import evaluate_contracts
from metadata_etl.schema import (
    SchemaFingerprint,
    canonical_schema_fingerprint,
    detect_schema_drift,
)
from metadata_etl.sql_compiler import compile_canonical_sql, compile_transform_sql
from metadata_etl.structured_logging import emit_event, redact_text
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
    rows_expired: int
    rows_history_inserted: int
    raw_schema_hash: str
    canonical_schema_hash: str
    drift_status: str
    watermark_before: str | None
    watermark_after: str | None
    source_type: str
    run_mode: str
    backfill_from: str | None
    backfill_to: str | None
    duration_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _create_run_id() -> str:
    now = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"RUN_{now}_{uuid.uuid4().hex[:8].upper()}"


def _git_sha(config_path: Path) -> str:
    injected_sha = os.getenv("ETL_GIT_COMMIT_SHA")
    if injected_sha:
        return injected_sha
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
    config: ETLConfig,
    extracted: ExtractedSource,
    watermark_value: object | None = None,
    watermark_to: object | None = None,
) -> tuple[int, list[tuple[str, Any]], list[tuple[Any, ...]], str | None]:
    canonical_query = compile_canonical_sql(
        config,
        source_relation=extracted.relation_sql,
        watermark_value=watermark_value,
        watermark_to=watermark_to,
    )
    query = compile_transform_sql(
        config,
        source_relation=extracted.relation_sql,
        watermark_value=watermark_value,
        watermark_to=watermark_to,
    )
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
        raise ExtractionError(f"Source normalization or transformation failed: {exc}") from exc

    if extracted_row is None or description is None:
        raise ExtractionError("Source query did not return a result")
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
    run_mode: str,
    backfill_from: str | None,
    backfill_to: str | None,
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
        if run_mode == "backfill":
            assert backfill_from is not None and backfill_to is not None
            return store.publish_backfill(
                **common,
                watermark_column=config.watermark.column,
                backfill_from=backfill_from,
                backfill_to=backfill_to,
            )
        return store.publish_incremental(
            **common,
            dataset=config.dataset,
            watermark_column=config.watermark.column,
            watermark_after=candidate_watermark,
            updated_at=datetime.now(UTC),
        )
    if config.load_strategy == "upsert":
        return store.publish_upsert(**common, keys=config.load_keys)
    assert config.scd2 is not None
    return store.publish_scd2(**common, scd2=config.scd2)


def _backfill_boundary(value: str, datatype: str) -> Any:
    try:
        if datatype == "timestamp":
            return datetime.fromisoformat(value)
        if datatype == "date":
            return date.fromisoformat(value)
        if datatype == "integer":
            return int(value)
        if datatype == "decimal":
            return Decimal(value)
        return value
    except (ValueError, ArithmeticError) as exc:
        raise ETLError(f"Backfill boundary {value!r} is not a valid {datatype}") from exc


def run_pipeline(
    config_path: str | Path,
    *,
    backfill_from: str | None = None,
    backfill_to: str | None = None,
) -> RunResult:
    """Execute an approved source configuration through the generic ETL runtime."""
    config = load_config(config_path)
    emit_event(
        "CONFIG_VALIDATED",
        dataset=config.dataset,
        stage="CONFIG",
        source_type=config.source_type,
        load_strategy=config.load_strategy,
    )
    if (backfill_from is None) != (backfill_to is None):
        raise ETLError("Backfill requires both --from and --to")
    run_mode = "backfill" if backfill_from is not None else "normal"
    lower_boundary: Any | None = None
    upper_boundary: Any | None = None
    if run_mode == "backfill":
        if config.load_strategy != "incremental" or config.watermark is None:
            raise ETLError("Backfill is supported only for incremental loads with a watermark")
        lower_boundary = _backfill_boundary(backfill_from, config.watermark.datatype)
        upper_boundary = _backfill_boundary(backfill_to, config.watermark.datatype)
        if lower_boundary >= upper_boundary:
            raise ETLError("Backfill --from must be earlier than --to")
    run_id = _create_run_id()
    git_sha = _git_sha(config.path)
    started_at = datetime.now(UTC)
    timer = time.perf_counter()
    extracted: ExtractedSource | None = None
    rows_extracted: int | None = None
    rows_transformed: int | None = None
    rows_contract_passed: int | None = None
    rows_quarantined: int | None = None
    raw_schema: SchemaFingerprint | None = None
    canonical_schema: SchemaFingerprint | None = None
    drift_status: str | None = None
    watermark_before: str | None = None
    watermark_after: str | None = None
    rows_expired = 0
    rows_history_inserted = 0

    with PostgresStore(config.dsn) as store:
        store.ensure_metadata_tables()
        store.start_run(
            run_id=run_id,
            dataset=config.dataset,
            schema_version=config.schema_version,
            config_hash=config.config_hash,
            git_sha=git_sha,
            started_at=started_at,
            source_type=config.source_type,
            run_mode=run_mode,
            backfill_from=backfill_from,
            backfill_to=backfill_to,
        )
        emit_event(
            "RUN_STARTED",
            run_id=run_id,
            dataset=config.dataset,
            stage="RUN",
            source_type=config.source_type,
            load_strategy=config.load_strategy,
            run_mode=run_mode,
        )
        try:
            extracted = (
                default_connector_registry()
                .get(config.source_type)
                .extract(config, run_id, config.raw_root)
            )
            emit_event(
                "SOURCE_EXTRACTED",
                run_id=run_id,
                dataset=config.dataset,
                stage="EXTRACT",
                source_type=config.source_type,
            )
            emit_event(
                "RAW_PRESERVED",
                run_id=run_id,
                dataset=config.dataset,
                stage="RAW",
                source_type=config.source_type,
            )
            raw_schema = extracted.raw_schema
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
            emit_event(
                "SCHEMA_CHECKED",
                run_id=run_id,
                dataset=config.dataset,
                stage="SCHEMA",
                drift_status=drift_status,
            )

            extraction_watermark: object | None = lower_boundary
            if config.watermark is not None and run_mode == "normal":
                watermark_before = store.get_watermark(config.dataset, config.watermark.column)
                extraction_watermark = (
                    watermark_before
                    if watermark_before is not None
                    else config.watermark.initial_value
                )
            rows_extracted, output_columns, rows, candidate_watermark = _extract_and_transform(
                config, extracted, extraction_watermark, upper_boundary
            )
            rows_transformed = len(rows)
            emit_event(
                "TRANSFORM_COMPLETED",
                run_id=run_id,
                dataset=config.dataset,
                stage="TRANSFORM",
                rows_processed=rows_transformed,
            )
            quality = evaluate_contracts(
                config.contracts,
                [name for name, _ in output_columns],
                rows,
                {column.name: column.quarantine_value for column in config.columns},
            )
            rows_contract_passed = quality.rows_contract_passed
            rows_quarantined = quality.rows_quarantined
            emit_event(
                "QUALITY_COMPLETED",
                run_id=run_id,
                dataset=config.dataset,
                stage="QUALITY",
                rows_processed=rows_contract_passed,
                rows_quarantined=rows_quarantined,
            )
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
                run_mode,
                backfill_from,
                backfill_to,
            )
            rows_loaded = load_metrics.rows_loaded
            rows_inserted = load_metrics.rows_inserted
            rows_updated = load_metrics.rows_updated
            rows_expired = load_metrics.rows_expired
            rows_history_inserted = load_metrics.rows_history_inserted
            emit_event(
                "PUBLISH_COMPLETED",
                run_id=run_id,
                dataset=config.dataset,
                stage="PUBLISH",
                load_strategy=config.load_strategy,
                rows_processed=rows_loaded,
            )
            if config.watermark is not None and run_mode == "normal":
                watermark_after = candidate_watermark or watermark_before
                if watermark_after != watermark_before:
                    emit_event(
                        "WATERMARK_ADVANCED",
                        run_id=run_id,
                        dataset=config.dataset,
                        stage="WATERMARK",
                    )
        except Exception as exc:
            safe_error = redact_text(exc)
            store.finish_run(
                run_id=run_id,
                status="FAILED",
                finished_at=datetime.now(UTC),
                duration_seconds=time.perf_counter() - timer,
                raw_path=str(extracted.artifact.path) if extracted else None,
                raw_sha256=extracted.artifact.sha256 if extracted else None,
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
                rows_expired=rows_expired,
                rows_history_inserted=rows_history_inserted,
                error_message=safe_error[:4000],
            )
            emit_event(
                "RUN_FAILED",
                level=logging.ERROR,
                run_id=run_id,
                dataset=config.dataset,
                stage="RUN",
                source_type=config.source_type,
                load_strategy=config.load_strategy,
                error_type=type(exc).__name__,
                error=safe_error,
                duration_seconds=time.perf_counter() - timer,
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
            raw_path=str(extracted.artifact.path),
            raw_sha256=extracted.artifact.sha256,
            rows_extracted=rows_extracted,
            rows_transformed=rows_transformed,
            rows_contract_passed=rows_contract_passed,
            rows_quarantined=rows_quarantined,
            rows_loaded=rows_loaded,
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_expired=rows_expired,
            rows_history_inserted=rows_history_inserted,
            raw_schema_hash=raw_schema.hash,
            canonical_schema_hash=canonical_schema.hash,
            raw_schema_json=raw_schema.json,
            canonical_schema_json=canonical_schema.json,
            drift_status=drift_status,
            watermark_before=watermark_before,
            watermark_after=watermark_after,
        )
        emit_event(
            "RUN_SUCCEEDED",
            run_id=run_id,
            dataset=config.dataset,
            stage="RUN",
            source_type=config.source_type,
            load_strategy=config.load_strategy,
            rows_processed=rows_loaded,
            duration_seconds=duration,
        )

    return RunResult(
        run_id=run_id,
        dataset=config.dataset,
        status="SUCCEEDED",
        config_hash=config.config_hash,
        git_commit_sha=git_sha,
        raw_path=str(extracted.artifact.path),
        raw_sha256=extracted.artifact.sha256,
        rows_extracted=rows_extracted,
        rows_transformed=rows_transformed,
        rows_contract_passed=rows_contract_passed,
        rows_quarantined=rows_quarantined,
        rows_loaded=rows_loaded,
        rows_inserted=rows_inserted,
        rows_updated=rows_updated,
        rows_expired=rows_expired,
        rows_history_inserted=rows_history_inserted,
        raw_schema_hash=raw_schema.hash,
        canonical_schema_hash=canonical_schema.hash,
        drift_status=drift_status,
        watermark_before=watermark_before,
        watermark_after=watermark_after,
        source_type=config.source_type,
        run_mode=run_mode,
        backfill_from=backfill_from,
        backfill_to=backfill_to,
        duration_seconds=duration,
    )
