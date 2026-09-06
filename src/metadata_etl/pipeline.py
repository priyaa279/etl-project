from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from metadata_etl.config import ETLConfig, load_config
from metadata_etl.errors import ETLError, ExtractionError
from metadata_etl.postgres import PostgresStore
from metadata_etl.source import RawArtifact, preserve_raw_copy
from metadata_etl.sql_compiler import compile_transform_sql, csv_relation


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
    rows_loaded: int
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
    config: ETLConfig, raw: RawArtifact
) -> tuple[int, list[tuple[str, Any]], list[tuple[Any, ...]]]:
    query = compile_transform_sql(config, source_path=raw.path)
    try:
        with duckdb.connect(":memory:") as connection:
            extracted_row = connection.execute(
                f"SELECT count(*) FROM {csv_relation(raw.path, config.delimiter)}"
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
    for column in config.columns:
        if not column.nullable:
            null_count = sum(row[positions[column.name]] is None for row in rows)
            if null_count:
                raise ExtractionError(
                    f"Column {column.name!r} is not nullable but contains {null_count} null value(s)"
                )
    return int(extracted_row[0]), output_columns, rows


def run_pipeline(config_path: str | Path) -> RunResult:
    """Execute the milestone-1 CSV-to-PostgreSQL vertical slice."""
    config = load_config(config_path)
    run_id = _create_run_id()
    git_sha = _git_sha(config.path)
    started_at = datetime.now(UTC)
    timer = time.perf_counter()
    raw: RawArtifact | None = None
    rows_extracted: int | None = None
    rows_transformed: int | None = None

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
            rows_extracted, output_columns, rows = _extract_and_transform(config, raw)
            rows_transformed = len(rows)
            rows_loaded = store.publish_full(
                destination_schema=config.destination_schema,
                staging_table=config.staging_table,
                target_table=config.target_table,
                columns=output_columns,
                rows=rows,
                run_id=run_id,
            )
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
            rows_loaded=rows_loaded,
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
        rows_loaded=rows_loaded,
        duration_seconds=duration,
    )
