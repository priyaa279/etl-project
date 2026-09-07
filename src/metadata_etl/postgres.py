from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Any, Self

import psycopg
from psycopg import sql

from metadata_etl.errors import LoadError


@dataclass(frozen=True)
class LoadMetrics:
    rows_loaded: int
    rows_inserted: int
    rows_updated: int


DUCKDB_TO_POSTGRES = {
    "VARCHAR": "TEXT",
    "BIGINT": "BIGINT",
    "INTEGER": "INTEGER",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
}


def _postgres_type(duckdb_type: Any) -> sql.SQL:
    type_name = str(duckdb_type).upper()
    if type_name.startswith("DECIMAL"):
        return sql.SQL(type_name)
    mapped = DUCKDB_TO_POSTGRES.get(type_name)
    if not mapped:
        raise LoadError(f"No PostgreSQL type mapping for DuckDB type {type_name!r}")
    return sql.SQL(mapped)


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.connection: psycopg.Connection[Any] | None = None

    def __enter__(self) -> Self:
        try:
            self.connection = psycopg.connect(self.dsn)
        except psycopg.Error as exc:
            raise LoadError(f"Could not connect to PostgreSQL: {exc}") from exc
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self.connection is not None:
            self.connection.close()

    @property
    def conn(self) -> psycopg.Connection[Any]:
        if self.connection is None:
            raise RuntimeError("PostgresStore is not connected")
        return self.connection

    def ensure_metadata_tables(self) -> None:
        with self.conn.transaction():
            self.conn.execute("CREATE SCHEMA IF NOT EXISTS etl_meta")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_meta.etl_run_ledger (
                    run_id TEXT PRIMARY KEY,
                    dataset TEXT NOT NULL,
                    config_schema_version TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    git_commit_sha TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    raw_path TEXT,
                    raw_sha256 TEXT,
                    rows_extracted BIGINT,
                    rows_transformed BIGINT,
                    rows_contract_passed BIGINT,
                    rows_quarantined BIGINT,
                    rows_loaded BIGINT,
                    rows_inserted BIGINT,
                    rows_updated BIGINT,
                    raw_schema_hash TEXT,
                    canonical_schema_hash TEXT,
                    raw_schema_json TEXT,
                    canonical_schema_json TEXT,
                    drift_status TEXT,
                    watermark_before TEXT,
                    watermark_after TEXT,
                    duration_seconds DOUBLE PRECISION,
                    error_message TEXT
                )
                """
            )
            self.conn.execute(
                "ALTER TABLE etl_meta.etl_run_ledger "
                "ADD COLUMN IF NOT EXISTS rows_contract_passed BIGINT"
            )
            self.conn.execute(
                "ALTER TABLE etl_meta.etl_run_ledger "
                "ADD COLUMN IF NOT EXISTS rows_quarantined BIGINT"
            )
            for definition in (
                "rows_inserted BIGINT",
                "rows_updated BIGINT",
                "raw_schema_hash TEXT",
                "canonical_schema_hash TEXT",
                "raw_schema_json TEXT",
                "canonical_schema_json TEXT",
                "drift_status TEXT",
                "watermark_before TEXT",
                "watermark_after TEXT",
            ):
                self.conn.execute(
                    f"ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS {definition}"
                )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_meta.data_quality_results (
                    run_id TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    rule_type TEXT NOT NULL,
                    records_checked BIGINT NOT NULL,
                    records_failed BIGINT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (run_id, rule_id)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_meta.etl_quarantine (
                    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    rule_id TEXT NOT NULL,
                    rule_type TEXT NOT NULL,
                    failure_reason TEXT NOT NULL,
                    record_identifier TEXT NOT NULL,
                    failed_column TEXT,
                    failed_value TEXT,
                    quarantined_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_meta.schema_drift_history (
                    drift_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    schema_level TEXT NOT NULL,
                    old_schema_hash TEXT NOT NULL,
                    new_schema_hash TEXT NOT NULL,
                    drift_type TEXT NOT NULL,
                    change_description TEXT NOT NULL,
                    policy TEXT NOT NULL,
                    action_taken TEXT NOT NULL,
                    detected_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS etl_meta.etl_watermarks (
                    dataset TEXT PRIMARY KEY,
                    watermark_column TEXT NOT NULL,
                    last_successful_value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    run_id TEXT NOT NULL
                )
                """
            )

    def start_run(
        self,
        *,
        run_id: str,
        dataset: str,
        schema_version: str,
        config_hash: str,
        git_sha: str,
        started_at: datetime,
    ) -> None:
        with self.conn.transaction():
            self.conn.execute(
                """
                INSERT INTO etl_meta.etl_run_ledger (
                    run_id, dataset, config_schema_version, config_hash,
                    git_commit_sha, status, started_at
                ) VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s)
                """,
                (run_id, dataset, schema_version, config_hash, git_sha, started_at),
            )

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        finished_at: datetime,
        duration_seconds: float,
        raw_path: str | None = None,
        raw_sha256: str | None = None,
        rows_extracted: int | None = None,
        rows_transformed: int | None = None,
        rows_contract_passed: int | None = None,
        rows_quarantined: int | None = None,
        rows_loaded: int | None = None,
        rows_inserted: int | None = None,
        rows_updated: int | None = None,
        raw_schema_hash: str | None = None,
        canonical_schema_hash: str | None = None,
        raw_schema_json: str | None = None,
        canonical_schema_json: str | None = None,
        drift_status: str | None = None,
        watermark_before: str | None = None,
        watermark_after: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self.conn.transaction():
            self.conn.execute(
                """
                UPDATE etl_meta.etl_run_ledger
                SET status = %s,
                    finished_at = %s,
                    duration_seconds = %s,
                    raw_path = COALESCE(%s, raw_path),
                    raw_sha256 = COALESCE(%s, raw_sha256),
                    rows_extracted = COALESCE(%s, rows_extracted),
                    rows_transformed = COALESCE(%s, rows_transformed),
                    rows_contract_passed = COALESCE(%s, rows_contract_passed),
                    rows_quarantined = COALESCE(%s, rows_quarantined),
                    rows_loaded = COALESCE(%s, rows_loaded),
                    rows_inserted = COALESCE(%s, rows_inserted),
                    rows_updated = COALESCE(%s, rows_updated),
                    raw_schema_hash = COALESCE(%s, raw_schema_hash),
                    canonical_schema_hash = COALESCE(%s, canonical_schema_hash),
                    raw_schema_json = COALESCE(%s, raw_schema_json),
                    canonical_schema_json = COALESCE(%s, canonical_schema_json),
                    drift_status = COALESCE(%s, drift_status),
                    watermark_before = COALESCE(%s, watermark_before),
                    watermark_after = COALESCE(%s, watermark_after),
                    error_message = %s
                WHERE run_id = %s
                """,
                (
                    status,
                    finished_at,
                    duration_seconds,
                    raw_path,
                    raw_sha256,
                    rows_extracted,
                    rows_transformed,
                    rows_contract_passed,
                    rows_quarantined,
                    rows_loaded,
                    rows_inserted,
                    rows_updated,
                    raw_schema_hash,
                    canonical_schema_hash,
                    raw_schema_json,
                    canonical_schema_json,
                    drift_status,
                    watermark_before,
                    watermark_after,
                    error_message,
                    run_id,
                ),
            )

    def get_previous_successful_schemas(self, dataset: str) -> tuple[str | None, str | None]:
        with self.conn.transaction():
            row = self.conn.execute(
                """
                SELECT raw_schema_json, canonical_schema_json
                FROM etl_meta.etl_run_ledger
                WHERE dataset = %s AND status = 'SUCCEEDED'
                  AND raw_schema_json IS NOT NULL AND canonical_schema_json IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT 1
                """,
                (dataset,),
            ).fetchone()
        return (row[0], row[1]) if row else (None, None)

    def get_watermark(self, dataset: str, watermark_column: str) -> str | None:
        with self.conn.transaction():
            row = self.conn.execute(
                """
                SELECT last_successful_value
                FROM etl_meta.etl_watermarks
                WHERE dataset = %s AND watermark_column = %s
                """,
                (dataset, watermark_column),
            ).fetchone()
        return str(row[0]) if row else None

    def record_schema_drift(
        self,
        *,
        run_id: str,
        dataset: str,
        old_raw_hash: str,
        new_raw_hash: str,
        old_canonical_hash: str,
        new_canonical_hash: str,
        events: Sequence[Any],
        detected_at: datetime,
    ) -> None:
        if not events:
            return
        with self.conn.transaction(), self.conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO etl_meta.schema_drift_history (
                    run_id, dataset, schema_level, old_schema_hash, new_schema_hash,
                    drift_type, change_description, policy, action_taken, detected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        run_id,
                        dataset,
                        event.schema_level,
                        old_raw_hash if event.schema_level == "raw" else old_canonical_hash,
                        new_raw_hash if event.schema_level == "raw" else new_canonical_hash,
                        event.drift_type,
                        event.description,
                        event.policy,
                        event.action_taken,
                        detected_at,
                    )
                    for event in events
                ],
            )

    def record_quality_results(
        self,
        *,
        run_id: str,
        dataset: str,
        summaries: Sequence[Any],
        quarantine_records: Sequence[Any],
    ) -> None:
        try:
            with self.conn.transaction(), self.conn.cursor() as cursor:
                if summaries:
                    cursor.executemany(
                        """
                        INSERT INTO etl_meta.data_quality_results (
                            run_id, dataset, rule_id, rule_type, records_checked,
                            records_failed, status, timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                run_id,
                                dataset,
                                summary.rule_id,
                                summary.rule_type,
                                summary.records_checked,
                                summary.records_failed,
                                summary.status,
                                summary.timestamp,
                            )
                            for summary in summaries
                        ],
                    )
                if quarantine_records:
                    cursor.executemany(
                        """
                        INSERT INTO etl_meta.etl_quarantine (
                            run_id, dataset, rule_id, rule_type, failure_reason,
                            record_identifier, failed_column, failed_value, quarantined_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                run_id,
                                dataset,
                                record.rule_id,
                                record.rule_type,
                                record.failure_reason,
                                record.record_identifier,
                                record.failed_column,
                                record.failed_value,
                                record.quarantined_at,
                            )
                            for record in quarantine_records
                        ],
                    )
        except psycopg.Error as exc:
            raise LoadError(f"Could not persist data-quality results: {exc}") from exc

    def publish_full(
        self,
        *,
        destination_schema: str,
        staging_table: str,
        target_table: str,
        columns: Sequence[tuple[str, Any]],
        rows: Sequence[tuple[Any, ...]],
        run_id: str,
    ) -> LoadMetrics:
        """Load staging and replace trusted data in one PostgreSQL transaction."""
        suffix = run_id.lower().replace("-", "_")[-24:]
        physical_staging = f"{staging_table}_{suffix}"[:63]
        definitions = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(name), _postgres_type(data_type))
            for name, data_type in columns
        )
        column_list = sql.SQL(", ").join(sql.Identifier(name) for name, _ in columns)
        staging_identifier = sql.Identifier(destination_schema, physical_staging)
        target_identifier = sql.Identifier(destination_schema, target_table)

        try:
            with self.conn.transaction():
                self.conn.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(destination_schema)
                    )
                )
                self.conn.execute(
                    sql.SQL("CREATE TABLE {} ({})").format(staging_identifier, definitions)
                )
                copy_statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
                    staging_identifier, column_list
                )
                with self.conn.cursor().copy(copy_statement) as copy:
                    for row in rows:
                        copy.write_row(row)
                self.conn.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE {} INCLUDING ALL)").format(
                        target_identifier, staging_identifier
                    )
                )
                self.conn.execute(sql.SQL("TRUNCATE TABLE {}").format(target_identifier))
                self.conn.execute(
                    sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                        target_identifier, column_list, column_list, staging_identifier
                    )
                )
                self.conn.execute(sql.SQL("DROP TABLE {}").format(staging_identifier))
        except psycopg.Error as exc:
            raise LoadError(f"PostgreSQL atomic publish failed: {exc}") from exc
        return LoadMetrics(len(rows), len(rows), 0)

    def publish_incremental(
        self,
        *,
        destination_schema: str,
        staging_table: str,
        target_table: str,
        columns: Sequence[tuple[str, Any]],
        rows: Sequence[tuple[Any, ...]],
        run_id: str,
        dataset: str,
        watermark_column: str,
        watermark_after: str | None,
        updated_at: datetime,
    ) -> LoadMetrics:
        """Append a successful incremental range and advance its watermark atomically."""
        suffix = run_id.lower().replace("-", "_")[-24:]
        physical_staging = f"{staging_table}_{suffix}"[:63]
        definitions = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(name), _postgres_type(data_type))
            for name, data_type in columns
        )
        column_list = sql.SQL(", ").join(sql.Identifier(name) for name, _ in columns)
        staging_identifier = sql.Identifier(destination_schema, physical_staging)
        target_identifier = sql.Identifier(destination_schema, target_table)
        try:
            with self.conn.transaction():
                self.conn.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(destination_schema)
                    )
                )
                self.conn.execute(
                    sql.SQL("CREATE TABLE {} ({})").format(staging_identifier, definitions)
                )
                copy_statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
                    staging_identifier, column_list
                )
                with self.conn.cursor().copy(copy_statement) as copy:
                    for row in rows:
                        copy.write_row(row)
                self.conn.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE {} INCLUDING ALL)").format(
                        target_identifier, staging_identifier
                    )
                )
                if rows:
                    self.conn.execute(
                        sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                            target_identifier, column_list, column_list, staging_identifier
                        )
                    )
                self.conn.execute(sql.SQL("DROP TABLE {}").format(staging_identifier))
                if watermark_after is not None:
                    self.conn.execute(
                        """
                        INSERT INTO etl_meta.etl_watermarks (
                            dataset, watermark_column, last_successful_value, updated_at, run_id
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (dataset) DO UPDATE SET
                            watermark_column = EXCLUDED.watermark_column,
                            last_successful_value = EXCLUDED.last_successful_value,
                            updated_at = EXCLUDED.updated_at,
                            run_id = EXCLUDED.run_id
                        """,
                        (dataset, watermark_column, watermark_after, updated_at, run_id),
                    )
        except psycopg.Error as exc:
            raise LoadError(f"PostgreSQL incremental publish failed: {exc}") from exc
        return LoadMetrics(len(rows), len(rows), 0)

    def publish_upsert(
        self,
        *,
        destination_schema: str,
        staging_table: str,
        target_table: str,
        columns: Sequence[tuple[str, Any]],
        rows: Sequence[tuple[Any, ...]],
        run_id: str,
        keys: Sequence[str],
    ) -> LoadMetrics:
        """Merge valid rows by configured business keys in one PostgreSQL transaction."""
        names = [name for name, _ in columns]
        key_positions = [names.index(key) for key in keys]
        source_keys = [tuple(row[index] for index in key_positions) for row in rows]
        if any(any(value is None for value in key) for key in source_keys):
            raise LoadError("Upsert business keys cannot contain null values")
        if len(set(source_keys)) != len(source_keys):
            raise LoadError("Upsert input contains duplicate business keys")

        suffix = run_id.lower().replace("-", "_")[-24:]
        physical_staging = f"{staging_table}_{suffix}"[:63]
        definitions = sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(name), _postgres_type(data_type))
            for name, data_type in columns
        )
        column_list = sql.SQL(", ").join(sql.Identifier(name) for name, _ in columns)
        staging_identifier = sql.Identifier(destination_schema, physical_staging)
        target_identifier = sql.Identifier(destination_schema, target_table)
        match = sql.SQL(" AND ").join(
            sql.SQL("target.{} = source.{}").format(sql.Identifier(key), sql.Identifier(key))
            for key in keys
        )
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = source.{}").format(sql.Identifier(name), sql.Identifier(name))
            for name in names
        )
        source_columns = sql.SQL(", ").join(
            sql.SQL("source.{}").format(sql.Identifier(name)) for name in names
        )
        existing_match = sql.SQL(" AND ").join(
            sql.SQL("target.{} = source.{}").format(sql.Identifier(key), sql.Identifier(key))
            for key in keys
        )

        try:
            with self.conn.transaction():
                self.conn.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(destination_schema)
                    )
                )
                self.conn.execute(
                    sql.SQL("CREATE TABLE {} ({})").format(staging_identifier, definitions)
                )
                copy_statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
                    staging_identifier, column_list
                )
                with self.conn.cursor().copy(copy_statement) as copy:
                    for row in rows:
                        copy.write_row(row)
                self.conn.execute(
                    sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE {} INCLUDING ALL)").format(
                        target_identifier, staging_identifier
                    )
                )
                self.conn.execute(
                    sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(target_identifier)
                )
                updated_row = self.conn.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} AS source WHERE EXISTS "
                        "(SELECT 1 FROM {} AS target WHERE {})"
                    ).format(staging_identifier, target_identifier, existing_match)
                ).fetchone()
                rows_updated = int(updated_row[0]) if updated_row else 0
                if rows:
                    self.conn.execute(
                        sql.SQL(
                            "MERGE INTO {} AS target USING {} AS source ON {} "
                            "WHEN MATCHED THEN UPDATE SET {} "
                            "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})"
                        ).format(
                            target_identifier,
                            staging_identifier,
                            match,
                            assignments,
                            column_list,
                            source_columns,
                        )
                    )
                self.conn.execute(sql.SQL("DROP TABLE {}").format(staging_identifier))
        except psycopg.Error as exc:
            raise LoadError(f"PostgreSQL upsert publish failed: {exc}") from exc
        return LoadMetrics(len(rows), len(rows) - rows_updated, rows_updated)
