from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from types import TracebackType
from typing import Any, Self

import psycopg
from psycopg import sql

from metadata_etl.errors import LoadError

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
                    error_message,
                    run_id,
                ),
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
    ) -> int:
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
        return len(rows)
