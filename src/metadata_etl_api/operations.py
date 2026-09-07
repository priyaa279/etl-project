from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from metadata_etl.postgres import PostgresStore


class OperationsError(RuntimeError):
    """Safe boundary error for application operation persistence."""


TRIGGERED_STATES = {"TRIGGERING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED"}


class UploadRepository:
    def __init__(self, dsn: str | None) -> None:
        self.dsn = dsn

    def _connect(self) -> psycopg.Connection[Any]:
        if not self.dsn:
            raise OperationsError("Application database is not configured")
        try:
            return psycopg.connect(self.dsn, row_factory=dict_row)
        except psycopg.Error as exc:
            raise OperationsError("Application database is unavailable") from exc

    def ensure_schema(self) -> None:
        try:
            if not self.dsn:
                raise OperationsError("Application database is not configured")
            with PostgresStore(self.dsn) as store:
                store.ensure_metadata_tables()
            with self._connect() as connection, connection.transaction():
                connection.execute("CREATE SCHEMA IF NOT EXISTS etl_app")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS etl_app.upload_sessions (
                        upload_id TEXT PRIMARY KEY,
                        correlation_id TEXT NOT NULL UNIQUE,
                        dataset TEXT NOT NULL,
                        original_filename TEXT NOT NULL,
                        source_type TEXT NOT NULL,
                        size_bytes BIGINT NOT NULL,
                        sha256 TEXT NOT NULL,
                        landing_key TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL CHECK (status IN (
                            'UPLOADED', 'VALIDATING', 'READY', 'WARNING', 'BLOCKED',
                            'TRIGGERING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED'
                        )),
                        uploaded_at TIMESTAMPTZ NOT NULL,
                        validated_at TIMESTAMPTZ,
                        preflight_result JSONB,
                        triggered_at TIMESTAMPTZ,
                        airflow_dag_id TEXT,
                        airflow_run_id TEXT UNIQUE,
                        airflow_state TEXT,
                        etl_run_id TEXT,
                        completed_at TIMESTAMPTZ,
                        safe_error TEXT
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS upload_sessions_dataset_uploaded_idx "
                    "ON etl_app.upload_sessions (dataset, uploaded_at DESC)"
                )
        except psycopg.Error as exc:
            raise OperationsError("Application schema installation failed") from exc

    def create(self, values: dict[str, Any]) -> dict[str, Any]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    """
                    INSERT INTO etl_app.upload_sessions (
                        upload_id, correlation_id, dataset, original_filename, source_type,
                        size_bytes, sha256, landing_key, status, uploaded_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'UPLOADED', %s)
                    RETURNING *
                    """,
                    (
                        values["upload_id"],
                        values["correlation_id"],
                        values["dataset"],
                        values["original_filename"],
                        values["source_type"],
                        values["size_bytes"],
                        values["sha256"],
                        values["landing_key"],
                        values["uploaded_at"],
                    ),
                ).fetchone()
            assert row is not None
            return dict(row)
        except psycopg.Error as exc:
            raise OperationsError("Could not persist upload session") from exc

    def get(self, upload_id: str) -> dict[str, Any] | None:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    "SELECT * FROM etl_app.upload_sessions WHERE upload_id = %s", (upload_id,)
                ).fetchone()
            return dict(row) if row else None
        except psycopg.Error as exc:
            raise OperationsError("Could not read upload session") from exc

    def set_preflight(
        self, upload_id: str, status: str, result: dict[str, Any], validated_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            "status = %s, preflight_result = %s, validated_at = %s, safe_error = NULL",
            (status, Jsonb(result), validated_at, upload_id),
        )

    def claim_trigger(
        self,
        upload_id: str,
        dag_id: str,
        airflow_run_id: str,
        triggered_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    "SELECT * FROM etl_app.upload_sessions WHERE upload_id = %s FOR UPDATE",
                    (upload_id,),
                ).fetchone()
                if row is None:
                    raise OperationsError("Upload session was not found")
                if row["status"] in TRIGGERED_STATES:
                    return dict(row), False
                if row["status"] not in {"READY", "WARNING"}:
                    raise OperationsError("Upload is not ready to run")
                updated = connection.execute(
                    """
                    UPDATE etl_app.upload_sessions
                    SET status = 'TRIGGERING', triggered_at = %s,
                        airflow_dag_id = %s, airflow_run_id = %s
                    WHERE upload_id = %s
                    RETURNING *
                    """,
                    (triggered_at, dag_id, airflow_run_id, upload_id),
                ).fetchone()
            assert updated is not None
            return dict(updated), True
        except psycopg.Error as exc:
            raise OperationsError("Could not claim upload run request") from exc

    def mark_triggered(
        self, upload_id: str, airflow_run_id: str, airflow_state: str | None
    ) -> dict[str, Any]:
        return self._update(
            "status = 'QUEUED', airflow_run_id = %s, airflow_state = %s",
            (airflow_run_id, airflow_state, upload_id),
        )

    def mark_failed(
        self, upload_id: str, safe_error: str, completed_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            "status = 'FAILED', safe_error = %s, completed_at = %s",
            (safe_error, completed_at, upload_id),
        )

    def sync(
        self,
        upload_id: str,
        *,
        status: str,
        airflow_state: str | None,
        etl_run_id: str | None,
        completed_at: datetime | None,
    ) -> dict[str, Any]:
        return self._update(
            "status = %s, airflow_state = %s, etl_run_id = COALESCE(%s, etl_run_id), "
            "completed_at = COALESCE(%s, completed_at)",
            (status, airflow_state, etl_run_id, completed_at, upload_id),
        )

    def _update(self, assignments: str, parameters: Sequence[Any]) -> dict[str, Any]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    f"UPDATE etl_app.upload_sessions SET {assignments} "
                    "WHERE upload_id = %s RETURNING *",
                    parameters,
                ).fetchone()
            if row is None:
                raise OperationsError("Upload session was not found")
            return dict(row)
        except psycopg.Error as exc:
            raise OperationsError("Could not update upload session") from exc
