from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from metadata_etl.postgres import PostgresStore


class OnboardingRepositoryError(RuntimeError):
    """Safe boundary error for onboarding persistence."""


class OnboardingCollisionError(OnboardingRepositoryError):
    """Raised when a dataset already owns an onboarding session."""


class OnboardingRepository:
    def __init__(self, dsn: str | None) -> None:
        self.dsn = dsn

    def _connect(self) -> psycopg.Connection[Any]:
        if not self.dsn:
            raise OnboardingRepositoryError("Application database is not configured")
        try:
            return psycopg.connect(self.dsn, row_factory=dict_row)
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Application database is unavailable") from exc

    def ensure_schema(self) -> None:
        try:
            if not self.dsn:
                raise OnboardingRepositoryError("Application database is not configured")
            with PostgresStore(self.dsn) as store:
                store.ensure_metadata_tables()
            with self._connect() as connection, connection.transaction():
                connection.execute("CREATE SCHEMA IF NOT EXISTS etl_app")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS etl_app.onboarding_sessions (
                        onboarding_id TEXT PRIMARY KEY,
                        proposed_dataset_name TEXT NOT NULL UNIQUE,
                        source_type TEXT NOT NULL CHECK (source_type IN ('csv', 'json', 'parquet')),
                        original_filename TEXT NOT NULL,
                        size_bytes BIGINT NOT NULL,
                        sha256 TEXT NOT NULL,
                        landing_key TEXT NOT NULL UNIQUE,
                        draft_key TEXT UNIQUE,
                        status TEXT NOT NULL CHECK (status IN (
                            'UPLOADED', 'PROFILING', 'NEEDS_REVIEW',
                            'REVIEW_IN_PROGRESS', 'REVIEW_COMPLETE', 'CONFIGURING',
                            'READY_FOR_VALIDATION', 'VALIDATION_FAILED',
                            'READY_FOR_APPROVAL', 'APPROVING', 'APPROVED', 'ACTIVATING',
                            'WAITING_FOR_DAG', 'ACTIVATION_FAILED', 'READY_FOR_FIRST_RUN',
                            'TRIGGERING', 'QUEUED', 'RUNNING', 'SUCCEEDED',
                            'FIRST_RUN_FAILED', 'FAILED'
                        )),
                        uploaded_at TIMESTAMPTZ NOT NULL,
                        profiled_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL,
                        profile_result JSONB,
                        key_decisions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        validation_hash TEXT,
                        validation_errors JSONB,
                        validated_at TIMESTAMPTZ,
                        approved_at TIMESTAMPTZ,
                        approved_by TEXT,
                        approved_config_hash TEXT,
                        approved_config_key TEXT,
                        source_key TEXT,
                        git_commit_sha TEXT,
                        git_push_status TEXT,
                        dag_id TEXT,
                        activation_checked_at TIMESTAMPTZ,
                        first_run_attempt INTEGER NOT NULL DEFAULT 0,
                        first_run_correlation_id TEXT,
                        airflow_run_id TEXT,
                        airflow_state TEXT,
                        etl_run_id TEXT,
                        completed_at TIMESTAMPTZ,
                        safe_error TEXT
                    )
                    """
                )
                connection.execute(
                    "ALTER TABLE etl_app.onboarding_sessions "
                    "ADD COLUMN IF NOT EXISTS validation_hash TEXT"
                )
                connection.execute(
                    "ALTER TABLE etl_app.onboarding_sessions "
                    "ADD COLUMN IF NOT EXISTS validation_errors JSONB"
                )
                connection.execute(
                    "ALTER TABLE etl_app.onboarding_sessions "
                    "ADD COLUMN IF NOT EXISTS validated_at TIMESTAMPTZ"
                )
                for definition in (
                    "approved_at TIMESTAMPTZ",
                    "approved_by TEXT",
                    "approved_config_hash TEXT",
                    "approved_config_key TEXT",
                    "source_key TEXT",
                    "git_commit_sha TEXT",
                    "git_push_status TEXT",
                    "dag_id TEXT",
                    "activation_checked_at TIMESTAMPTZ",
                    "first_run_attempt INTEGER NOT NULL DEFAULT 0",
                    "first_run_correlation_id TEXT",
                    "airflow_run_id TEXT",
                    "airflow_state TEXT",
                    "etl_run_id TEXT",
                    "completed_at TIMESTAMPTZ",
                ):
                    connection.execute(
                        "ALTER TABLE etl_app.onboarding_sessions ADD COLUMN IF NOT EXISTS "
                        + definition
                    )
                connection.execute(
                    "ALTER TABLE etl_app.onboarding_sessions "
                    "DROP CONSTRAINT IF EXISTS onboarding_sessions_status_check"
                )
                connection.execute(
                    "ALTER TABLE etl_app.onboarding_sessions ADD CONSTRAINT "
                    "onboarding_sessions_status_check CHECK (status IN ("
                    "'UPLOADED', 'PROFILING', 'NEEDS_REVIEW', 'REVIEW_IN_PROGRESS', "
                    "'REVIEW_COMPLETE', 'CONFIGURING', 'READY_FOR_VALIDATION', "
                    "'VALIDATION_FAILED', 'READY_FOR_APPROVAL', 'APPROVING', 'APPROVED', "
                    "'ACTIVATING', 'WAITING_FOR_DAG', 'ACTIVATION_FAILED', "
                    "'READY_FOR_FIRST_RUN', 'TRIGGERING', 'QUEUED', 'RUNNING', "
                    "'SUCCEEDED', 'FIRST_RUN_FAILED', 'FAILED'))"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS onboarding_first_run_correlation_uq "
                    "ON etl_app.onboarding_sessions (first_run_correlation_id) "
                    "WHERE first_run_correlation_id IS NOT NULL"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS onboarding_airflow_run_uq "
                    "ON etl_app.onboarding_sessions (airflow_run_id) "
                    "WHERE airflow_run_id IS NOT NULL"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS onboarding_sessions_updated_idx "
                    "ON etl_app.onboarding_sessions (updated_at DESC)"
                )
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Onboarding schema installation failed") from exc

    def create(self, values: dict[str, Any]) -> dict[str, Any]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    """
                    INSERT INTO etl_app.onboarding_sessions (
                        onboarding_id, proposed_dataset_name, source_type, original_filename,
                        size_bytes, sha256, landing_key, status, uploaded_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'UPLOADED', %s, %s)
                    RETURNING *
                    """,
                    (
                        values["onboarding_id"],
                        values["proposed_dataset_name"],
                        values["source_type"],
                        values["original_filename"],
                        values["size_bytes"],
                        values["sha256"],
                        values["landing_key"],
                        values["uploaded_at"],
                        values["uploaded_at"],
                    ),
                ).fetchone()
            assert row is not None
            return dict(row)
        except UniqueViolation as exc:
            raise OnboardingCollisionError("A draft already exists for this dataset") from exc
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Could not persist onboarding session") from exc

    def get(self, onboarding_id: str) -> dict[str, Any] | None:
        return self._select(
            "SELECT * FROM etl_app.onboarding_sessions WHERE onboarding_id = %s",
            (onboarding_id,),
        )

    def find_by_dataset(self, dataset: str) -> dict[str, Any] | None:
        return self._select(
            "SELECT * FROM etl_app.onboarding_sessions WHERE proposed_dataset_name = %s",
            (dataset,),
        )

    def begin_profile(self, onboarding_id: str, updated_at: datetime) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'PROFILING', updated_at = %s, safe_error = NULL",
            (updated_at,),
        )

    def set_profile(
        self,
        onboarding_id: str,
        *,
        status: str,
        draft_key: str,
        profile_result: dict[str, Any],
        profiled_at: datetime,
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = %s, draft_key = %s, profile_result = %s, "
            "profiled_at = %s, updated_at = %s, safe_error = NULL",
            (status, draft_key, Jsonb(profile_result), profiled_at, profiled_at),
        )

    def set_review(
        self,
        onboarding_id: str,
        *,
        status: str,
        key_decisions: dict[str, str],
        updated_at: datetime,
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = %s, key_decisions = %s, updated_at = %s, "
            "validation_hash = NULL, validation_errors = NULL, validated_at = NULL",
            (status, Jsonb(key_decisions), updated_at),
        )

    def set_configuration(
        self, onboarding_id: str, *, status: str, updated_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = %s, updated_at = %s, validation_hash = NULL, "
            "validation_errors = NULL, validated_at = NULL",
            (status, updated_at),
        )

    def set_validation(
        self,
        onboarding_id: str,
        *,
        status: str,
        validation_hash: str,
        validation_errors: list[dict[str, str]],
        validated_at: datetime,
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = %s, validation_hash = %s, validation_errors = %s, "
            "validated_at = %s, updated_at = %s",
            (
                status,
                validation_hash,
                Jsonb(validation_errors),
                validated_at,
                validated_at,
            ),
        )

    def set_failed(
        self, onboarding_id: str, *, safe_error: str, updated_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'FAILED', safe_error = %s, updated_at = %s",
            (safe_error, updated_at),
        )

    def claim_approval(
        self,
        onboarding_id: str,
        *,
        expected_hash: str,
        approved_by: str,
        updated_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    "SELECT * FROM etl_app.onboarding_sessions WHERE onboarding_id = %s FOR UPDATE",
                    (onboarding_id,),
                ).fetchone()
                if row is None:
                    raise OnboardingRepositoryError("Onboarding session was not found")
                if row["approved_at"] is not None:
                    return dict(row), False
                if row["status"] == "APPROVING":
                    return dict(row), False
                if row["status"] != "READY_FOR_APPROVAL":
                    raise OnboardingRepositoryError("Configuration is not ready for approval")
                if row["validation_hash"] != expected_hash:
                    raise OnboardingRepositoryError("Validated configuration hash does not match")
                updated = connection.execute(
                    "UPDATE etl_app.onboarding_sessions SET status = 'APPROVING', "
                    "approved_by = %s, updated_at = %s, safe_error = NULL "
                    "WHERE onboarding_id = %s RETURNING *",
                    (approved_by, updated_at, onboarding_id),
                ).fetchone()
            assert updated is not None
            return dict(updated), True
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Could not claim configuration approval") from exc

    def finish_approval(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'APPROVED', approved_at = %s, approved_by = %s, "
            "approved_config_hash = %s, approved_config_key = %s, source_key = %s, "
            "git_commit_sha = %s, git_push_status = %s, dag_id = %s, "
            "updated_at = %s, safe_error = NULL",
            (
                values["approved_at"],
                values["approved_by"],
                values["approved_config_hash"],
                values["approved_config_key"],
                values["source_key"],
                values["git_commit_sha"],
                values["git_push_status"],
                values["dag_id"],
                values["approved_at"],
            ),
        )

    def fail_approval(
        self, onboarding_id: str, *, safe_error: str, updated_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'READY_FOR_APPROVAL', safe_error = %s, updated_at = %s",
            (safe_error, updated_at),
        )

    def invalidate_approval(self, onboarding_id: str, *, updated_at: datetime) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'CONFIGURING', validation_hash = NULL, validation_errors = NULL, "
            "validated_at = NULL, approved_by = NULL, safe_error = %s, updated_at = %s",
            ("Draft changed after validation; validate the current version again.", updated_at),
        )

    def begin_activation(self, onboarding_id: str, updated_at: datetime) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'ACTIVATING', updated_at = %s, safe_error = NULL",
            (updated_at,),
        )

    def finish_activation(
        self, onboarding_id: str, *, discovered: bool, updated_at: datetime
    ) -> dict[str, Any]:
        status = "READY_FOR_FIRST_RUN" if discovered else "ACTIVATION_FAILED"
        error = None if discovered else "Airflow has not discovered the approved dataset DAG yet."
        return self._update(
            onboarding_id,
            "status = %s, activation_checked_at = %s, updated_at = %s, safe_error = %s",
            (status, updated_at, updated_at, error),
        )

    def claim_first_run(
        self,
        onboarding_id: str,
        *,
        correlation_id: str,
        airflow_run_id: str,
        retry: bool,
        updated_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    "SELECT * FROM etl_app.onboarding_sessions WHERE onboarding_id = %s FOR UPDATE",
                    (onboarding_id,),
                ).fetchone()
                if row is None:
                    raise OnboardingRepositoryError("Onboarding session was not found")
                if row["status"] in {"TRIGGERING", "QUEUED", "RUNNING", "SUCCEEDED"}:
                    return dict(row), False
                allowed = row["status"] == "READY_FOR_FIRST_RUN" or (
                    retry and row["status"] == "FIRST_RUN_FAILED"
                )
                if not allowed:
                    raise OnboardingRepositoryError("Dataset is not ready for its first run")
                updated = connection.execute(
                    "UPDATE etl_app.onboarding_sessions SET status = 'TRIGGERING', "
                    "first_run_attempt = first_run_attempt + 1, "
                    "first_run_correlation_id = %s, airflow_run_id = %s, "
                    "airflow_state = NULL, etl_run_id = NULL, completed_at = NULL, "
                    "updated_at = %s, safe_error = NULL "
                    "WHERE onboarding_id = %s RETURNING *",
                    (correlation_id, airflow_run_id, updated_at, onboarding_id),
                ).fetchone()
            assert updated is not None
            return dict(updated), True
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Could not claim first-run request") from exc

    def mark_first_run_triggered(
        self, onboarding_id: str, *, airflow_state: str, updated_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'QUEUED', airflow_state = %s, updated_at = %s",
            (airflow_state, updated_at),
        )

    def sync_first_run(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = %s, airflow_state = %s, etl_run_id = COALESCE(%s, etl_run_id), "
            "completed_at = COALESCE(%s, completed_at), safe_error = %s, updated_at = %s",
            (
                values["status"],
                values.get("airflow_state"),
                values.get("etl_run_id"),
                values.get("completed_at"),
                values.get("safe_error"),
                values["updated_at"],
            ),
        )

    def _select(self, statement: str, parameters: tuple[Any, ...]) -> dict[str, Any] | None:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(statement, parameters).fetchone()
            return dict(row) if row else None
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Could not read onboarding session") from exc

    def _update(
        self,
        onboarding_id: str,
        assignments: str,
        parameters: tuple[Any, ...],
    ) -> dict[str, Any]:
        try:
            with self._connect() as connection, connection.transaction():
                row = connection.execute(
                    f"UPDATE etl_app.onboarding_sessions SET {assignments} "
                    "WHERE onboarding_id = %s RETURNING *",
                    parameters + (onboarding_id,),
                ).fetchone()
            if row is None:
                raise OnboardingRepositoryError("Onboarding session was not found")
            return dict(row)
        except psycopg.Error as exc:
            raise OnboardingRepositoryError("Could not update onboarding session") from exc
