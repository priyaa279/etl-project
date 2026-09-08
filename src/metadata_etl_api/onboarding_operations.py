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
                            'REVIEW_IN_PROGRESS', 'REVIEW_COMPLETE', 'FAILED'
                        )),
                        uploaded_at TIMESTAMPTZ NOT NULL,
                        profiled_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL,
                        profile_result JSONB,
                        key_decisions JSONB NOT NULL DEFAULT '{}'::jsonb,
                        safe_error TEXT
                    )
                    """
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
            "status = %s, key_decisions = %s, updated_at = %s",
            (status, Jsonb(key_decisions), updated_at),
        )

    def set_failed(
        self, onboarding_id: str, *, safe_error: str, updated_at: datetime
    ) -> dict[str, Any]:
        return self._update(
            onboarding_id,
            "status = 'FAILED', safe_error = %s, updated_at = %s",
            (safe_error, updated_at),
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
