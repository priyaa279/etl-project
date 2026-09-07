from __future__ import annotations

import os
from importlib import resources
from types import TracebackType
from typing import Any, Self

import psycopg
from psycopg.rows import dict_row

from metadata_etl.errors import ObservabilityError

OBSERVABILITY_VIEW_NAMES = (
    "pipeline_runs",
    "dataset_health",
    "daily_pipeline_summary",
    "quality_summary",
    "quality_daily_trend",
    "quarantine_summary",
    "schema_drift_summary",
    "watermark_status",
)


def observability_statements() -> tuple[str, ...]:
    sql_text = (
        resources.files("metadata_etl.sql")
        .joinpath("observability.sql")
        .read_text(encoding="utf-8")
    )
    return tuple(statement.strip() for statement in sql_text.split(";") if statement.strip())


def install_observability(connection: psycopg.Connection[Any]) -> None:
    """Create or refresh the read-only observability model without replacing metadata."""
    for statement in observability_statements():
        connection.execute(statement)


def monitoring_dsn() -> str:
    dsn = os.getenv("ETL_POSTGRES_DSN")
    if not dsn:
        raise ObservabilityError("ETL_POSTGRES_DSN is required for monitoring commands")
    return dsn


class MonitoringStore:
    """Read-only query access to the SQL observability views."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.connection: psycopg.Connection[dict[str, Any]] | None = None

    def __enter__(self) -> Self:
        try:
            self.connection = psycopg.connect(self.dsn, row_factory=dict_row)
        except psycopg.Error as exc:
            raise ObservabilityError(f"Could not connect to observability database: {exc}") from exc
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
    def conn(self) -> psycopg.Connection[dict[str, Any]]:
        if self.connection is None:
            raise RuntimeError("MonitoringStore is not connected")
        return self.connection

    def status(self, dataset: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM etl_observability.dataset_health"
        parameters: tuple[str, ...] = ()
        if dataset is not None:
            query += " WHERE dataset = %s"
            parameters = (dataset,)
        query += " ORDER BY dataset"
        try:
            with self.conn.transaction():
                rows = self.conn.execute(query, parameters).fetchall()
        except psycopg.Error as exc:
            raise ObservabilityError(f"Could not query dataset health: {exc}") from exc
        if dataset is not None and not rows:
            raise ObservabilityError(f"Dataset {dataset!r} was not found in operational metadata")
        return rows

    def runs(self, *, dataset: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        if limit < 1:
            raise ObservabilityError("Run limit must be a positive integer")
        query = "SELECT * FROM etl_observability.pipeline_runs"
        parameters: tuple[Any, ...]
        if dataset is None:
            parameters = (limit,)
        else:
            query += " WHERE dataset = %s"
            parameters = (dataset, limit)
        query += " ORDER BY started_at DESC, run_id DESC LIMIT %s"
        try:
            with self.conn.transaction():
                return self.conn.execute(query, parameters).fetchall()
        except psycopg.Error as exc:
            raise ObservabilityError(f"Could not query recent pipeline runs: {exc}") from exc
