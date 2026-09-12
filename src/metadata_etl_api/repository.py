from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

import psycopg
from psycopg.rows import dict_row


class RepositoryError(RuntimeError):
    """A safe boundary error for observability storage failures."""


class ObservabilityRepository:
    """Read-only, parameterized access to the existing observability views."""

    RUN_SORT_FIELDS: ClassVar[dict[str, str]] = {
        "run_id": "LOWER(run_id)",
        "dataset": "LOWER(dataset)",
        "status": "LOWER(status)",
        "started_at": "started_at",
        "duration_seconds": "duration_seconds",
        "source_type": "LOWER(source_type)",
        "load_strategy": "LOWER(load_strategy)",
        "rows_loaded": "rows_loaded",
        "rows_quarantined": "rows_quarantined",
    }
    QUALITY_SORT_FIELDS: ClassVar[dict[str, str]] = {
        "dataset": "LOWER(dataset)",
        "rule_id": "LOWER(rule_id)",
        "rule_type": "LOWER(rule_type)",
        "records_checked": "records_checked",
        "records_failed": "records_failed",
        "failure_rate": "failure_rate",
        "status": "LOWER(status)",
        "timestamp": "timestamp",
    }
    DRIFT_SORT_FIELDS: ClassVar[dict[str, str]] = {
        "dataset": "LOWER(dataset)",
        "schema_level": "LOWER(schema_level)",
        "drift_type": "LOWER(drift_type)",
        "policy": "LOWER(policy)",
        "action_taken": "LOWER(action_taken)",
        "detected_at": "detected_at",
    }

    def __init__(self, dsn: str | None) -> None:
        self.dsn = dsn

    def _fetch_all(self, query: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
        if not self.dsn:
            raise RepositoryError("Observability database is not configured")
        try:
            with (
                psycopg.connect(self.dsn, row_factory=dict_row) as connection,
                connection.transaction(),
            ):
                connection.execute("SET TRANSACTION READ ONLY")
                return list(connection.execute(query, parameters).fetchall())
        except psycopg.Error as exc:
            raise RepositoryError("Observability database query failed") from exc

    def _fetch_one(self, query: str, parameters: Sequence[Any] = ()) -> dict[str, Any] | None:
        rows = self._fetch_all(query, parameters)
        return rows[0] if rows else None

    @staticmethod
    def _order_by(
        field: str,
        direction: str,
        allowed: dict[str, str],
        tie_breakers: Sequence[tuple[str, str]],
    ) -> str:
        if field not in allowed or direction not in {"asc", "desc"}:
            raise RepositoryError("Unsupported sort request")
        clauses = [f"{allowed[field]} {direction.upper()} NULLS LAST"]
        for tie_field, tie_direction in tie_breakers:
            if tie_field != field:
                clauses.append(f"{allowed[tie_field]} {tie_direction} NULLS LAST")
        return ", ".join(clauses)

    def overview(self, *, hours: int = 24) -> dict[str, Any]:
        row = self._fetch_one(
            """
            WITH health AS (
                SELECT * FROM etl_observability.dataset_health
            ),
            recent_runs AS (
                SELECT *
                FROM etl_observability.pipeline_runs
                WHERE started_at >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
            )
            SELECT
                %s::INTEGER AS time_scope_hours,
                (SELECT COUNT(*) FROM health)::BIGINT AS total_datasets,
                (SELECT COUNT(*) FROM health WHERE health_status = 'HEALTHY')::BIGINT
                    AS healthy_datasets,
                (SELECT COUNT(*) FROM health WHERE health_status = 'WARNING')::BIGINT
                    AS warning_datasets,
                (SELECT COUNT(*) FROM health WHERE health_status = 'FAILED')::BIGINT
                    AS failed_datasets,
                (SELECT COUNT(*) FROM health WHERE health_status = 'UNKNOWN')::BIGINT
                    AS unknown_datasets,
                (SELECT COUNT(*) FROM recent_runs)::BIGINT AS total_recent_runs,
                COALESCE(
                    (SELECT ROUND(
                        COUNT(*) FILTER (WHERE status = 'SUCCEEDED')::NUMERIC * 100.0
                        / NULLIF(COUNT(*), 0), 2
                    ) FROM recent_runs),
                    0
                ) AS success_rate,
                COALESCE((SELECT SUM(COALESCE(rows_loaded, 0)) FROM recent_runs), 0)::BIGINT
                    AS total_rows_loaded,
                COALESCE((SELECT SUM(COALESCE(rows_quarantined, 0)) FROM recent_runs), 0)::BIGINT
                    AS total_rows_quarantined,
                (SELECT COUNT(*) FROM health WHERE latest_drift_status = 'WARN')::BIGINT
                    AS datasets_with_drift_warnings
            """,
            (hours, hours),
        )
        assert row is not None
        return row

    @staticmethod
    def _dataset_query() -> str:
        return """
            SELECT
                health.dataset,
                health.health_status,
                health.latest_run_status,
                health.latest_run_id,
                health.latest_run_time,
                health.latest_duration_seconds,
                health.latest_rows_extracted,
                health.latest_rows_loaded,
                health.latest_rows_quarantined,
                health.latest_drift_status,
                health.latest_watermark,
                health.last_successful_run,
                runs.source_type,
                runs.load_strategy
            FROM etl_observability.dataset_health AS health
            LEFT JOIN etl_observability.pipeline_runs AS runs
                ON runs.run_id = health.latest_run_id
        """

    def datasets(self) -> list[dict[str, Any]]:
        return self._fetch_all(self._dataset_query() + " ORDER BY health.dataset")

    def dataset(self, dataset: str) -> dict[str, Any] | None:
        return self._fetch_one(
            self._dataset_query() + " WHERE health.dataset = %s",
            (dataset,),
        )

    def runs(
        self,
        *,
        limit: int,
        dataset: str | None = None,
        search: str | None = None,
        status: str | None = None,
        load_strategy: str | None = None,
        sort: str = "started_at",
        direction: str = "desc",
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        parameters: list[Any] = []
        if dataset:
            conditions.append("dataset = %s")
            parameters.append(dataset)
        if search:
            conditions.append("(LOWER(run_id) LIKE %s OR LOWER(dataset) LIKE %s)")
            search_pattern = f"%{search.lower()}%"
            parameters.extend((search_pattern, search_pattern))
        if status:
            conditions.append("status = %s")
            parameters.append(status)
        if load_strategy:
            conditions.append("load_strategy = %s")
            parameters.append(load_strategy)
        query = "SELECT * FROM etl_observability.pipeline_runs"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        order_by = self._order_by(
            sort,
            direction,
            self.RUN_SORT_FIELDS,
            (("started_at", "DESC"), ("run_id", "DESC")),
        )
        query += f" ORDER BY {order_by} LIMIT %s"
        parameters.append(limit)
        return self._fetch_all(query, parameters)

    def run(self, run_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM etl_observability.pipeline_runs WHERE run_id = %s",
            (run_id,),
        )

    def run_by_correlation(self, correlation_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM etl_observability.pipeline_runs WHERE correlation_id = %s",
            (correlation_id,),
        )

    def schema_baseline(self, dataset: str) -> tuple[str | None, str | None]:
        row = self._fetch_one(
            """
            SELECT raw_schema_json, canonical_schema_json
            FROM etl_meta.etl_run_ledger
            WHERE dataset = %s AND status = 'SUCCEEDED'
              AND raw_schema_json IS NOT NULL AND canonical_schema_json IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            (dataset,),
        )
        if row is None:
            return None, None
        return row["raw_schema_json"], row["canonical_schema_json"]

    def run_quality(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT run_id, dataset, rule_id, rule_type, records_checked,
                   records_failed, failure_rate, status, timestamp
            FROM etl_observability.quality_summary
            WHERE run_id = %s
            ORDER BY rule_id
            """,
            (run_id,),
        )

    def dataset_quality(
        self,
        dataset: str,
        *,
        limit: int,
        sort: str = "timestamp",
        direction: str = "desc",
    ) -> list[dict[str, Any]]:
        order_by = self._order_by(
            sort,
            direction,
            self.QUALITY_SORT_FIELDS,
            (("timestamp", "DESC"), ("rule_id", "ASC")),
        )
        return self._fetch_all(
            f"""
            SELECT run_id, dataset, rule_id, rule_type, records_checked,
                   records_failed, failure_rate, status, timestamp
            FROM etl_observability.quality_summary
            WHERE dataset = %s
            ORDER BY {order_by}, run_id DESC
            LIMIT %s
            """,
            (dataset, limit),
        )

    def quality_overview(self, *, days: int) -> dict[str, Any]:
        totals = self._fetch_one(
            """
            SELECT
                %s::INTEGER AS time_scope_days,
                COALESCE(SUM(records_checked), 0)::BIGINT AS total_records_checked,
                COALESCE(SUM(records_failed), 0)::BIGINT AS total_records_failed,
                CASE WHEN COALESCE(SUM(records_checked), 0) = 0 THEN 0::NUMERIC
                     ELSE ROUND(SUM(records_failed)::NUMERIC * 100.0
                         / SUM(records_checked), 2)
                END AS failure_rate,
                COALESCE((
                    SELECT SUM(COALESCE(rows_quarantined, 0))
                    FROM etl_observability.pipeline_runs
                    WHERE started_at >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 day')
                ), 0)::BIGINT AS rows_quarantined
            FROM etl_observability.quality_summary
            WHERE timestamp >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 day')
            """,
            (days, days, days),
        )
        assert totals is not None
        window = " WHERE timestamp >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 day')"
        breakdowns: dict[str, list[dict[str, Any]]] = {}
        for key, column in (
            ("by_dataset", "dataset"),
            ("by_rule", "rule_id"),
            ("by_rule_type", "rule_type"),
        ):
            breakdowns[key] = self._fetch_all(
                f"""
                SELECT {column} AS label,
                       SUM(records_checked)::BIGINT AS records_checked,
                       SUM(records_failed)::BIGINT AS records_failed,
                       CASE WHEN SUM(records_checked) = 0 THEN 0::NUMERIC
                            ELSE ROUND(SUM(records_failed)::NUMERIC * 100.0
                                / SUM(records_checked), 2)
                       END AS failure_rate
                FROM etl_observability.quality_summary
                {window}
                GROUP BY {column}
                ORDER BY records_failed DESC, {column}
                """,
                (days,),
            )
        trend = self._fetch_all(
            """
            SELECT date,
                   SUM(records_checked)::BIGINT AS records_checked,
                   SUM(records_failed)::BIGINT AS records_failed,
                   CASE WHEN SUM(records_checked) = 0 THEN 0::NUMERIC
                        ELSE ROUND(SUM(records_failed)::NUMERIC * 100.0
                            / SUM(records_checked), 2)
                   END AS failure_rate,
                   SUM(runs_affected)::BIGINT AS runs_affected
            FROM etl_observability.quality_daily_trend
            WHERE date >= CURRENT_DATE - %s
            GROUP BY date
            ORDER BY date
            """,
            (days,),
        )
        return {**totals, **breakdowns, "recent_trend": trend}

    def schema_drift(
        self,
        *,
        limit: int,
        dataset: str | None = None,
        drift_type: str | None = None,
        action_taken: str | None = None,
        sort: str = "detected_at",
        direction: str = "desc",
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM etl_observability.schema_drift_summary"
        conditions: list[str] = []
        parameters: list[Any] = []
        if dataset:
            conditions.append("dataset = %s")
            parameters.append(dataset)
        if drift_type:
            conditions.append("drift_type = %s")
            parameters.append(drift_type)
        if action_taken:
            conditions.append("action_taken = %s")
            parameters.append(action_taken)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        order_by = self._order_by(
            sort,
            direction,
            self.DRIFT_SORT_FIELDS,
            (("detected_at", "DESC"), ("dataset", "ASC")),
        )
        query += f" ORDER BY {order_by}, run_id DESC LIMIT %s"
        parameters.append(limit)
        return self._fetch_all(query, parameters)

    def watermarks(self) -> list[dict[str, Any]]:
        return self._fetch_all("SELECT * FROM etl_observability.watermark_status ORDER BY dataset")

    def watermark(self, dataset: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM etl_observability.watermark_status WHERE dataset = %s",
            (dataset,),
        )
