from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from metadata_etl.config import ETLConfig
from metadata_etl_api.catalog import CatalogError, DatasetCatalog
from metadata_etl_api.settings import APISettings

DEFAULT_PREVIEW_LIMIT = 25
MAX_PREVIEW_LIMIT = 100
SENSITIVE_CLASSIFICATIONS = frozenset({"pii", "restricted"})


class TrustedDataError(RuntimeError):
    """A safe API boundary error for trusted-output inspection."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class TrustedDataRepository:
    """Read-only access to one target already resolved from approved configuration."""

    def __init__(self, dsn: str | None) -> None:
        self.dsn = dsn

    def preview(self, config: ETLConfig, *, limit: int, offset: int) -> dict[str, Any]:
        if not self.dsn:
            raise TrustedDataError(503, "Trusted data is temporarily unavailable.")
        classifications = {
            column.name: column.classification.lower()
            if column.classification is not None
            else None
            for column in config.columns
        }
        try:
            with (
                psycopg.connect(self.dsn, row_factory=dict_row) as connection,
                connection.transaction(),
            ):
                connection.execute("SET TRANSACTION READ ONLY")
                latest_run = connection.execute(
                    """
                    SELECT run_id, finished_at
                    FROM etl_meta.etl_run_ledger
                    WHERE dataset = %s AND status = 'SUCCEEDED'
                    ORDER BY finished_at DESC, run_id DESC
                    LIMIT 1
                    """,
                    (config.dataset,),
                ).fetchone()
                column_rows = connection.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (config.destination_schema, config.target_table),
                ).fetchall()
                if not column_rows:
                    return self._result(
                        config,
                        state="NOT_PUBLISHED",
                        columns=[],
                        rows=[],
                        total_rows=0,
                        limit=limit,
                        offset=offset,
                        latest_run=latest_run,
                    )

                columns = []
                redacted_names: set[str] = set()
                for row in column_rows:
                    name = row["column_name"]
                    classification = classifications.get(name)
                    redacted = classification in SENSITIVE_CLASSIFICATIONS
                    if redacted:
                        redacted_names.add(name)
                    columns.append(
                        {
                            "name": name,
                            "data_type": row["data_type"],
                            "classification": classification,
                            "redacted": redacted,
                        }
                    )

                target = sql.Identifier(config.destination_schema, config.target_table)
                total_row = connection.execute(
                    sql.SQL("SELECT COUNT(*) AS total_rows FROM {}").format(target)
                ).fetchone()
                total_rows = int(total_row["total_rows"])

                column_names = [column["name"] for column in columns]
                selected = [
                    sql.SQL("NULL AS {}").format(sql.Identifier(name))
                    if name in redacted_names
                    else sql.Identifier(name)
                    for name in column_names
                ]
                preferred = [name for name in config.load_keys if name in column_names]
                order_names = preferred + [name for name in column_names if name not in preferred]
                order_by = [
                    sql.SQL("{} NULLS LAST").format(sql.Identifier(name)) for name in order_names
                ]
                rows = list(
                    connection.execute(
                        sql.SQL("SELECT {} FROM {} ORDER BY {} LIMIT %s OFFSET %s").format(
                            sql.SQL(", ").join(selected),
                            target,
                            sql.SQL(", ").join(order_by),
                        ),
                        (limit, offset),
                    ).fetchall()
                )
                return self._result(
                    config,
                    state="AVAILABLE" if total_rows else "EMPTY",
                    columns=columns,
                    rows=rows,
                    total_rows=total_rows,
                    limit=limit,
                    offset=offset,
                    latest_run=latest_run,
                )
        except TrustedDataError:
            raise
        except psycopg.Error as exc:
            raise TrustedDataError(503, "Trusted data is temporarily unavailable.") from exc

    @staticmethod
    def _result(
        config: ETLConfig,
        *,
        state: str,
        columns: list[dict[str, Any]],
        rows: list[dict[str, Any]],
        total_rows: int,
        limit: int,
        offset: int,
        latest_run: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "dataset": config.dataset,
            "target": f"{config.destination_schema}.{config.target_table}",
            "state": state,
            "columns": columns,
            "rows": rows,
            "total_rows": total_rows,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(rows) < total_rows,
            "latest_successful_run_id": latest_run["run_id"] if latest_run else None,
            "last_updated": latest_run["finished_at"] if latest_run else None,
        }


class TrustedDataService:
    """Resolve logical datasets through approved config before any trusted-table query."""

    def __init__(
        self,
        *,
        enabled: bool,
        catalog: DatasetCatalog,
        repository: TrustedDataRepository,
    ) -> None:
        self.enabled = enabled
        self.catalog = catalog
        self.repository = repository

    @classmethod
    def from_settings(cls, settings: APISettings) -> TrustedDataService:
        return cls(
            enabled=settings.trusted_data_preview_enabled,
            catalog=DatasetCatalog(settings.config_dir),
            repository=TrustedDataRepository(settings.database_dsn),
        )

    def preview(self, dataset: str, *, limit: int, offset: int) -> dict[str, Any]:
        if not self.enabled:
            raise TrustedDataError(
                403,
                "Trusted data preview is disabled in this environment.",
            )
        config = self._config(dataset)
        return self.repository.preview(config, limit=limit, offset=offset)

    def pipeline_summary(self, dataset: str) -> dict[str, Any]:
        config = self._config(dataset)
        return {
            "dataset": config.dataset,
            "label": "Current Transformation Plan",
            "transformations": [
                {
                    "position": position,
                    "id": item.id,
                    "type": item.type,
                    "details": self._transformation_details(item.type, item.values),
                }
                for position, item in enumerate(config.transformations, start=1)
            ],
        }

    def _config(self, dataset: str) -> ETLConfig:
        try:
            return self.catalog.get(dataset)
        except CatalogError as exc:
            raise TrustedDataError(404, "Dataset not found.") from exc

    @staticmethod
    def _transformation_details(kind: str, values: dict[str, Any]) -> dict[str, Any]:
        fields: dict[str, Sequence[str]] = {
            "cast": ("column", "datatype", "format"),
            "filter": ("condition",),
            "derive": ("target_column", "datatype", "expression", "format"),
            "map": ("column", "mappings", "datatype", "default"),
            "deduplicate": ("keys", "order_by"),
        }
        return {
            name: values[name]
            for name in fields[kind]
            if name in values and values[name] is not None
        }
