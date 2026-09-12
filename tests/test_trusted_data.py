from __future__ import annotations

from pathlib import Path
from typing import Any, Self

import pytest
from fastapi.testclient import TestClient

from metadata_etl_api.catalog import DatasetCatalog
from metadata_etl_api.main import create_app
from metadata_etl_api.trusted_data import (
    TrustedDataError,
    TrustedDataRepository,
    TrustedDataService,
)


def _config(directory: Path, *, transformations: str = "[]") -> None:
    (directory / "customers.yaml").write_text(
        f"""
config_schema_version: "1.0"
dataset:
  name: customers
review:
  required: true
  approved: true
  approved_by: reviewer
source:
  type: csv
  path: data/incoming/customers.csv
columns:
  customer_id:
    type: string
    nullable: false
  email:
    type: string
    classification: pii
  note:
    type: string
transformations: {transformations}
contracts: []
load:
  strategy: full
  schema: trusted
  staging_table: customers_staging
  target_table: customers
""".strip()
        + "\n",
        encoding="utf-8",
    )


class _Cursor:
    def __init__(
        self, *, one: dict[str, Any] | None = None, all_rows: list[dict[str, Any]] | None = None
    ) -> None:
        self.one = one
        self.all_rows = all_rows or []

    def fetchone(self) -> dict[str, Any] | None:
        return self.one

    def fetchall(self) -> list[dict[str, Any]]:
        return self.all_rows


class _Transaction:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _Connection(_Transaction):
    def __init__(self, *, empty: bool = False, missing: bool = False) -> None:
        self.empty = empty
        self.missing = missing
        self.calls: list[tuple[object, tuple[Any, ...]]] = []
        self.composed_calls = 0

    def transaction(self) -> _Transaction:
        return _Transaction()

    def execute(self, query: object, parameters: tuple[Any, ...] = ()) -> _Cursor:
        self.calls.append((query, parameters))
        if isinstance(query, str):
            if query == "SET TRANSACTION READ ONLY":
                return _Cursor()
            if "etl_run_ledger" in query:
                return _Cursor(one={"run_id": "RUN_001", "finished_at": "2026-09-09T10:00:00Z"})
            if "information_schema.columns" in query:
                columns = (
                    []
                    if self.missing
                    else [
                        {"column_name": "customer_id", "data_type": "text"},
                        {"column_name": "email", "data_type": "text"},
                        {"column_name": "note", "data_type": "text"},
                    ]
                )
                return _Cursor(all_rows=columns)
        self.composed_calls += 1
        if self.composed_calls == 1:
            return _Cursor(one={"total_rows": 0 if self.empty else 2})
        rows = (
            []
            if self.empty
            else [
                {"customer_id": "00123", "email": None, "note": None},
                {"customer_id": "00451", "email": None, "note": "A long-lived customer"},
            ]
        )
        return _Cursor(all_rows=rows)


class _Connect:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def __call__(self, *_args: object, **_kwargs: object) -> _Connection:
        return self.connection


class _NoopRepository:
    def __init__(self) -> None:
        self.calls = 0

    def preview(self, *_args: object, **_kwargs: object) -> dict[str, Any]:
        self.calls += 1
        return {}


def test_repository_returns_real_rows_with_read_only_safe_identifiers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _config(tmp_path)
    config = DatasetCatalog(tmp_path).get("customers")
    connection = _Connection()
    monkeypatch.setattr("metadata_etl_api.trusted_data.psycopg.connect", _Connect(connection))

    result = TrustedDataRepository("postgresql://not-returned").preview(config, limit=25, offset=0)

    assert result["state"] == "AVAILABLE"
    assert result["target"] == "trusted.customers"
    assert result["total_rows"] == 2
    assert result["rows"][0]["customer_id"] == "00123"
    assert result["rows"][0]["note"] is None
    assert result["rows"][0]["email"] is None
    assert result["columns"][1] == {
        "name": "email",
        "data_type": "text",
        "classification": "pii",
        "redacted": True,
    }
    assert connection.calls[0] == ("SET TRANSACTION READ ONLY", ())
    assert connection.calls[1][1] == ("customers",)
    assert connection.calls[2][1] == ("trusted", "customers")
    assert connection.calls[-1][1] == (25, 0)
    rendered_queries = " ".join(repr(query) for query, _ in connection.calls)
    assert "Identifier('trusted', 'customers')" in rendered_queries
    assert "NULL AS" in rendered_queries
    assert "not-returned" not in str(result)


@pytest.mark.parametrize(
    ("connection", "state"),
    [(_Connection(empty=True), "EMPTY"), (_Connection(missing=True), "NOT_PUBLISHED")],
)
def test_repository_handles_empty_and_missing_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    connection: _Connection,
    state: str,
) -> None:
    _config(tmp_path)
    monkeypatch.setattr("metadata_etl_api.trusted_data.psycopg.connect", _Connect(connection))

    result = TrustedDataRepository("configured").preview(
        DatasetCatalog(tmp_path).get("customers"), limit=25, offset=0
    )

    assert result["state"] == state
    assert result["rows"] == []


@pytest.mark.parametrize(
    "dataset",
    ["../../private", "trusted.users; DROP TABLE users", 'customers" OR true --'],
)
def test_dataset_resolution_blocks_identifier_and_path_attacks(
    tmp_path: Path, dataset: str
) -> None:
    _config(tmp_path)
    repository = _NoopRepository()
    service = TrustedDataService(
        enabled=True,
        catalog=DatasetCatalog(tmp_path),
        repository=repository,  # type: ignore[arg-type]
    )

    with pytest.raises(TrustedDataError) as error:
        service.preview(dataset, limit=25, offset=0)

    assert error.value.status_code == 404
    assert repository.calls == 0


def test_preview_gate_is_disabled_by_default(tmp_path: Path) -> None:
    _config(tmp_path)
    service = TrustedDataService(
        enabled=False,
        catalog=DatasetCatalog(tmp_path),
        repository=_NoopRepository(),  # type: ignore[arg-type]
    )

    with pytest.raises(TrustedDataError) as error:
        service.preview("customers", limit=25, offset=0)

    assert error.value.status_code == 403


def test_disabled_preview_endpoint_returns_safe_403(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ETL_CONTROL_TRUSTED_DATA_PREVIEW_ENABLED", raising=False)

    response = TestClient(create_app()).get("/api/datasets/customers/trusted-data")

    assert response.status_code == 403
    assert response.json() == {"detail": "Trusted data preview is disabled in this environment."}


def test_pipeline_summary_uses_current_approved_configuration(tmp_path: Path) -> None:
    transformations = """
  - id: T001
    type: map
    column: note
    mappings:
      old: current
  - id: T002
    type: derive
    target_column: customer_label
    datatype: string
    expression: customer_id || '-' || note
"""
    _config(tmp_path, transformations=transformations)
    service = TrustedDataService(
        enabled=False,
        catalog=DatasetCatalog(tmp_path),
        repository=_NoopRepository(),  # type: ignore[arg-type]
    )

    result = service.pipeline_summary("customers")

    assert result["label"] == "Current Transformation Plan"
    assert [item["type"] for item in result["transformations"]] == ["map", "derive"]
    assert result["transformations"][1]["details"]["target_column"] == "customer_label"
    assert "path" not in str(result).lower()


class _EndpointService:
    def __init__(self, state: str = "AVAILABLE") -> None:
        self.state = state
        self.calls: list[tuple[str, int, int]] = []

    def preview(self, dataset: str, *, limit: int, offset: int) -> dict[str, Any]:
        self.calls.append((dataset, limit, offset))
        if dataset == "missing":
            raise TrustedDataError(404, "Dataset not found.")
        return {
            "dataset": dataset,
            "target": "trusted.customers",
            "state": self.state,
            "columns": [
                {
                    "name": "customer_id",
                    "data_type": "text",
                    "classification": None,
                    "redacted": False,
                },
                {"name": "note", "data_type": "text", "classification": None, "redacted": False},
            ],
            "rows": [] if self.state != "AVAILABLE" else [{"customer_id": "00123", "note": None}],
            "total_rows": 0 if self.state != "AVAILABLE" else 26,
            "limit": limit,
            "offset": offset,
            "has_more": self.state == "AVAILABLE" and offset + 1 < 26,
            "latest_successful_run_id": "RUN_001" if self.state != "NOT_PUBLISHED" else None,
            "last_updated": "2026-09-09T10:00:00Z" if self.state != "NOT_PUBLISHED" else None,
        }

    def pipeline_summary(self, dataset: str) -> dict[str, Any]:
        return {"dataset": dataset, "label": "Current Transformation Plan", "transformations": []}


def _client(service: _EndpointService) -> TestClient:
    app = create_app()
    app.state.trusted_data_service = service
    return TestClient(app)


def test_endpoint_defaults_paginates_and_returns_json_null() -> None:
    service = _EndpointService()
    response = _client(service).get("/api/datasets/customers/trusted-data")

    assert response.status_code == 200
    assert service.calls == [("customers", 25, 0)]
    assert response.json()["rows"] == [{"customer_id": "00123", "note": None}]
    assert response.json()["has_more"] is True


def test_endpoint_supports_next_page_without_accepting_database_controls() -> None:
    service = _EndpointService()
    response = _client(service).get(
        "/api/datasets/customers/trusted-data",
        params={
            "limit": 10,
            "offset": 10,
            "schema": "private",
            "table": "users",
            "sql": "DROP TABLE users",
        },
    )

    assert response.status_code == 200
    assert service.calls == [("customers", 10, 10)]
    assert "private" not in response.text
    assert "DROP TABLE" not in response.text


@pytest.mark.parametrize("query", ["limit=0", "limit=101", "limit=invalid", "offset=-1"])
def test_endpoint_rejects_invalid_pagination(query: str) -> None:
    response = _client(_EndpointService()).get(f"/api/datasets/customers/trusted-data?{query}")

    assert response.status_code == 422


def test_endpoint_handles_unknown_empty_and_never_published_datasets() -> None:
    assert _client(_EndpointService()).get("/api/datasets/missing/trusted-data").status_code == 404
    assert (
        _client(_EndpointService("EMPTY"))
        .get("/api/datasets/customers/trusted-data")
        .json()["state"]
        == "EMPTY"
    )
    assert (
        _client(_EndpointService("NOT_PUBLISHED"))
        .get("/api/datasets/customers/trusted-data")
        .json()["state"]
        == "NOT_PUBLISHED"
    )


def test_trusted_data_endpoint_is_get_only() -> None:
    response = _client(_EndpointService()).post("/api/datasets/customers/trusted-data")

    assert response.status_code == 405
