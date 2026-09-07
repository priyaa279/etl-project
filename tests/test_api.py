from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from metadata_etl_api.main import create_app
from metadata_etl_api.repository import RepositoryError


def _dataset() -> dict[str, Any]:
    return {
        "dataset": "customers",
        "health_status": "HEALTHY",
        "latest_run_status": "SUCCEEDED",
        "latest_run_id": "RUN_001",
        "latest_run_time": "2026-09-07T10:00:00Z",
        "latest_duration_seconds": 1.25,
        "latest_rows_extracted": 4,
        "latest_rows_loaded": 3,
        "latest_rows_quarantined": 0,
        "latest_drift_status": "NONE",
        "latest_watermark": None,
        "last_successful_run": "2026-09-07T10:00:01Z",
        "source_type": "csv",
        "load_strategy": "full",
    }


def _run() -> dict[str, Any]:
    return {
        "run_id": "RUN_001",
        "dataset": "customers",
        "status": "SUCCEEDED",
        "started_at": "2026-09-07T10:00:00Z",
        "completed_at": "2026-09-07T10:00:01Z",
        "duration_seconds": 1.25,
        "source_type": "csv",
        "load_strategy": "full",
        "run_mode": "normal",
        "backfill_from": None,
        "backfill_to": None,
        "rows_extracted": 4,
        "rows_transformed": 3,
        "rows_contract_passed": 3,
        "rows_quarantined": 0,
        "rows_loaded": 3,
        "rows_inserted": 3,
        "rows_updated": 0,
        "rows_expired": 0,
        "rows_history_inserted": 0,
        "raw_schema_hash": "raw-hash",
        "canonical_schema_hash": "canonical-hash",
        "drift_status": "NONE",
        "watermark_before": None,
        "watermark_after": None,
        "git_commit_sha": "git-sha",
        "config_hash": "config-hash",
    }


def _quality() -> dict[str, Any]:
    return {
        "run_id": "RUN_001",
        "dataset": "customers",
        "rule_id": "DQ001",
        "rule_type": "not_null",
        "records_checked": 4,
        "records_failed": 1,
        "failure_rate": 25.0,
        "status": "FAILED",
        "timestamp": "2026-09-07T10:00:01Z",
    }


class FakeRepository:
    def overview(self, *, hours: int) -> dict[str, Any]:
        return {
            "time_scope_hours": hours,
            "total_datasets": 3,
            "healthy_datasets": 1,
            "warning_datasets": 1,
            "failed_datasets": 1,
            "unknown_datasets": 0,
            "total_recent_runs": 10,
            "success_rate": 90,
            "total_rows_loaded": 100,
            "total_rows_quarantined": 4,
            "datasets_with_drift_warnings": 1,
        }

    def datasets(self) -> list[dict[str, Any]]:
        return [_dataset()]

    def dataset(self, dataset: str) -> dict[str, Any] | None:
        return _dataset() if dataset == "customers" else None

    def runs(self, **_values: Any) -> list[dict[str, Any]]:
        return [_run()]

    def run(self, run_id: str) -> dict[str, Any] | None:
        return _run() if run_id == "RUN_001" else None

    def run_quality(self, _run_id: str) -> list[dict[str, Any]]:
        return [_quality()]

    def dataset_quality(self, _dataset_name: str, *, limit: int) -> list[dict[str, Any]]:
        assert limit > 0
        return [_quality()]

    def quality_overview(self, *, days: int) -> dict[str, Any]:
        breakdown = {
            "label": "customers",
            "records_checked": 4,
            "records_failed": 1,
            "failure_rate": 25,
        }
        return {
            "time_scope_days": days,
            "total_records_checked": 4,
            "total_records_failed": 1,
            "failure_rate": 25,
            "rows_quarantined": 1,
            "by_dataset": [breakdown],
            "by_rule": [{**breakdown, "label": "DQ001"}],
            "by_rule_type": [{**breakdown, "label": "not_null"}],
            "recent_trend": [
                {
                    "date": "2026-09-07",
                    "records_checked": 4,
                    "records_failed": 1,
                    "failure_rate": 25,
                    "runs_affected": 1,
                }
            ],
        }

    def schema_drift(self, **_values: Any) -> list[dict[str, Any]]:
        return [
            {
                "dataset": "customers",
                "run_id": "RUN_001",
                "schema_level": "raw",
                "drift_type": "column_added",
                "policy": "warn",
                "action_taken": "WARNED",
                "detected_at": "2026-09-07T10:00:00Z",
                "old_schema_hash": "old",
                "new_schema_hash": "new",
            }
        ]

    def watermarks(self) -> list[dict[str, Any]]:
        watermark = self.watermark("customers")
        assert watermark is not None
        return [watermark]

    def watermark(self, dataset: str) -> dict[str, Any] | None:
        if dataset != "customers":
            return None
        return {
            "dataset": "customers",
            "watermark_column": "updated_at",
            "last_successful_value": "2026-09-07T10:00:00Z",
            "updated_at": "2026-09-07T10:00:01Z",
            "run_id": "RUN_001",
        }


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    app.state.repository = FakeRepository()
    return TestClient(app)


def test_health_endpoint(client: TestClient) -> None:
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_overview_and_dataset_list(client: TestClient) -> None:
    overview = client.get("/api/overview")
    datasets = client.get("/api/datasets")

    assert overview.status_code == 200
    assert overview.json()["time_scope_hours"] == 24
    assert overview.json()["success_rate"] == 90
    assert datasets.status_code == 200
    assert datasets.json()[0]["source_type"] == "csv"


def test_known_and_unknown_dataset_detail(client: TestClient) -> None:
    known = client.get("/api/datasets/customers")
    unknown = client.get("/api/datasets/missing")

    assert known.status_code == 200
    assert known.json()["health_status"] == "HEALTHY"
    assert unknown.status_code == 404
    assert unknown.json() == {"detail": "Dataset not found."}


def test_dataset_runs_quality_schema_and_watermark(client: TestClient) -> None:
    assert client.get("/api/datasets/customers/runs?limit=20").json()[0]["run_id"] == "RUN_001"
    assert client.get("/api/datasets/customers/quality").json()[0]["rule_id"] == "DQ001"
    drift = client.get("/api/datasets/customers/schema-drift").json()[0]
    assert drift["schema_level"] == "raw"
    watermark = client.get("/api/datasets/customers/watermark").json()["watermark"]
    assert watermark["watermark_column"] == "updated_at"


def test_run_detail_and_quality_exclude_sensitive_quarantine_data(client: TestClient) -> None:
    detail = client.get("/api/runs/RUN_001")
    quality = client.get("/api/runs/RUN_001/quality")

    assert detail.status_code == 200
    assert detail.json()["rows_contract_passed"] == 3
    assert quality.status_code == 200
    serialized = quality.text.lower()
    assert "failed_value" not in serialized
    assert "record_identifier" not in serialized
    assert "failure_reason" not in serialized
    assert "raw-secret" not in serialized


def test_global_runs_quality_drift_watermark_and_validation(client: TestClient) -> None:
    assert client.get("/api/runs?limit=10").status_code == 200
    assert client.get("/api/quality?days=30").json()["rows_quarantined"] == 1
    assert client.get("/api/schema-drift").status_code == 200
    assert client.get("/api/watermarks").status_code == 200
    assert client.get("/api/runs?limit=0").status_code == 422


def test_database_errors_are_generic_and_do_not_leak_secrets() -> None:
    class BrokenRepository(FakeRepository):
        def overview(self, *, hours: int) -> dict[str, Any]:
            raise RepositoryError(
                "postgresql://private-user:private-password@database.internal/etl"
            )

    app = create_app()
    app.state.repository = BrokenRepository()
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/api/overview")

    assert response.status_code == 503
    assert response.json() == {"detail": "Observability data is temporarily unavailable."}
    assert "private-password" not in response.text
