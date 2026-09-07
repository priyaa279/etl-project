from __future__ import annotations

import os
import uuid
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from fastapi.testclient import TestClient

from metadata_etl.config import load_config
from metadata_etl.schema import canonical_schema_fingerprint, raw_csv_schema_fingerprint
from metadata_etl_api.airflow_client import AirflowError
from metadata_etl_api.catalog import DatasetCatalog
from metadata_etl_api.main import create_app
from metadata_etl_api.operations import UploadRepository
from metadata_etl_api.repository import ObservabilityRepository
from metadata_etl_api.settings import APISettings
from metadata_etl_api.upload_service import UploadService

ROOT = Path(__file__).parents[1]


class MemoryOperations:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}

    def ensure_schema(self) -> None:
        return None

    def create(self, values: dict[str, Any]) -> dict[str, Any]:
        record = {
            **values,
            "status": "UPLOADED",
            "validated_at": None,
            "preflight_result": None,
            "triggered_at": None,
            "airflow_dag_id": None,
            "airflow_run_id": None,
            "airflow_state": None,
            "etl_run_id": None,
            "completed_at": None,
            "safe_error": None,
        }
        self.records[record["upload_id"]] = record
        return deepcopy(record)

    def get(self, upload_id: str) -> dict[str, Any] | None:
        record = self.records.get(upload_id)
        return deepcopy(record) if record else None

    def set_preflight(
        self, upload_id: str, status: str, result: dict[str, Any], validated_at: datetime
    ) -> dict[str, Any]:
        self.records[upload_id].update(
            status=status, preflight_result=result, validated_at=validated_at
        )
        return deepcopy(self.records[upload_id])

    def claim_trigger(
        self,
        upload_id: str,
        dag_id: str,
        airflow_run_id: str,
        triggered_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        record = self.records[upload_id]
        if record["status"] in {"TRIGGERING", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED"}:
            return deepcopy(record), False
        if record["status"] not in {"READY", "WARNING"}:
            from metadata_etl_api.operations import OperationsError

            raise OperationsError("Upload is not ready to run")
        record.update(
            status="TRIGGERING",
            airflow_dag_id=dag_id,
            airflow_run_id=airflow_run_id,
            triggered_at=triggered_at,
        )
        return deepcopy(record), True

    def mark_triggered(
        self, upload_id: str, airflow_run_id: str, airflow_state: str | None
    ) -> dict[str, Any]:
        self.records[upload_id].update(
            status="QUEUED", airflow_run_id=airflow_run_id, airflow_state=airflow_state
        )
        return deepcopy(self.records[upload_id])

    def mark_failed(
        self, upload_id: str, safe_error: str, completed_at: datetime
    ) -> dict[str, Any]:
        self.records[upload_id].update(
            status="FAILED", safe_error=safe_error, completed_at=completed_at
        )
        return deepcopy(self.records[upload_id])

    def sync(self, upload_id: str, **values: Any) -> dict[str, Any]:
        self.records[upload_id].update(values)
        return deepcopy(self.records[upload_id])


class FakeObservability:
    def __init__(self) -> None:
        self.baseline: tuple[str | None, str | None] = (None, None)
        self.runs: dict[str, dict[str, Any]] = {}
        self.lookups: list[str] = []

    def schema_baseline(self, _dataset: str) -> tuple[str | None, str | None]:
        return self.baseline

    def run_by_correlation(self, correlation_id: str) -> dict[str, Any] | None:
        self.lookups.append(correlation_id)
        return self.runs.get(correlation_id)


class FakeAirflow:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.triggers: list[dict[str, str]] = []
        self.state = "queued"

    def trigger(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        source_override: str,
        correlation_id: str,
    ) -> dict[str, object]:
        if self.fail:
            raise AirflowError("private upstream detail")
        self.triggers.append(
            {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "source_override": source_override,
                "correlation_id": correlation_id,
            }
        )
        return {"dag_run_id": dag_run_id, "state": self.state}

    def run(self, _dag_id: str, _dag_run_id: str) -> dict[str, object]:
        return {"state": self.state}


def _settings(tmp_path: Path, *, enabled: bool = True, max_bytes: int = 1_000_000) -> APISettings:
    return APISettings(
        database_dsn=None,
        allowed_origins=("http://localhost:4173",),
        operations_enabled=enabled,
        config_dir=ROOT / "configs",
        upload_root=tmp_path / "uploads",
        airflow_upload_root="/opt/airflow/data/uploads",
        upload_max_bytes=max_bytes,
        airflow_api_url="http://airflow.test",
        airflow_username=None,
        airflow_password=None,
        airflow_token="test-token",
    )


def _client(
    tmp_path: Path,
    *,
    enabled: bool = True,
    max_bytes: int = 1_000_000,
    airflow: FakeAirflow | None = None,
    observability: FakeObservability | None = None,
) -> tuple[TestClient, MemoryOperations, FakeAirflow, FakeObservability]:
    settings = _settings(tmp_path, enabled=enabled, max_bytes=max_bytes)
    operations = MemoryOperations()
    airflow = airflow or FakeAirflow()
    observability = observability or FakeObservability()
    service = UploadService(
        settings,
        operations,  # type: ignore[arg-type]
        observability,  # type: ignore[arg-type]
        DatasetCatalog(settings.config_dir),
        airflow,  # type: ignore[arg-type]
    )
    app = create_app()
    app.state.upload_service = service
    return TestClient(app), operations, airflow, observability


def _upload(client: TestClient, dataset: str = "customers", content: bytes | None = None):
    content = content or (ROOT / "data" / "incoming" / "customers.csv").read_bytes()
    return client.post(
        f"/api/datasets/{dataset}/uploads",
        files={"file": ("batch.csv", content, "text/plain")},
    )


def test_known_upload_is_controlled_hashed_and_never_exposes_server_path(tmp_path: Path) -> None:
    client, operations, _, _ = _client(tmp_path)
    first = _upload(client)
    second = _upload(client)

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["sha256"] == second.json()["sha256"]
    assert first.json()["upload_id"] != second.json()["upload_id"]
    assert (
        operations.records[first.json()["upload_id"]]["landing_key"]
        != operations.records[second.json()["upload_id"]]["landing_key"]
    )
    serialized = first.text.lower()
    assert "landing" not in serialized
    assert "data/uploads" not in serialized
    assert "opt/airflow" not in serialized


def test_unknown_postgres_wrong_type_and_oversized_uploads_are_rejected(tmp_path: Path) -> None:
    client, _, _, _ = _client(tmp_path, max_bytes=4)

    assert _upload(client, "missing").status_code == 404
    postgres = client.post(
        "/api/datasets/postgres_assets/uploads",
        files={"file": ("assets.csv", b"a\n1\n", "text/csv")},
    )
    wrong = client.post(
        "/api/datasets/customers/uploads",
        files={"file": ("batch.json", b"{}", "text/csv")},
    )
    oversized = _upload(client, content=b"12345")

    assert postgres.status_code == 409
    assert "does not accept file uploads" in postgres.json()["detail"]
    assert wrong.status_code == 415
    assert oversized.status_code == 413


def test_unapproved_config_cannot_enter_the_upload_flow(tmp_path: Path) -> None:
    config = yaml.safe_load((ROOT / "configs" / "customers.yaml").read_text(encoding="utf-8"))
    config["review"]["approved"] = False
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "unapproved.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )
    settings = _settings(tmp_path)
    service = UploadService(
        settings,
        MemoryOperations(),  # type: ignore[arg-type]
        FakeObservability(),  # type: ignore[arg-type]
        DatasetCatalog(config_dir),
        FakeAirflow(),  # type: ignore[arg-type]
    )
    app = create_app()
    app.state.upload_service = service
    client = TestClient(app)

    capability = client.get("/api/datasets/customers/upload-capability")
    upload = _upload(client)

    assert capability.status_code == 404
    assert upload.status_code == 404
    assert "approved configuration" in upload.json()["detail"]


def test_traversal_filename_cannot_escape_root(tmp_path: Path) -> None:
    client, operations, _, _ = _client(tmp_path)
    response = client.post(
        "/api/datasets/customers/uploads",
        files={
            "file": (
                "../../outside.csv",
                (ROOT / "data" / "incoming" / "customers.csv").read_bytes(),
                "text/csv",
            )
        },
    )

    assert response.status_code == 201
    record = operations.records[response.json()["upload_id"]]
    stored = (_settings(tmp_path).upload_root / record["landing_key"]).resolve()
    assert stored.is_relative_to(_settings(tmp_path).upload_root.resolve())
    assert not (tmp_path / "outside.csv").exists()


def test_ready_warning_and_blocked_preflight_use_existing_schema_logic(tmp_path: Path) -> None:
    observability = FakeObservability()
    config = load_config(ROOT / "configs" / "customers.yaml")
    observability.baseline = (
        raw_csv_schema_fingerprint(config.source_path).json,  # type: ignore[arg-type]
        canonical_schema_fingerprint(config.columns).json,
    )
    client, _, _, _ = _client(tmp_path, observability=observability)

    ready_upload = _upload(client).json()
    ready = client.post(f"/api/uploads/{ready_upload['upload_id']}/validate")

    original = (ROOT / "data" / "incoming" / "customers.csv").read_text(encoding="utf-8")
    lines = [line for line in original.splitlines() if line]
    added = "\n".join([lines[0] + ",Phone", *[line + ",555-0100" for line in lines[1:]]])
    warning_upload = _upload(client, content=added.encode()).json()
    warning = client.post(f"/api/uploads/{warning_upload['upload_id']}/validate")

    removed = "Customer ID,First Name\n00123,Ada\n"
    blocked_upload = _upload(client, content=removed.encode()).json()
    blocked = client.post(f"/api/uploads/{blocked_upload['upload_id']}/validate")

    assert ready.json()["status"] == "READY"
    assert warning.json()["status"] == "WARNING"
    assert warning.json()["preflight_result"]["drift"]["events"][0]["policy"] == "warn"
    assert blocked.json()["status"] == "BLOCKED"
    assert blocked.json()["preflight_result"]["drift"]["events"]


@pytest.mark.parametrize(
    ("dataset", "filename", "content"),
    [
        ("customers", "bad.csv", b"Customer ID,First Name\n00123\n"),
        ("portfolio_order_lines", "bad.json", b'{"orders": [}'),
        ("portfolio_sensor_telemetry", "bad.parquet", b"not parquet"),
    ],
)
def test_malformed_sources_are_blocked(
    tmp_path: Path, dataset: str, filename: str, content: bytes
) -> None:
    client, _, _, _ = _client(tmp_path)
    upload = client.post(
        f"/api/datasets/{dataset}/uploads",
        files={"file": (filename, content, "application/octet-stream")},
    )

    assert upload.status_code == 201
    validation = client.post(f"/api/uploads/{upload.json()['upload_id']}/validate")
    assert validation.status_code == 200
    assert validation.json()["status"] == "BLOCKED"


def test_feature_gate_and_blocked_run_are_enforced_server_side(tmp_path: Path) -> None:
    disabled, _, _, _ = _client(tmp_path / "disabled", enabled=False)
    capability = disabled.get("/api/datasets/customers/upload-capability")
    assert capability.status_code == 200
    assert capability.json()["upload_eligible"] is False
    assert "disabled" in capability.json()["reason"].lower()
    assert _upload(disabled).status_code == 403

    client, operations, airflow, _ = _client(tmp_path / "blocked")
    upload = _upload(client, content=b"Customer ID\n00123\n").json()
    validated = client.post(f"/api/uploads/{upload['upload_id']}/validate").json()
    assert validated["status"] == "BLOCKED"
    run = client.post(f"/api/uploads/{upload['upload_id']}/run")
    assert run.status_code == 409
    assert airflow.triggers == []
    assert operations.records[upload["upload_id"]]["triggered_at"] is None


def test_operations_disabled_mode_rejects_an_existing_ready_run_request(tmp_path: Path) -> None:
    client, operations, airflow, observability = _client(tmp_path)
    upload = _upload(client).json()
    client.post(f"/api/uploads/{upload['upload_id']}/validate")
    disabled = UploadService(
        _settings(tmp_path, enabled=False),
        operations,  # type: ignore[arg-type]
        observability,  # type: ignore[arg-type]
        DatasetCatalog(ROOT / "configs"),
        airflow,  # type: ignore[arg-type]
    )
    client.app.state.upload_service = disabled

    response = client.post(f"/api/uploads/{upload['upload_id']}/run")

    assert response.status_code == 403
    assert airflow.triggers == []


def test_ready_run_is_generic_idempotent_and_persists_airflow_identifiers(tmp_path: Path) -> None:
    client, operations, airflow, _ = _client(tmp_path)
    upload = _upload(client).json()
    client.post(f"/api/uploads/{upload['upload_id']}/validate")

    first = client.post(f"/api/uploads/{upload['upload_id']}/run")
    second = client.post(f"/api/uploads/{upload['upload_id']}/run")

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(airflow.triggers) == 1
    trigger = airflow.triggers[0]
    assert trigger["dag_id"] == "etl_customers"
    assert trigger["correlation_id"] == upload["upload_id"]
    assert trigger["source_override"].startswith("/opt/airflow/data/uploads/customers/")
    record = operations.records[upload["upload_id"]]
    assert record["airflow_run_id"] == f"upload__{upload['upload_id']}"


def test_airflow_failure_is_safe_and_credentials_are_not_exposed(tmp_path: Path) -> None:
    client, operations, _, _ = _client(tmp_path, airflow=FakeAirflow(fail=True))
    upload = _upload(client).json()
    client.post(f"/api/uploads/{upload['upload_id']}/validate")

    response = client.post(f"/api/uploads/{upload['upload_id']}/run")

    assert response.status_code == 502
    assert response.json() == {"detail": "Airflow could not accept the run request."}
    assert "private" not in response.text
    assert operations.records[upload["upload_id"]]["status"] == "FAILED"


def test_monitoring_uses_exact_correlation_and_returns_safe_etl_metrics(tmp_path: Path) -> None:
    client, _, airflow, observability = _client(tmp_path)
    upload = _upload(client).json()
    client.post(f"/api/uploads/{upload['upload_id']}/validate")
    client.post(f"/api/uploads/{upload['upload_id']}/run")
    airflow.state = "success"
    observability.runs[upload["upload_id"]] = {
        "run_id": "RUN_EXACT",
        "status": "SUCCEEDED",
        "started_at": datetime.now(UTC),
        "completed_at": datetime.now(UTC),
        "duration_seconds": 1.2,
        "rows_extracted": 4,
        "rows_transformed": 3,
        "rows_contract_passed": 3,
        "rows_quarantined": 0,
        "rows_loaded": 3,
        "drift_status": "NONE",
    }

    response = client.get(f"/api/uploads/{upload['upload_id']}")

    assert response.json()["etl_run_id"] == "RUN_EXACT"
    assert response.json()["etl_run"]["rows_loaded"] == 3
    assert observability.lookups[-1] == upload["upload_id"]
    serialized = response.text.lower()
    for forbidden in ("landing_key", "raw_path", "failed_value", "postgresql://", "password"):
        assert forbidden not in serialized


def test_airflow_success_without_correlated_etl_result_fails_safely(tmp_path: Path) -> None:
    client, _, airflow, _ = _client(tmp_path)
    upload = _upload(client).json()
    client.post(f"/api/uploads/{upload['upload_id']}/validate")
    client.post(f"/api/uploads/{upload['upload_id']}/run")
    airflow.state = "success"

    response = client.get(f"/api/uploads/{upload['upload_id']}")

    assert response.json()["status"] == "FAILED"
    assert response.json()["safe_error"] == ("Airflow completed without a correlated ETL result.")


def test_concurrent_same_dataset_uploads_cannot_resolve_to_each_others_run(tmp_path: Path) -> None:
    client, _, airflow, observability = _client(tmp_path)
    uploads = [_upload(client).json(), _upload(client).json()]
    for upload in uploads:
        client.post(f"/api/uploads/{upload['upload_id']}/validate")
        client.post(f"/api/uploads/{upload['upload_id']}/run")
    airflow.state = "success"
    for index, upload in enumerate(uploads, start=1):
        observability.runs[upload["upload_id"]] = {
            "run_id": f"RUN_EXACT_{index}",
            "status": "SUCCEEDED",
            "started_at": datetime.now(UTC),
            "completed_at": datetime.now(UTC),
            "duration_seconds": 1.0,
            "rows_extracted": index,
            "rows_transformed": index,
            "rows_contract_passed": index,
            "rows_quarantined": 0,
            "rows_loaded": index,
            "drift_status": "NONE",
        }

    first = client.get(f"/api/uploads/{uploads[0]['upload_id']}").json()
    second = client.get(f"/api/uploads/{uploads[1]['upload_id']}").json()

    assert first["etl_run_id"] == "RUN_EXACT_1"
    assert second["etl_run_id"] == "RUN_EXACT_2"
    assert first["etl_run"]["rows_loaded"] == 1
    assert second["etl_run"]["rows_loaded"] == 2


@pytest.mark.skipif(
    not os.getenv("ETL_TEST_POSTGRES_DSN"),
    reason="ETL_TEST_POSTGRES_DSN is required for PostgreSQL integration verification",
)
def test_preflight_does_not_mutate_etl_truth(tmp_path: Path) -> None:
    dsn = os.environ["ETL_TEST_POSTGRES_DSN"]
    dataset = f"preflight_safety_{uuid.uuid4().hex[:10]}"
    config = yaml.safe_load((ROOT / "configs" / "customers.yaml").read_text(encoding="utf-8"))
    config["dataset"]["name"] = dataset
    config["source"]["path"] = str(ROOT / "data" / "incoming" / "customers.csv")
    config["runtime"]["raw_root"] = str(tmp_path / "raw")
    config["load"]["staging_table"] = f"{dataset}_staging"
    config["load"]["target_table"] = dataset
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    (config_dir / "dataset.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )
    settings = APISettings(
        **{
            **_settings(tmp_path).__dict__,
            "database_dsn": dsn,
            "config_dir": config_dir,
        }
    )
    operations = UploadRepository(dsn)
    service = UploadService(
        settings,
        operations,
        ObservabilityRepository(dsn),
        DatasetCatalog(config_dir),
        FakeAirflow(),  # type: ignore[arg-type]
    )
    app = create_app()
    app.state.upload_service = service
    client = TestClient(app)

    def etl_truth(connection: psycopg.Connection[Any]) -> tuple[Any, ...]:
        return connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM etl_meta.etl_run_ledger WHERE dataset = %s),
                (SELECT COUNT(*) FROM etl_meta.schema_drift_history WHERE dataset = %s),
                (SELECT COUNT(*) FROM etl_meta.data_quality_results WHERE dataset = %s),
                (SELECT COUNT(*) FROM etl_meta.etl_quarantine WHERE dataset = %s),
                (SELECT COUNT(*) FROM etl_meta.etl_watermarks WHERE dataset = %s),
                to_regclass(%s)
            """,
            (dataset, dataset, dataset, dataset, dataset, f"public.{dataset}"),
        ).fetchone()

    with psycopg.connect(dsn) as connection:
        before = etl_truth(connection)
    upload = _upload(client, dataset=dataset).json()
    validation = client.post(f"/api/uploads/{upload['upload_id']}/validate")
    with psycopg.connect(dsn) as connection:
        after = etl_truth(connection)
        connection.execute(
            "DELETE FROM etl_app.upload_sessions WHERE upload_id = %s",
            (upload["upload_id"],),
        )

    assert validation.status_code == 200
    assert validation.json()["status"] == "READY"
    assert before == (0, 0, 0, 0, 0, None)
    assert after == before
