from __future__ import annotations

import os
import shutil
import uuid
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pytest
import yaml
from fastapi.testclient import TestClient

from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError
from metadata_etl.orchestration import discover_scheduled_configs, validate_all_configs
from metadata_etl_api.catalog import DatasetCatalog
from metadata_etl_api.main import create_app
from metadata_etl_api.onboarding_operations import OnboardingRepository
from metadata_etl_api.onboarding_service import OnboardingService
from metadata_etl_api.settings import APISettings

ROOT = Path(__file__).parents[1]
CSV = (
    b"Student ID,Enrolled On,Birth Date,Advisor\n"
    b"00123,2026-09-01,01/02/2001,a@example.test\n"
    b"00451,2026-09-02,05/07/1999,NULL\n"
    b"00982,2026-09-03,03/04/2000,\n"
)


class MemoryOnboardingRepository:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}

    def ensure_schema(self) -> None:
        return None

    def create(self, values: dict[str, Any]) -> dict[str, Any]:
        record = {
            **values,
            "status": "UPLOADED",
            "profiled_at": None,
            "updated_at": values["uploaded_at"],
            "draft_key": None,
            "profile_result": None,
            "key_decisions": {},
            "safe_error": None,
        }
        self.records[record["onboarding_id"]] = record
        return deepcopy(record)

    def get(self, onboarding_id: str) -> dict[str, Any] | None:
        value = self.records.get(onboarding_id)
        return deepcopy(value) if value else None

    def find_by_dataset(self, dataset: str) -> dict[str, Any] | None:
        return next(
            (
                deepcopy(record)
                for record in self.records.values()
                if record["proposed_dataset_name"] == dataset
            ),
            None,
        )

    def begin_profile(self, onboarding_id: str, updated_at: datetime) -> dict[str, Any]:
        self.records[onboarding_id].update(status="PROFILING", updated_at=updated_at)
        return deepcopy(self.records[onboarding_id])

    def set_profile(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        profiled_at = values["profiled_at"]
        self.records[onboarding_id].update(**values, updated_at=profiled_at)
        return deepcopy(self.records[onboarding_id])

    def set_review(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        self.records[onboarding_id].update(**values)
        return deepcopy(self.records[onboarding_id])

    def set_failed(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        self.records[onboarding_id].update(status="FAILED", **values)
        return deepcopy(self.records[onboarding_id])


def _settings(tmp_path: Path, *, enabled: bool = True, max_bytes: int = 1_000_000):
    return APISettings(
        database_dsn=None,
        allowed_origins=("http://localhost:4173",),
        operations_enabled=False,
        config_dir=ROOT / "configs",
        upload_root=tmp_path / "uploads",
        airflow_upload_root="/opt/airflow/data/uploads",
        upload_max_bytes=max_bytes,
        airflow_api_url="http://airflow.test",
        airflow_username=None,
        airflow_password=None,
        airflow_token=None,
        onboarding_enabled=enabled,
        onboarding_root=tmp_path / "onboarding",
        draft_config_dir=tmp_path / "configs" / "drafts",
    )


def _client(tmp_path: Path, *, enabled: bool = True, max_bytes: int = 1_000_000):
    settings = _settings(tmp_path, enabled=enabled, max_bytes=max_bytes)
    repository = MemoryOnboardingRepository()
    service = OnboardingService(
        settings,
        repository,  # type: ignore[arg-type]
        DatasetCatalog(settings.config_dir),
    )
    app = create_app()
    app.state.onboarding_service = service
    return TestClient(app), repository, settings


def _create(
    client: TestClient,
    *,
    dataset: str = "course_enrollments",
    source_type: str = "csv",
    filename: str = "students.csv",
    content: bytes = CSV,
):
    return client.post(
        "/api/onboarding",
        data={"dataset_name": dataset, "source_type": source_type},
        files={"file": (filename, content, "application/octet-stream")},
    )


def test_gate_defaults_to_safe_and_write_endpoint_returns_403(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path, enabled=False)

    assert client.get("/api/onboarding/capability").json()["enabled"] is False
    response = _create(client)

    assert response.status_code == 403
    assert "disabled" in response.json()["detail"].lower()


@pytest.mark.parametrize("dataset", ["Course_Enrollments", "../unsafe", "1dataset", "a-b"])
def test_dataset_name_is_conservative(tmp_path: Path, dataset: str) -> None:
    client, _, _ = _client(tmp_path)

    assert _create(client, dataset=dataset).status_code == 422


def test_existing_approved_dataset_and_duplicate_draft_are_rejected(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)

    existing = _create(client, dataset="customers")
    first = _create(client)
    duplicate = _create(client)

    assert existing.status_code == 409
    assert "Upload new data" in existing.json()["detail"]
    assert first.status_code == 201
    assert duplicate.status_code == 409


@pytest.mark.parametrize(
    ("source_type", "filename", "content"),
    [
        ("csv", "source.csv", CSV),
        ("json", "source.json", b'[{"id":"001","value":1}]'),
        ("parquet", "source.parquet", b"PAR1placeholder"),
    ],
)
def test_file_source_types_are_accepted_at_upload(
    tmp_path: Path, source_type: str, filename: str, content: bytes
) -> None:
    client, repository, _ = _client(tmp_path)
    response = _create(
        client,
        dataset=f"new_{source_type}",
        source_type=source_type,
        filename=filename,
        content=content,
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["source_type"] == source_type
    assert "landing_key" not in payload
    assert str(tmp_path) not in response.text
    assert repository.records[payload["onboarding_id"]]["landing_key"].endswith(
        Path(filename).suffix
    )


def test_oversized_upload_is_rejected_and_cleaned(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path, max_bytes=3)

    response = _create(client, content=b"too large")

    assert response.status_code == 413
    assert repository.records == {}
    assert not list(settings.onboarding_root.rglob("*.part"))


def test_client_filename_cannot_control_the_landing_path(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)

    response = _create(client, filename="../../outside.csv")

    assert response.status_code == 201
    payload = response.json()
    assert payload["original_filename"] == "outside.csv"
    internal = repository.records[payload["onboarding_id"]]["landing_key"]
    assert ".." not in internal
    assert (settings.onboarding_root / internal).is_file()
    assert not (tmp_path / "outside.csv").exists()


def test_real_profiler_generates_unapproved_non_runnable_draft(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    created = _create(client).json()

    response = client.post(f"/api/onboarding/{created['onboarding_id']}/profile")

    assert response.status_code == 200
    assert response.json()["status"] == "NEEDS_REVIEW"
    record = repository.records[created["onboarding_id"]]
    draft_path = settings.draft_config_dir / record["draft_key"]
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert draft["review"]["required"] is True
    assert draft["review"]["approved"] is False
    assert draft["columns"]["student_id"]["type"] == "string"
    assert draft["columns"]["student_id"]["inference"]["reason"] == "leading_zeros"
    assert draft["columns"]["enrolled_on"]["format"] == "%Y-%m-%d"
    assert draft["columns"]["birth_date"]["format"] is None
    assert draft["columns"]["birth_date"]["review"]["approved"] is False
    assert draft["columns"]["student_id"]["profile"]["possible_key_candidate"] is True
    with pytest.raises(ConfigError, match="requires explicit approval"):
        load_config(draft_path, require_source=False)
    shutil.copy2(ROOT / "configs" / "customers.yaml", settings.draft_config_dir.parent)
    validated = validate_all_configs(settings.draft_config_dir.parent)
    scheduled = discover_scheduled_configs(settings.draft_config_dir.parent)
    assert [item.dataset for item in validated] == ["customers"]
    assert [item.dataset for item in scheduled] == ["customers"]


def test_review_edit_persists_authoritative_yaml_and_preserves_unrelated_content(
    tmp_path: Path,
) -> None:
    client, repository, settings = _client(tmp_path)
    created = _create(client).json()
    client.post(f"/api/onboarding/{created['onboarding_id']}/profile")
    onboarding_id = created["onboarding_id"]
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    before = yaml.safe_load(draft_path.read_text(encoding="utf-8"))

    response = client.patch(
        f"/api/onboarding/{onboarding_id}/schema/birth_date",
        json={
            "canonical_name": "date_of_birth",
            "datatype": "date",
            "nullable": False,
            "format": "%d/%m/%Y",
        },
    )

    assert response.status_code == 200
    after = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert "birth_date" not in after["columns"]
    assert after["columns"]["date_of_birth"]["format"] == "%d/%m/%Y"
    assert after["columns"]["date_of_birth"]["review"]["approved"] is True
    assert after["onboarding"] == before["onboarding"]
    assert after["review"]["approved"] is False
    assert response.json()["final_approved"] is False
    yaml_response = client.get(f"/api/onboarding/{onboarding_id}/yaml")
    assert yaml_response.json()["yaml"] == draft_path.read_text(encoding="utf-8")
    assert str(draft_path) not in yaml_response.text


def test_invalid_schema_edits_do_not_change_draft(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    client.post(f"/api/onboarding/{onboarding_id}/profile")
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    before = draft_path.read_bytes()

    invalid_type = client.patch(
        f"/api/onboarding/{onboarding_id}/schema/birth_date",
        json={
            "canonical_name": "birth_date",
            "datatype": "money",
            "nullable": False,
            "format": None,
        },
    )
    invalid_format = client.patch(
        f"/api/onboarding/{onboarding_id}/schema/birth_date",
        json={
            "canonical_name": "birth_date",
            "datatype": "date",
            "nullable": False,
            "format": "%Y-%m-%d",
        },
    )

    assert invalid_type.status_code == 422
    assert invalid_format.status_code == 422
    assert draft_path.read_bytes() == before


def test_key_candidate_decision_persists_separately_from_load_semantics(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    client.post(f"/api/onboarding/{onboarding_id}/profile")

    response = client.post(
        f"/api/onboarding/{onboarding_id}/key-decisions",
        json={"field": "student_id", "decision": "accepted"},
    )

    assert response.status_code == 200
    assert repository.records[onboarding_id]["key_decisions"]["student_id"] == "accepted"
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert draft["load"]["strategy"] == "full"
    assert "keys" not in draft["load"]


def test_profile_failure_is_safe_and_never_creates_a_draft(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    created = _create(client, content=b"not,a,valid,row\n1,2\n").json()

    response = client.post(f"/api/onboarding/{created['onboarding_id']}/profile")

    assert response.status_code == 422
    assert repository.records[created["onboarding_id"]]["status"] == "FAILED"
    assert not list(settings.draft_config_dir.glob("*.yaml"))
    assert str(tmp_path) not in response.text


def test_parquet_onboarding_profiles_through_existing_inference(tmp_path: Path) -> None:
    parquet = tmp_path / "fixture.parquet"
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            "COPY (SELECT * FROM (VALUES ('001', DATE '2026-01-01'), "
            "('002', DATE '2026-01-02')) AS t(device_id, observed_on)) "
            f"TO '{parquet.as_posix()}' (FORMAT PARQUET)"
        )
    client, _, _ = _client(tmp_path)
    created = _create(
        client,
        dataset="new_parquet",
        source_type="parquet",
        filename="fixture.parquet",
        content=parquet.read_bytes(),
    ).json()

    profiled = client.post(f"/api/onboarding/{created['onboarding_id']}/profile")
    review = client.get(f"/api/onboarding/{created['onboarding_id']}/review").json()

    assert profiled.status_code == 200
    assert review["column_count"] == 2
    assert (
        next(item for item in review["fields"] if item["canonical_name"] == "device_id")["reason"]
        == "leading_zeros"
    )


def test_nested_json_is_described_but_normalization_remains_unresolved(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)
    payload = b'[{"order_id":"001","items":[{"sku":"A"}]}]'
    created = _create(
        client,
        dataset="nested_new_orders",
        source_type="json",
        filename="orders.json",
        content=payload,
    ).json()

    client.post(f"/api/onboarding/{created['onboarding_id']}/profile")
    review = client.get(f"/api/onboarding/{created['onboarding_id']}/review").json()
    nested = next(item for item in review["fields"] if item["canonical_name"] == "items")

    assert nested["editable"] is False
    assert nested["reason"] == "nested_structure_requires_explicit_normalization"
    assert nested["profile"]["observed_examples"] == ["array (1 item; object elements)"]
    assert "sku" not in str(nested["profile"]["observed_examples"])
    assert review["final_approved"] is False


@pytest.mark.skipif(
    not os.getenv("ETL_TEST_POSTGRES_DSN"),
    reason="ETL_TEST_POSTGRES_DSN is required for PostgreSQL integration verification",
)
def test_onboarding_state_is_persisted_separately_from_etl_truth(tmp_path: Path) -> None:
    dsn = os.environ["ETL_TEST_POSTGRES_DSN"]
    dataset = f"new_dataset_{uuid.uuid4().hex[:10]}"
    settings = APISettings(
        **{
            **_settings(tmp_path).__dict__,
            "database_dsn": dsn,
            "onboarding_enabled": True,
        }
    )
    repository = OnboardingRepository(dsn)
    service = OnboardingService(settings, repository, DatasetCatalog(settings.config_dir))
    app = create_app()
    app.state.onboarding_service = service
    client = TestClient(app)

    created = _create(client, dataset=dataset).json()
    profiled = client.post(f"/api/onboarding/{created['onboarding_id']}/profile")

    assert profiled.status_code == 200
    with psycopg.connect(dsn) as connection, connection.transaction():
        persisted = connection.execute(
            "SELECT status, draft_key IS NOT NULL FROM etl_app.onboarding_sessions "
            "WHERE onboarding_id = %s",
            (created["onboarding_id"],),
        ).fetchone()
        etl_runs = connection.execute(
            "SELECT count(*) FROM etl_meta.etl_run_ledger WHERE dataset = %s", (dataset,)
        ).fetchone()[0]
        upload_sessions = connection.execute(
            "SELECT count(*) FROM etl_app.upload_sessions WHERE dataset = %s", (dataset,)
        ).fetchone()[0]
        connection.execute(
            "DELETE FROM etl_app.onboarding_sessions WHERE onboarding_id = %s",
            (created["onboarding_id"],),
        )
    assert persisted == ("NEEDS_REVIEW", True)
    assert etl_runs == 0
    assert upload_sessions == 0
