from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import uuid
from copy import deepcopy
from datetime import UTC, datetime
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
from metadata_etl_api.git_adapter import GitConfigAdapter, GitPromotionError
from metadata_etl_api.main import create_app
from metadata_etl_api.onboarding_operations import OnboardingRepository, OnboardingRepositoryError
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
            "validation_hash": None,
            "validation_errors": None,
            "validated_at": None,
            "approved_at": None,
            "approved_by": None,
            "approved_config_hash": None,
            "approved_config_key": None,
            "source_key": None,
            "git_commit_sha": None,
            "git_push_status": None,
            "dag_id": None,
            "activation_checked_at": None,
            "first_run_attempt": 0,
            "first_run_correlation_id": None,
            "airflow_run_id": None,
            "airflow_state": None,
            "etl_run_id": None,
            "completed_at": None,
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
        self.records[onboarding_id].update(
            **values,
            validation_hash=None,
            validation_errors=None,
            validated_at=None,
        )
        return deepcopy(self.records[onboarding_id])

    def set_configuration(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        self.records[onboarding_id].update(
            **values,
            validation_hash=None,
            validation_errors=None,
            validated_at=None,
        )
        return deepcopy(self.records[onboarding_id])

    def set_validation(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        self.records[onboarding_id].update(**values)
        return deepcopy(self.records[onboarding_id])

    def set_failed(self, onboarding_id: str, **values: Any) -> dict[str, Any]:
        self.records[onboarding_id].update(status="FAILED", **values)
        return deepcopy(self.records[onboarding_id])

    def claim_approval(self, onboarding_id: str, **values: Any):
        record = self.records[onboarding_id]
        if record["approved_at"] is not None or record["status"] == "APPROVING":
            return deepcopy(record), False
        if record["status"] != "READY_FOR_APPROVAL":
            raise OnboardingRepositoryError("Configuration is not ready for approval")
        record.update(status="APPROVING", approved_by=values["approved_by"])
        return deepcopy(record), True

    def finish_approval(self, onboarding_id: str, **values: Any):
        self.records[onboarding_id].update(
            status="APPROVED", **values, updated_at=values["approved_at"]
        )
        return deepcopy(self.records[onboarding_id])

    def fail_approval(self, onboarding_id: str, **values: Any):
        self.records[onboarding_id].update(status="READY_FOR_APPROVAL", **values)
        return deepcopy(self.records[onboarding_id])

    def invalidate_approval(self, onboarding_id: str, **values: Any):
        self.records[onboarding_id].update(
            status="CONFIGURING",
            validation_hash=None,
            validation_errors=None,
            validated_at=None,
            safe_error="Draft changed after validation; validate the current version again.",
            **values,
        )
        return deepcopy(self.records[onboarding_id])

    def begin_activation(self, onboarding_id: str, updated_at: datetime):
        self.records[onboarding_id].update(status="ACTIVATING", updated_at=updated_at)
        return deepcopy(self.records[onboarding_id])

    def finish_activation(self, onboarding_id: str, *, discovered: bool, updated_at: datetime):
        self.records[onboarding_id].update(
            status="READY_FOR_FIRST_RUN" if discovered else "ACTIVATION_FAILED",
            activation_checked_at=updated_at,
            updated_at=updated_at,
            safe_error=None
            if discovered
            else "Airflow has not discovered the approved dataset DAG yet.",
        )
        return deepcopy(self.records[onboarding_id])

    def claim_first_run(self, onboarding_id: str, **values: Any):
        record = self.records[onboarding_id]
        if record["status"] in {"TRIGGERING", "QUEUED", "RUNNING", "SUCCEEDED"}:
            return deepcopy(record), False
        if record["status"] != "READY_FOR_FIRST_RUN" and not (
            values["retry"] and record["status"] == "FIRST_RUN_FAILED"
        ):
            raise OnboardingRepositoryError("Dataset is not ready for its first run")
        record.update(
            status="TRIGGERING",
            first_run_attempt=record["first_run_attempt"] + 1,
            first_run_correlation_id=values["correlation_id"],
            airflow_run_id=values["airflow_run_id"],
            airflow_state=None,
            etl_run_id=None,
            completed_at=None,
            safe_error=None,
        )
        return deepcopy(record), True

    def mark_first_run_triggered(self, onboarding_id: str, **values: Any):
        self.records[onboarding_id].update(status="QUEUED", **values)
        return deepcopy(self.records[onboarding_id])

    def sync_first_run(self, onboarding_id: str, **values: Any):
        self.records[onboarding_id].update(**values)
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


def _profile_and_complete_review(client: TestClient, onboarding_id: str) -> dict[str, Any]:
    assert client.post(f"/api/onboarding/{onboarding_id}/profile").status_code == 200
    review = client.get(f"/api/onboarding/{onboarding_id}/review").json()
    for item in review["fields"]:
        if item["review_required"] and not item["review_resolved"]:
            date_format = (
                "%d/%m/%Y" if item["reason"] == "ambiguous_date_format" else item["format"]
            )
            response = client.patch(
                f"/api/onboarding/{onboarding_id}/schema/{item['canonical_name']}",
                json={
                    "canonical_name": item["canonical_name"],
                    "datatype": item["datatype"],
                    "nullable": item["nullable"],
                    "format": date_format,
                },
            )
            assert response.status_code == 200, response.text
    review = client.get(f"/api/onboarding/{onboarding_id}/review").json()
    for item in review["fields"]:
        if item["key_candidate"] and item["key_decision"] is None:
            response = client.post(
                f"/api/onboarding/{onboarding_id}/key-decisions",
                json={"field": item["canonical_name"], "decision": "rejected"},
            )
            assert response.status_code == 200, response.text
    return client.get(f"/api/onboarding/{onboarding_id}/configuration").json()


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


def test_configuration_operators_persist_validate_and_reorder(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    configuration = _profile_and_complete_review(client, onboarding_id)
    assert configuration["review_complete"] is True

    operators = [
        {"id": "T001", "type": "cast", "column": "student_id", "datatype": "string"},
        {"id": "T002", "type": "filter", "condition": "student_id <> ''"},
        {
            "id": "T003",
            "type": "derive",
            "target_column": "profile_score",
            "datatype": "integer",
            "expression": "1",
        },
        {
            "id": "T004",
            "type": "map",
            "column": "advisor",
            "datatype": "string",
            "mappings": {"a@example.test": "assigned"},
            "default": "unassigned",
        },
        {
            "id": "T005",
            "type": "deduplicate",
            "keys": ["student_id"],
            "order_by": {"enrolled_on": "desc"},
        },
    ]
    for operator in operators:
        response = client.post(f"/api/onboarding/{onboarding_id}/transformations", json=operator)
        assert response.status_code == 200, response.text

    edited = client.patch(
        f"/api/onboarding/{onboarding_id}/transformations/T002",
        json={"id": "T002", "type": "filter", "condition": "student_id IS NOT NULL"},
    )
    moved = client.post(
        f"/api/onboarding/{onboarding_id}/transformations/T005/move",
        json={"direction": "up"},
    )
    removed = client.delete(f"/api/onboarding/{onboarding_id}/transformations/T001")

    assert edited.status_code == moved.status_code == removed.status_code == 200
    assert [item["id"] for item in moved.json()["transformations"]][-2:] == ["T005", "T004"]
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert [item["id"] for item in draft["transformations"]] == ["T002", "T003", "T005", "T004"]
    assert draft["review"]["approved"] is False


def test_invalid_transformation_is_rejected_without_changing_yaml(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    before = draft_path.read_bytes()

    unsupported = client.post(
        f"/api/onboarding/{onboarding_id}/transformations",
        json={"id": "T001", "type": "python", "expression": "danger()"},
    )
    missing = client.post(
        f"/api/onboarding/{onboarding_id}/transformations",
        json={"id": "T001", "type": "cast", "column": "missing", "datatype": "string"},
    )
    duplicate_map = client.post(
        f"/api/onboarding/{onboarding_id}/transformations",
        json={
            "id": "T001",
            "type": "map",
            "column": "advisor",
            "mappings": [
                {"source": "A", "result": "one"},
                {"source": "A", "result": "two"},
            ],
        },
    )

    assert unsupported.status_code == 422
    assert missing.status_code == 422
    assert duplicate_map.status_code == 422
    assert draft_path.read_bytes() == before


def test_contracts_use_post_transformation_schema_and_privacy_persists(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    derived = client.post(
        f"/api/onboarding/{onboarding_id}/transformations",
        json={
            "id": "T001",
            "type": "derive",
            "target_column": "profile_score",
            "datatype": "integer",
            "expression": "1",
        },
    )
    assert derived.status_code == 200

    contracts = [
        {"id": "DQ001", "type": "not_null", "column": "student_id"},
        {"id": "DQ002", "type": "unique", "columns": ["student_id"]},
        {"id": "DQ003", "type": "range", "column": "profile_score", "min": 0, "max": 4},
        {"id": "DQ004", "type": "regex", "column": "student_id", "pattern": r"^\d{5}$"},
    ]
    for contract in contracts:
        response = client.post(f"/api/onboarding/{onboarding_id}/contracts", json=contract)
        assert response.status_code == 200, response.text
    privacy = client.patch(
        f"/api/onboarding/{onboarding_id}/columns/student_id/privacy",
        json={"classification": "restricted", "quarantine_value": "hashed"},
    )
    assert privacy.status_code == 200
    assert (
        next(item for item in privacy.json()["columns"] if item["name"] == "student_id")[
            "quarantine_value"
        ]
        == "hashed"
    )
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert draft["columns"]["student_id"]["classification"] == "restricted"
    assert draft["columns"]["student_id"]["quarantine"] == {"value": "hashed"}


@pytest.mark.parametrize(
    "contract",
    [
        {"id": "DQ001", "type": "regex", "column": "student_id", "pattern": "["},
        {"id": "DQ001", "type": "range", "column": "profile_score", "min": 5, "max": 1},
        {"id": "DQ001", "type": "not_null", "column": "missing"},
        {"id": "DQ001", "type": "unique", "columns": []},
    ],
)
def test_invalid_contracts_are_rejected(tmp_path: Path, contract: dict[str, Any]) -> None:
    client, _, _ = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    if contract["type"] == "range":
        response = client.post(
            f"/api/onboarding/{onboarding_id}/transformations",
            json={
                "id": "T001",
                "type": "derive",
                "target_column": "profile_score",
                "datatype": "integer",
                "expression": "1",
            },
        )
        assert response.status_code == 200
    response = client.post(f"/api/onboarding/{onboarding_id}/contracts", json=contract)
    assert response.status_code == 422


def test_duplicate_rule_ids_are_rejected(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    rule = {"id": "DQ001", "type": "not_null", "column": "student_id"}
    assert client.post(f"/api/onboarding/{onboarding_id}/contracts", json=rule).status_code == 200
    assert client.post(f"/api/onboarding/{onboarding_id}/contracts", json=rule).status_code == 422


def test_load_strategies_validate_and_clear_incompatible_settings(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    routes = f"/api/onboarding/{onboarding_id}/load"

    incremental = client.patch(
        routes,
        json={
            "strategy": "incremental",
            "watermark": {"column": "enrolled_on", "type": "date", "initial_value": "2026-01-01"},
        },
    )
    assert incremental.status_code == 200, incremental.text
    assert client.patch(routes, json={"strategy": "incremental"}).status_code == 422
    upsert = client.patch(routes, json={"strategy": "upsert", "keys": ["student_id"]})
    assert upsert.status_code == 200
    assert "watermark" not in upsert.json()["load"]
    assert client.patch(routes, json={"strategy": "upsert", "keys": []}).status_code == 422
    assert (
        client.patch(
            routes, json={"strategy": "upsert", "keys": ["student_id", "student_id"]}
        ).status_code
        == 422
    )
    scd2 = client.patch(
        routes,
        json={
            "strategy": "scd2",
            "keys": ["student_id"],
            "tracked_columns": ["advisor"],
            "effective_timestamp": {"column": "enrolled_on"},
            "history_columns": {
                "valid_from": "valid_from",
                "valid_to": "valid_to",
                "is_current": "is_current",
            },
        },
    )
    assert scd2.status_code == 200, scd2.text
    assert client.patch(routes, json={"strategy": "scd2", "keys": []}).status_code == 422
    full = client.patch(routes, json={"strategy": "full"})
    assert full.status_code == 200
    assert set(full.json()["load"]) == {
        "strategy",
        "connection_env",
        "schema",
        "staging_table",
        "target_table",
    }


def test_drift_actions_validate(tmp_path: Path) -> None:
    client, _, _ = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    route = f"/api/onboarding/{onboarding_id}/schema-drift"
    response = client.patch(
        route,
        json={"added_columns": "allow", "removed_columns": "warn", "datatype_change": "fail"},
    )
    assert response.status_code == 200
    assert response.json()["schema_drift"]["added_columns"] == "allow"
    assert client.patch(route, json={"added_columns": "review"}).status_code == 422
    assert client.patch(route, json={"renamed_columns": "warn"}).status_code == 422


def test_validation_is_hash_bound_unapproved_and_invalidated_by_edit(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    onboarding_id = _create(client).json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    validated = client.post(f"/api/onboarding/{onboarding_id}/validate")
    assert validated.status_code == 200
    body = validated.json()
    assert body["status"] == "READY_FOR_APPROVAL"
    assert body["validation"]["result"] == "VALID"
    assert body["validation"]["validated_hash"] == body["validation"]["draft_hash"]
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    with pytest.raises(ConfigError, match="requires explicit approval"):
        load_config(draft_path, require_source=False)
    assert yaml.safe_load(draft_path.read_text(encoding="utf-8"))["review"]["approved"] is False

    edited = client.patch(
        f"/api/onboarding/{onboarding_id}/schema-drift", json={"added_columns": "allow"}
    )
    assert edited.status_code == 200
    assert edited.json()["status"] == "CONFIGURING"
    assert edited.json()["validation"]["result"] == "NOT_VALIDATED"
    assert edited.json()["validation"]["validated_hash"] is None


def test_nested_json_normalization_is_explicit_and_runtime_validated(tmp_path: Path) -> None:
    client, repository, settings = _client(tmp_path)
    payload = b'{"orders":[{"order":{"id":"001","items":[{"sku":"A","quantity":2}]}}]}'
    onboarding_id = _create(
        client,
        dataset="nested_configuration",
        source_type="json",
        filename="orders.json",
        content=payload,
    ).json()["onboarding_id"]
    client.post(f"/api/onboarding/{onboarding_id}/profile")
    before = client.post(f"/api/onboarding/{onboarding_id}/validate").json()
    assert before["validation"]["result"] == "INVALID"
    assert any(item["section"] == "normalization" for item in before["validation"]["errors"])

    invalid = client.patch(
        f"/api/onboarding/{onboarding_id}/normalization",
        json={
            "root_path": "orders",
            "fields": {"order_id": "order.missing"},
            "explode": {"path": "order.items", "as": "item", "fields": {"sku": "item.sku"}},
            "columns": {"order_id": {"type": "string"}, "sku": {"type": "string"}},
        },
    )
    assert invalid.status_code == 422
    duplicate = client.patch(
        f"/api/onboarding/{onboarding_id}/normalization",
        json={
            "root_path": "orders",
            "fields": {"order_id": "order.id"},
            "explode": {
                "path": "order.items",
                "as": "item",
                "fields": {"order_id": "item.sku"},
            },
            "columns": {"order_id": {"type": "string"}},
        },
    )
    assert duplicate.status_code == 422
    valid = client.patch(
        f"/api/onboarding/{onboarding_id}/normalization",
        json={
            "root_path": "orders",
            "fields": {"order_id": "order.id"},
            "explode": {
                "path": "order.items",
                "as": "item",
                "fields": {"sku": "item.sku", "quantity": "item.quantity"},
            },
            "columns": {
                "order_id": {"type": "string", "nullable": False},
                "sku": {"type": "string", "nullable": False},
                "quantity": {"type": "integer", "nullable": False},
            },
        },
    )
    assert valid.status_code == 200, valid.text
    assert valid.json()["normalization_complete"] is True
    assert valid.json()["review_complete"] is True
    validated = client.post(f"/api/onboarding/{onboarding_id}/validate")
    assert validated.json()["validation"]["result"] == "VALID"
    draft_path = settings.draft_config_dir / repository.records[onboarding_id]["draft_key"]
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert draft["normalization"]["json"]["root_path"] == "orders"
    assert draft["normalization"]["json"]["explode"]["path"] == "order.items"
    assert draft["review"]["approved"] is False


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


class FakeOnboardingAirflow:
    def __init__(self, *, discovered: bool = True, discovery_delay_checks: int = 0) -> None:
        self.discovered = discovered
        self.discovery_delay_checks = discovery_delay_checks
        self.dag_checks = 0
        self.state = "QUEUED"
        self.triggers: list[dict[str, str]] = []

    def dag(self, dag_id: str) -> dict[str, object]:
        self.dag_checks += 1
        if not self.discovered or self.dag_checks <= self.discovery_delay_checks:
            from metadata_etl_api.airflow_client import AirflowError

            raise AirflowError("not discovered")
        return {"dag_id": dag_id}

    def trigger(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        source_override: str,
        correlation_id: str,
        git_commit_sha: str | None = None,
    ) -> dict[str, object]:
        self.triggers.append(
            {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "source_override": source_override,
                "correlation_id": correlation_id,
                "git_commit_sha": git_commit_sha or "",
            }
        )
        return {"state": self.state}

    def run(self, _dag_id: str, _dag_run_id: str) -> dict[str, object]:
        return {"state": self.state}


class FakeOnboardingObservability:
    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.lookups: list[str] = []

    def run_by_correlation(self, correlation_id: str) -> dict[str, Any] | None:
        self.lookups.append(correlation_id)
        return deepcopy(self.runs.get(correlation_id))


def _git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _approval_client(
    tmp_path: Path,
    *,
    discovered: bool = True,
    discovery_delay_checks: int = 0,
    discovery_timeout_seconds: float = 0,
):
    repo_root = tmp_path / "approval-repo"
    (repo_root / "configs" / "drafts").mkdir(parents=True)
    (repo_root / "data" / "onboarding").mkdir(parents=True)
    (repo_root / "data" / "sources").mkdir(parents=True)
    (repo_root / ".gitignore").write_text(
        "data/onboarding/\ndata/sources/\nconfigs/drafts/\n", encoding="utf-8"
    )
    _git(repo_root, "init")
    _git(repo_root, "branch", "-M", "main")
    _git(repo_root, "add", ".gitignore")
    _git(
        repo_root,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.test",
        "commit",
        "-m",
        "Initial",
    )
    settings = APISettings(
        **{
            **_settings(tmp_path).__dict__,
            "config_dir": repo_root / "configs",
            "onboarding_root": repo_root / "data" / "onboarding",
            "draft_config_dir": repo_root / "configs" / "drafts",
            "repository_root": repo_root,
            "approved_source_root": repo_root / "data" / "sources",
            "approved_source_config_root": "data/sources",
            "airflow_source_root": "/opt/airflow/data/sources",
            "airflow_discovery_timeout_seconds": discovery_timeout_seconds,
            "airflow_discovery_poll_seconds": 0.001,
        }
    )
    repository = MemoryOnboardingRepository()
    airflow = FakeOnboardingAirflow(
        discovered=discovered,
        discovery_delay_checks=discovery_delay_checks,
    )
    observability = FakeOnboardingObservability()
    service = OnboardingService(
        settings,
        repository,  # type: ignore[arg-type]
        DatasetCatalog(settings.config_dir),
        airflow=airflow,  # type: ignore[arg-type]
        observability=observability,  # type: ignore[arg-type]
        git=GitConfigAdapter(repo_root),
    )
    app = create_app()
    app.state.onboarding_service = service
    return TestClient(app), repository, settings, airflow, observability, repo_root


def _ready_for_approval(client: TestClient) -> tuple[str, dict[str, Any]]:
    onboarding_id = _create(client, dataset="first_time_students").json()["onboarding_id"]
    _profile_and_complete_review(client, onboarding_id)
    validated = client.post(f"/api/onboarding/{onboarding_id}/validate").json()
    assert validated["status"] == "READY_FOR_APPROVAL"
    return onboarding_id, validated


def test_exact_version_approval_promotes_only_config_and_does_not_run(tmp_path: Path) -> None:
    client, repository, settings, airflow, _, repo_root = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    route = f"/api/onboarding/{onboarding_id}/approve"

    missing_ack = client.post(
        route,
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya A",
            "acknowledged": False,
        },
    )
    approved = client.post(
        route,
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya A",
            "acknowledged": True,
        },
    )

    assert missing_ack.status_code == 422
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "READY_FOR_FIRST_RUN"
    config_path = settings.config_dir / "first_time_students.yaml"
    source_path = settings.approved_source_root / "first_time_students" / "source.csv"
    config = load_config(config_path, require_source=False)
    assert config.raw["review"] == {
        "required": True,
        "approved": True,
        "approved_by": "Priya A",
    }
    assert config.raw["source"]["path"] == "data/sources/first_time_students/source.csv"
    assert source_path.read_bytes() == CSV
    assert airflow.triggers == []
    assert _git(repo_root, "show", "--pretty=format:", "--name-only", "HEAD") == (
        "configs/first_time_students.yaml"
    )
    assert _git(repo_root, "log", "-1", "--pretty=%s") == (
        "Approve dataset configuration: first_time_students"
    )
    assert _git(repo_root, "status", "--porcelain") == ""
    assert (
        repository.records[onboarding_id]["validation_hash"]
        == validated["validation"]["validated_hash"]
    )
    assert (
        repository.records[onboarding_id]["approved_config_hash"]
        != repository.records[onboarding_id]["validation_hash"]
    )
    assert not (settings.draft_config_dir / f"{onboarding_id}.yaml").exists()
    assert (settings.draft_config_dir / "archive" / f"{onboarding_id}.yaml").exists()

    duplicate = client.post(
        route,
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya A",
            "acknowledged": True,
        },
    )
    assert duplicate.status_code == 200
    assert _git(repo_root, "rev-list", "--count", "HEAD") == "2"
    blocked_edit = client.patch(
        f"/api/onboarding/{onboarding_id}/schema-drift", json={"added_columns": "allow"}
    )
    assert blocked_edit.status_code == 409


def test_approval_rejects_stale_hash_without_promotion(tmp_path: Path) -> None:
    client, repository, settings, _, _, _ = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    draft_path = settings.draft_config_dir / f"{onboarding_id}.yaml"
    draft_path.write_bytes(draft_path.read_bytes() + b"\n")

    response = client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )

    assert response.status_code == 409
    assert repository.records[onboarding_id]["status"] == "CONFIGURING"
    assert repository.records[onboarding_id]["validation_hash"] is None
    assert not (settings.config_dir / "first_time_students.yaml").exists()
    assert not (settings.approved_source_root / "first_time_students").exists()


def test_approval_rejects_legacy_draft_until_activation_shape_is_revalidated(
    tmp_path: Path,
) -> None:
    client, repository, settings, _, _, _ = _approval_client(tmp_path)
    onboarding_id, _ = _ready_for_approval(client)
    draft_path = settings.draft_config_dir / f"{onboarding_id}.yaml"
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    draft["source"]["path"] = f"data/onboarding/{onboarding_id}/students.csv"
    draft.pop("orchestration")
    draft_path.write_text(yaml.safe_dump(draft, sort_keys=False), encoding="utf-8")
    legacy_hash = hashlib.sha256(draft_path.read_bytes()).hexdigest()
    repository.records[onboarding_id].update(
        status="READY_FOR_APPROVAL", validation_hash=legacy_hash
    )

    configuration = client.get(f"/api/onboarding/{onboarding_id}/configuration").json()
    response = client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": legacy_hash,
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )

    assert configuration["activation_ready"] is False
    assert response.status_code == 409
    assert repository.records[onboarding_id]["status"] == "CONFIGURING"
    assert not (settings.config_dir / "first_time_students.yaml").exists()


def test_interrupted_approval_resumes_without_duplicate_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, repository, _, _, _, repo_root = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    original_finish = repository.finish_approval
    interrupted = True

    def interrupt_once(onboarding_id: str, **values: Any):
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise OnboardingRepositoryError("simulated process interruption")
        return original_finish(onboarding_id, **values)

    monkeypatch.setattr(repository, "finish_approval", interrupt_once)
    payload = {
        "expected_hash": validated["validation"]["validated_hash"],
        "approved_by": "Priya",
        "acknowledged": True,
    }
    interrupted_response = client.post(f"/api/onboarding/{onboarding_id}/approve", json=payload)
    assert interrupted_response.status_code == 503
    assert repository.records[onboarding_id]["status"] == "APPROVING"
    assert _git(repo_root, "rev-list", "--count", "HEAD") == "2"

    resumed = client.post(f"/api/onboarding/{onboarding_id}/approve", json=payload)

    assert resumed.status_code == 200
    assert resumed.json()["status"] == "READY_FOR_FIRST_RUN"
    assert _git(repo_root, "rev-list", "--count", "HEAD") == "2"
    assert repository.records[onboarding_id]["approved_at"] is not None


def test_collision_and_dirty_repository_block_without_overwrite(tmp_path: Path) -> None:
    client, _, settings, _, _, repo_root = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    collision = settings.config_dir / "first_time_students.yaml"
    collision.write_text("do-not-overwrite\n", encoding="utf-8")
    payload = {
        "expected_hash": validated["validation"]["validated_hash"],
        "approved_by": "Priya",
        "acknowledged": True,
    }
    response = client.post(f"/api/onboarding/{onboarding_id}/approve", json=payload)
    assert response.status_code == 409
    assert collision.read_text(encoding="utf-8") == "do-not-overwrite\n"
    collision.unlink()

    (repo_root / "unexpected.txt").write_text("dirty", encoding="utf-8")
    response = client.post(f"/api/onboarding/{onboarding_id}/approve", json=payload)
    assert response.status_code == 409
    assert not (settings.config_dir / "first_time_students.yaml").exists()
    assert not (settings.approved_source_root / "first_time_students").exists()


def test_activation_timeout_preserves_approval_and_can_retry(tmp_path: Path) -> None:
    client, repository, _, airflow, _, _ = _approval_client(tmp_path, discovered=False)
    onboarding_id, validated = _ready_for_approval(client)
    response = client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )
    assert response.json()["status"] == "ACTIVATION_FAILED"
    assert repository.records[onboarding_id]["approved_at"] is not None
    airflow.discovered = True
    retried = client.post(f"/api/onboarding/{onboarding_id}/activate")
    assert retried.json()["status"] == "READY_FOR_FIRST_RUN"


def test_new_approved_config_is_discovered_after_application_startup(tmp_path: Path) -> None:
    client, _, _, airflow, _, _ = _approval_client(
        tmp_path,
        discovery_delay_checks=2,
        discovery_timeout_seconds=0.2,
    )
    onboarding_id, validated = _ready_for_approval(client)

    approved = client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )

    assert approved.json()["status"] == "READY_FOR_FIRST_RUN"
    assert airflow.dag_checks == 3
    assert airflow.triggers == []


def test_git_commit_failure_rolls_back_promoted_config_and_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, repository, settings, _, _, _ = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    service = client.app.state.onboarding_service

    def fail_commit(*_args: Any, **_values: Any):
        raise GitPromotionError("simulated commit failure")

    monkeypatch.setattr(service.git, "commit_config", fail_commit)
    response = client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )

    assert response.status_code == 409
    assert repository.records[onboarding_id]["status"] == "READY_FOR_APPROVAL"
    assert not (settings.config_dir / "first_time_students.yaml").exists()
    assert not (settings.approved_source_root / "first_time_students" / "source.csv").exists()
    assert (settings.draft_config_dir / f"{onboarding_id}.yaml").exists()


def test_first_run_is_explicit_correlated_idempotent_and_retryable(tmp_path: Path) -> None:
    client, repository, _, airflow, observability, repo_root = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )
    commit_count = _git(repo_root, "rev-list", "--count", "HEAD")
    first = client.post(f"/api/onboarding/{onboarding_id}/first-run", json={"retry": False})
    duplicate = client.post(f"/api/onboarding/{onboarding_id}/first-run", json={"retry": False})
    assert first.status_code == duplicate.status_code == 200
    assert len(airflow.triggers) == 1
    correlation = first.json()["first_run"]["correlation_id"]
    assert airflow.triggers[0]["correlation_id"] == correlation
    assert airflow.triggers[0]["source_override"].endswith("/first_time_students/source.csv")
    assert _git(repo_root, "rev-list", "--count", "HEAD") == commit_count

    observability.runs[correlation] = {
        "run_id": "RUN_FIRST",
        "status": "SUCCEEDED",
        "started_at": datetime.now(UTC),
        "completed_at": datetime.now(UTC),
        "duration_seconds": 1.2,
        "rows_extracted": 3,
        "rows_transformed": 3,
        "rows_contract_passed": 3,
        "rows_quarantined": 0,
        "rows_loaded": 3,
        "drift_status": "NONE",
    }
    airflow.state = "SUCCESS"
    completed = client.get(f"/api/onboarding/{onboarding_id}/completion")
    assert completed.json()["status"] == "SUCCEEDED"
    assert completed.json()["first_run"]["etl_run_id"] == "RUN_FIRST"
    assert observability.lookups[-1] == correlation
    assert repository.records[onboarding_id]["status"] == "SUCCEEDED"


def test_failed_first_run_preserves_approval_and_retry_uses_new_exact_correlation(
    tmp_path: Path,
) -> None:
    client, repository, _, airflow, _, _ = _approval_client(tmp_path)
    onboarding_id, validated = _ready_for_approval(client)
    client.post(
        f"/api/onboarding/{onboarding_id}/approve",
        json={
            "expected_hash": validated["validation"]["validated_hash"],
            "approved_by": "Priya",
            "acknowledged": True,
        },
    )
    airflow.state = "FAILED"
    failed = client.post(f"/api/onboarding/{onboarding_id}/first-run", json={"retry": False}).json()
    first_correlation = failed["first_run"]["correlation_id"]
    assert failed["status"] == "FIRST_RUN_FAILED"
    assert repository.records[onboarding_id]["approved_at"] is not None

    airflow.state = "QUEUED"
    retried = client.post(f"/api/onboarding/{onboarding_id}/first-run", json={"retry": True}).json()
    assert retried["status"] == "QUEUED"
    assert retried["first_run"]["correlation_id"] != first_correlation
    assert retried["first_run"]["attempt"] == 2
    assert len(airflow.triggers) == 2


def test_git_adapter_rejects_non_config_paths_and_dirty_repositories(tmp_path: Path) -> None:
    repo = tmp_path / "git-adapter"
    (repo / "configs").mkdir(parents=True)
    _git(repo, "init")
    _git(repo, "branch", "-M", "main")
    (repo / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    _git(repo, "add", ".gitignore")
    _git(repo, "-c", "user.name=Test", "-c", "user.email=t@t", "commit", "-m", "Initial")
    adapter = GitConfigAdapter(repo)
    (repo / "dirty.txt").write_text("dirty", encoding="utf-8")
    with pytest.raises(GitPromotionError, match="blocked"):
        adapter.ensure_clean()
    (repo / "dirty.txt").unlink()
    adapter.ensure_clean()
    with pytest.raises(GitPromotionError, match="Only one"):
        adapter.commit_config(repo / ".gitignore", "unsafe")
