from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Self

from metadata_etl_api.airflow_client import AirflowClient
from metadata_etl_api.settings import APISettings


class Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()


def test_airflow_3_client_uses_token_and_stable_v2_dag_run_api(monkeypatch: Any) -> None:
    requests: list[Any] = []

    def fake_urlopen(request: Any, timeout: float) -> Response:
        assert timeout == 10
        requests.append(request)
        if request.full_url.endswith("/auth/token"):
            return Response({"access_token": "server-token"})
        return Response({"dag_run_id": "upload__123", "state": "queued"})

    monkeypatch.setattr("metadata_etl_api.airflow_client.urlopen", fake_urlopen)
    client = AirflowClient("http://airflow", username="admin", password="secret")

    response = client.trigger(
        "etl_customers",
        "upload__123",
        source_override="/opt/airflow/data/uploads/customers/123/artifact.csv",
        correlation_id="123",
        git_commit_sha="a" * 40,
    )

    assert response["state"] == "queued"
    assert requests[0].full_url == "http://airflow/auth/token"
    assert requests[1].full_url == "http://airflow/api/v2/dags/etl_customers/dagRuns"
    assert requests[1].headers["Authorization"] == "Bearer server-token"
    payload = json.loads(requests[1].data)
    assert payload["logical_date"] is None
    assert payload["conf"]["correlation_id"] == "123"
    assert payload["conf"]["git_commit_sha"] == "a" * 40
    assert "secret" not in json.dumps(payload)


def test_airflow_client_reads_generated_password_file_after_startup(
    monkeypatch: Any, tmp_path: Path
) -> None:
    password_file = tmp_path / "simple_auth_manager_passwords.json.generated"
    requests: list[Any] = []
    client = AirflowClient(
        "http://airflow",
        username="admin",
        password_file=password_file,
    )
    password_file.write_text(json.dumps({"admin": "generated-secret"}), encoding="utf-8")

    def fake_urlopen(request: Any, timeout: float) -> Response:
        assert timeout == 10
        requests.append(request)
        if request.full_url.endswith("/auth/token"):
            credentials = json.loads(request.data)
            assert credentials == {"username": "admin", "password": "generated-secret"}
            return Response({"access_token": "server-token"})
        return Response({"dag_id": "etl_new_domain"})

    monkeypatch.setattr("metadata_etl_api.airflow_client.urlopen", fake_urlopen)

    response = client.dag("etl_new_domain")

    assert response == {"dag_id": "etl_new_domain"}
    assert requests[1].headers["Authorization"] == "Bearer server-token"


def test_default_discovery_timeout_covers_airflow_reparse_interval(monkeypatch: Any) -> None:
    monkeypatch.delenv("ETL_AIRFLOW_DISCOVERY_TIMEOUT_SECONDS", raising=False)

    settings = APISettings.from_environment()

    assert settings.airflow_discovery_timeout_seconds >= 60
