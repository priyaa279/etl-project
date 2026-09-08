from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class AirflowError(RuntimeError):
    """Safe boundary for Airflow authentication and API failures."""


@dataclass
class AirflowClient:
    base_url: str
    username: str | None = None
    password: str | None = None
    token: str | None = None
    timeout_seconds: float = 10

    def _request(
        self, method: str, path: str, payload: dict[str, object] | None = None
    ) -> dict[str, object]:
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if not path.startswith("/auth/"):
            headers["Authorization"] = f"Bearer {self._token()}"
        request = Request(
            f"{self.base_url.rstrip('/')}{path}", data=body, headers=headers, method=method
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode())
        except HTTPError as exc:
            raise AirflowError(f"Airflow API returned HTTP {exc.code}") from exc
        except (OSError, URLError, UnicodeError, json.JSONDecodeError) as exc:
            raise AirflowError("Airflow API is unavailable") from exc

    def _token(self) -> str:
        if self.token:
            return self.token
        if self.username and self.password:
            response = self._request(
                "POST", "/auth/token", {"username": self.username, "password": self.password}
            )
        else:
            raise AirflowError("Airflow API authentication is not configured")
        token = response.get("access_token")
        if not isinstance(token, str) or not token:
            raise AirflowError("Airflow API authentication failed")
        self.token = token
        return token

    def trigger(
        self,
        dag_id: str,
        dag_run_id: str,
        *,
        source_override: str,
        correlation_id: str,
        git_commit_sha: str | None = None,
    ) -> dict[str, object]:
        path = f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns"
        conf = {
            "source_override": source_override,
            "correlation_id": correlation_id,
        }
        if git_commit_sha:
            conf["git_commit_sha"] = git_commit_sha
        payload = {
            "dag_run_id": dag_run_id,
            "logical_date": None,
            "conf": conf,
        }
        try:
            return self._request("POST", path, payload)
        except AirflowError as exc:
            if "HTTP 409" not in str(exc):
                raise
            return self.run(dag_id, dag_run_id)

    def run(self, dag_id: str, dag_run_id: str) -> dict[str, object]:
        return self._request(
            "GET",
            f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(dag_run_id, safe='')}",
        )

    def dag(self, dag_id: str) -> dict[str, object]:
        """Return one exact DAG; callers decide how long discovery may take."""
        return self._request("GET", f"/api/v2/dags/{quote(dag_id, safe='')}")
