from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from metadata_etl_api.main import create_app
from metadata_etl_api.repository import ObservabilityRepository, RepositoryError


class CapturingRepository(ObservabilityRepository):
    def __init__(self) -> None:
        super().__init__("configured")
        self.query = ""
        self.parameters: tuple[Any, ...] = ()

    def _fetch_all(
        self, query: str, parameters: tuple[Any, ...] | list[Any] = ()
    ) -> list[dict[str, Any]]:
        self.query = query
        self.parameters = tuple(parameters)
        return []


@pytest.mark.parametrize(
    ("field", "direction", "expected"),
    [
        ("started_at", "desc", "started_at DESC NULLS LAST"),
        ("duration_seconds", "asc", "duration_seconds ASC NULLS LAST"),
        ("dataset", "asc", "LOWER(dataset) ASC NULLS LAST"),
    ],
)
def test_run_sorting_uses_allowlisted_expressions_and_stable_ties(
    field: str, direction: str, expected: str
) -> None:
    repository = CapturingRepository()

    repository.runs(limit=25, sort=field, direction=direction)

    assert expected in repository.query
    assert "LOWER(run_id) DESC NULLS LAST" in repository.query
    assert repository.parameters == (25,)


def test_run_filters_are_applied_before_server_sort_and_limit() -> None:
    repository = CapturingRepository()

    repository.runs(
        limit=10,
        search="Course",
        status="SUCCEEDED",
        load_strategy="full",
        sort="rows_loaded",
        direction="desc",
    )

    assert repository.query.index(" WHERE ") < repository.query.index(" ORDER BY ")
    assert repository.query.index(" ORDER BY ") < repository.query.index(" LIMIT ")
    assert repository.parameters == (
        "%course%",
        "%course%",
        "SUCCEEDED",
        "full",
        10,
    )


@pytest.mark.parametrize(
    ("method", "field", "expected"),
    [
        ("dataset_quality", "records_failed", "records_failed DESC NULLS LAST"),
        ("schema_drift", "policy", "LOWER(policy) ASC NULLS LAST"),
    ],
)
def test_quality_and_drift_sorting_is_allowlisted(method: str, field: str, expected: str) -> None:
    repository = CapturingRepository()

    if method == "dataset_quality":
        repository.dataset_quality("customers", limit=20, sort=field, direction="desc")
    else:
        repository.schema_drift(limit=20, sort=field, direction="asc")

    assert expected in repository.query
    assert repository.parameters[-1] == 20


def test_schema_drift_filters_before_sorting_and_limit() -> None:
    repository = CapturingRepository()

    repository.schema_drift(
        limit=20,
        dataset="customers",
        drift_type="column_added",
        action_taken="WARNED",
        sort="detected_at",
        direction="desc",
    )

    assert repository.query.index(" WHERE ") < repository.query.index(" ORDER BY ")
    assert repository.query.index(" ORDER BY ") < repository.query.index(" LIMIT ")
    assert repository.parameters == ("customers", "column_added", "WARNED", 20)


@pytest.mark.parametrize(
    ("field", "direction"),
    [
        ("started_at; DROP TABLE runs", "asc"),
        ("not_a_column", "desc"),
        ("started_at", "sideways"),
    ],
)
def test_repository_rejects_arbitrary_sort_input(field: str, direction: str) -> None:
    with pytest.raises(RepositoryError, match="Unsupported sort request"):
        CapturingRepository().runs(limit=10, sort=field, direction=direction)


@pytest.mark.parametrize(
    "path",
    [
        "/api/runs?sort=started_at%3B%20DROP%20TABLE%20runs",
        "/api/runs?sort=not_a_column",
        "/api/runs?direction=sideways",
        "/api/schema-drift?sort=not_a_column",
        "/api/datasets/customers/quality?direction=sideways",
    ],
)
def test_api_rejects_invalid_sort_fields_and_directions(path: str) -> None:
    response = TestClient(create_app()).get(path)

    assert response.status_code == 422


@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_runs_endpoint_passes_valid_sort_filters_and_direction(direction: str) -> None:
    class ApiRepository:
        def __init__(self) -> None:
            self.values: dict[str, Any] = {}

        def runs(self, **values: Any) -> list[dict[str, Any]]:
            self.values = values
            return []

    repository = ApiRepository()
    application = create_app()
    application.state.repository = repository

    response = TestClient(application).get(
        "/api/runs",
        params={
            "limit": 25,
            "search": "term",
            "status": "SUCCEEDED",
            "load_strategy": "full",
            "sort": "duration_seconds",
            "direction": direction,
        },
    )

    assert response.status_code == 200
    assert repository.values == {
        "limit": 25,
        "dataset": None,
        "search": "term",
        "status": "SUCCEEDED",
        "load_strategy": "full",
        "sort": "duration_seconds",
        "direction": direction,
    }
