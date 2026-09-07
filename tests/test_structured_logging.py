from __future__ import annotations

import json
from io import StringIO

import pytest

from metadata_etl.structured_logging import configure_logging, emit_event


def test_json_log_contains_operational_context() -> None:
    stream = StringIO()
    configure_logging(json_output=True, stream=stream)

    emit_event(
        "TRANSFORM_COMPLETED",
        run_id="RUN_TEST",
        dataset="example",
        stage="TRANSFORM",
        rows_processed=12,
    )

    payload = json.loads(stream.getvalue())
    assert payload["level"] == "INFO"
    assert payload["event"] == "TRANSFORM_COMPLETED"
    assert payload["run_id"] == "RUN_TEST"
    assert payload["dataset"] == "example"
    assert payload["stage"] == "TRANSFORM"
    assert payload["rows_processed"] == 12
    assert "timestamp" in payload


def test_sensitive_values_are_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "postgresql://etl:super-secret@db:5432/etl"
    monkeypatch.setenv("ETL_POSTGRES_DSN", secret)
    stream = StringIO()
    configure_logging(json_output=True, stream=stream)

    emit_event("RUN_FAILED", error=f"Connection failed: {secret}", password="super-secret")

    output = stream.getvalue()
    assert secret not in output
    assert "super-secret" not in output
    assert "***" in output
