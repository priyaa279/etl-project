from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, Self

import pytest
import yaml

from metadata_etl import pipeline
from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError
from metadata_etl.postgres import PostgresStore
from metadata_etl.quality.engine import QualitySummary, QuarantineRecord
from metadata_etl.quality.privacy import protect_value

ROOT = Path(__file__).parents[1]
QUALITY_CONFIG = ROOT / "configs" / "students_quality.yaml"


class CapturingPostgresStore:
    summaries: ClassVar[list[QualitySummary]] = []
    quarantine_records: ClassVar[list[QuarantineRecord]] = []
    published_rows: ClassVar[list[tuple[Any, ...]]] = []
    published_columns: ClassVar[list[str]] = []
    finished: ClassVar[dict[str, Any]] = {}

    def __init__(self, dsn: str) -> None:
        assert dsn == "postgresql://test"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        assert values["dataset"] == "students_quality"

    def finish_run(self, **values: Any) -> None:
        type(self).finished = values

    def record_quality_results(self, **values: Any) -> None:
        type(self).summaries = list(values["summaries"])
        type(self).quarantine_records = list(values["quarantine_records"])

    def publish_full(self, **values: Any) -> int:
        type(self).published_rows = list(values["rows"])
        type(self).published_columns = [column[0] for column in values["columns"]]
        return len(type(self).published_rows)

    @classmethod
    def reset(cls) -> None:
        cls.summaries = []
        cls.quarantine_records = []
        cls.published_rows = []
        cls.published_columns = []
        cls.finished = {}


def _temporary_quality_config(tmp_path: Path) -> Path:
    config = QUALITY_CONFIG.read_text(encoding="utf-8").replace(
        "raw_root: data/raw", f"raw_root: {tmp_path.as_posix()}/raw"
    )
    path = tmp_path / "students_quality.yaml"
    path.write_text(config, encoding="utf-8")
    return path


def test_contracts_split_rows_after_derived_transform_and_preserve_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", CapturingPostgresStore)
    CapturingPostgresStore.reset()

    result = pipeline.run_pipeline(_temporary_quality_config(tmp_path))

    assert result.rows_extracted == 5
    assert result.rows_transformed == 5
    assert result.rows_contract_passed == 1
    assert result.rows_quarantined == 4
    assert result.rows_loaded == 1
    assert len(CapturingPostgresStore.published_rows) == 1
    assert CapturingPostgresStore.published_rows[0][0] == "00123"
    assert CapturingPostgresStore.finished["status"] == "SUCCEEDED"
    assert CapturingPostgresStore.finished["rows_contract_passed"] == 1
    assert CapturingPostgresStore.finished["rows_quarantined"] == 4

    summaries = {summary.rule_id: summary for summary in CapturingPostgresStore.summaries}
    assert summaries["DQ001"].records_failed == 1
    assert summaries["DQ002"].records_failed == 2
    assert summaries["DQ003"].records_failed == 1
    assert summaries["DQ004"].records_failed == 1
    assert summaries["DQ005"].records_failed == 1
    assert all(summary.records_checked == 5 for summary in summaries.values())
    assert all(summary.status == "FAILED" for summary in summaries.values())


def test_quarantine_details_apply_all_privacy_policies_and_track_multiple_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", CapturingPostgresStore)
    CapturingPostgresStore.reset()

    pipeline.run_pipeline(_temporary_quality_config(tmp_path))
    records = CapturingPostgresStore.quarantine_records

    assert len(records) == 6
    assert (
        next(record for record in records if record.rule_id == "DQ003").failed_value == "4.200000"
    )
    assert (
        next(record for record in records if record.rule_id == "DQ004").failed_value == "b*******l"
    )
    assert next(record for record in records if record.rule_id == "DQ001").failed_value is None
    assert next(record for record in records if record.rule_id == "DQ005").failed_value is None

    unique_values = {record.failed_value for record in records if record.rule_id == "DQ002"}
    expected_hash = protect_value("00982", "hashed")
    assert unique_values == {f'{{"student_id":"{expected_hash}"}}'}

    gpa_failure = next(record for record in records if record.rule_id == "DQ003")
    email_failure = next(record for record in records if record.rule_id == "DQ004")
    assert gpa_failure.record_identifier == email_failure.record_identifier
    assert gpa_failure.failed_column == "gpa"
    assert email_failure.failed_column == "email"


def test_privacy_helpers_are_deterministic() -> None:
    assert protect_value("secret", "full") == "secret"
    assert protect_value("secret", "masked") == "s****t"
    assert protect_value("secret", "hashed") == protect_value("secret", "hashed")
    assert protect_value("secret", "hashed").startswith("sha256:")
    assert protect_value("secret", "none") is None


@pytest.mark.parametrize(
    ("contracts", "message"),
    [
        ([{"id": "DQ001", "type": "unknown", "column": "gpa"}], "Unsupported contract type"),
        ([{"id": "DQ001", "type": "not_null", "column": "missing"}], "unknown column"),
        (
            [{"id": "DQ001", "type": "range", "column": "gpa", "min": 5, "max": 4}],
            "min cannot be greater",
        ),
        (
            [{"id": "DQ001", "type": "range", "column": "gpa", "min": "low"}],
            "finite numeric value",
        ),
        (
            [{"id": "DQ001", "type": "regex", "column": "email", "pattern": "["}],
            "valid regular expression",
        ),
        ([{"id": "DQ001", "type": "unique", "columns": []}], "non-empty list"),
        (
            [
                {"id": "DQ001", "type": "not_null", "column": "email"},
                {"id": "DQ001", "type": "not_null", "column": "student_id"},
            ],
            "Duplicate contract id",
        ),
    ],
)
def test_invalid_contract_definitions_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    contracts: list[dict[str, Any]],
    message: str,
) -> None:
    monkeypatch.chdir(ROOT)
    config = yaml.safe_load(QUALITY_CONFIG.read_text(encoding="utf-8"))
    config["contracts"] = contracts
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_contract_can_reference_derived_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    config = load_config(QUALITY_CONFIG)

    assert config.contracts[-1].values["column"] == "total_cost"


def test_invalid_quarantine_policy_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    config = yaml.safe_load(QUALITY_CONFIG.read_text(encoding="utf-8"))
    config["columns"]["email"]["quarantine"]["value"] = "plaintext"
    path = tmp_path / "invalid_policy.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(ConfigError, match="must be full, masked, hashed, or none"):
        load_config(path)


class FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def transaction(self):
        return nullcontext()

    def cursor(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def executemany(self, statement: str, values: list[tuple[Any, ...]]) -> None:
        self.calls.append((statement, values))


def test_postgres_store_writes_rule_summaries_and_quarantine_records() -> None:
    timestamp = datetime.now(UTC)
    summary = QualitySummary("DQ001", "not_null", 2, 1, "FAILED", timestamp)
    record = QuarantineRecord(
        "DQ001", "not_null", "id must not be null", "sha256:record", "id", None, timestamp
    )
    connection = FakeConnection()
    store = PostgresStore("unused")
    store.connection = connection  # type: ignore[assignment]

    store.record_quality_results(
        run_id="RUN_TEST",
        dataset="example",
        summaries=[summary],
        quarantine_records=[record],
    )

    assert len(connection.calls) == 2
    assert "data_quality_results" in connection.calls[0][0]
    assert connection.calls[0][1][0][:4] == ("RUN_TEST", "example", "DQ001", "not_null")
    assert "etl_quarantine" in connection.calls[1][0]
    assert connection.calls[1][1][0][5] == "sha256:record"
