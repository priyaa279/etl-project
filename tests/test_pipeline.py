from pathlib import Path
from typing import Any, ClassVar, Self

import pytest

from metadata_etl import pipeline

ROOT = Path(__file__).parents[1]
CONFIG = ROOT / "configs" / "customers.yaml"


class FakePostgresStore:
    statuses: ClassVar[list[str]] = []

    def __init__(self, dsn: str) -> None:
        assert dsn == "postgresql://test"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        assert values["dataset"] == "customers"

    def finish_run(self, **values: Any) -> None:
        self.statuses.append(values["status"])

    def record_quality_results(self, **values: Any) -> None:
        assert values["summaries"] == []
        assert values["quarantine_records"] == []

    def publish_full(self, **values: Any) -> int:
        rows = values["rows"]
        columns = [item[0] for item in values["columns"]]
        assert len(rows) == 3
        assert rows[0][columns.index("customer_id")] == "00123"
        return len(rows)


def test_pipeline_orchestrates_the_vertical_slice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", FakePostgresStore)
    FakePostgresStore.statuses = []
    config_text = CONFIG.read_text(encoding="utf-8").replace(
        "raw_root: data/raw", f"raw_root: {tmp_path.as_posix()}/raw"
    )
    config_path = tmp_path / "customers.yaml"
    config_path.write_text(config_text, encoding="utf-8")

    result = pipeline.run_pipeline(config_path)

    assert result.status == "SUCCEEDED"
    assert result.rows_extracted == 4
    assert result.rows_transformed == 3
    assert result.rows_contract_passed == 3
    assert result.rows_quarantined == 0
    assert result.rows_loaded == 3
    assert Path(result.raw_path).read_bytes() == (ROOT / "data/incoming/customers.csv").read_bytes()
    assert FakePostgresStore.statuses == ["SUCCEEDED"]
