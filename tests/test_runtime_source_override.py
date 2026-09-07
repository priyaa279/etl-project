from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, Self

import duckdb
import pytest

from metadata_etl import pipeline
from metadata_etl.config import load_config, with_source_override
from metadata_etl.errors import ConfigError
from metadata_etl.postgres import LoadMetrics
from metadata_etl.validation import validate_plan

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize(
    ("config_name", "source_name"),
    [
        ("customers.yaml", "customers.csv"),
        ("portfolio_orders.yaml", "portfolio_orders.json"),
        ("portfolio_sensor_telemetry.yaml", "portfolio_sensor_telemetry.parquet"),
    ],
)
def test_file_source_override_reuses_approved_plan(
    tmp_path: Path, config_name: str, source_name: str
) -> None:
    source = ROOT / "data" / "incoming" / source_name
    override = tmp_path / source.name
    override.write_bytes(source.read_bytes())
    config = load_config(ROOT / "configs" / config_name)

    runtime = with_source_override(config, override)
    validate_plan(config, source_override=override)

    assert runtime.source_path == override.resolve()
    assert runtime.config_hash == config.config_hash
    assert runtime.raw == config.raw


def test_postgres_source_cannot_be_replaced_with_a_file(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    config = load_config(ROOT / "configs" / "postgres_assets.yaml", require_source=False)

    with pytest.raises(ConfigError, match="only for file sources"):
        with_source_override(config, source)


class CorrelationStore:
    started: ClassVar[dict[str, Any]] = {}

    def __init__(self, _dsn: str) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        self.started = values
        CorrelationStore.started = values

    def finish_run(self, **_values: Any) -> None:
        return None

    def get_previous_successful_schemas(self, _dataset: str) -> tuple[None, None]:
        return None, None

    def record_schema_drift(self, **_values: Any) -> None:
        return None

    def record_quality_results(self, **_values: Any) -> None:
        return None

    def publish_full(self, **values: Any) -> LoadMetrics:
        return LoadMetrics(len(values["rows"]), len(values["rows"]), 0)


def test_pipeline_preserves_override_raw_copy_and_exact_correlation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", CorrelationStore)
    source = tmp_path / "uploaded.csv"
    source.write_bytes((ROOT / "data" / "incoming" / "customers.csv").read_bytes())
    config_text = (ROOT / "configs" / "customers.yaml").read_text(encoding="utf-8")
    config_text = config_text.replace("raw_root: data/raw", f"raw_root: {tmp_path.as_posix()}/raw")
    config_path = tmp_path / "customers.yaml"
    config_path.write_text(config_text, encoding="utf-8")

    result = pipeline.run_pipeline(
        config_path,
        source_override=source,
        correlation_id="upload-correlation-1",
    )

    assert result.correlation_id == "upload-correlation-1"
    assert CorrelationStore.started["correlation_id"] == "upload-correlation-1"
    assert Path(result.raw_path).read_bytes() == source.read_bytes()
    assert Path(result.raw_path).resolve() != source.resolve()


def test_non_override_compilation_remains_unchanged() -> None:
    config = load_config(ROOT / "configs" / "customers.yaml")
    with duckdb.connect(":memory:"):
        validate_plan(config)
