from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, ClassVar, Self

import pytest
import yaml

from metadata_etl import pipeline
from metadata_etl.config import load_config
from metadata_etl.postgres import LoadMetrics
from metadata_etl.validation import validate_plan
from scripts import run_portfolio_demo

ROOT = Path(__file__).parents[1]


class PortfolioProofStore:
    trusted: ClassVar[dict[str, list[tuple[Any, ...]]]] = {}
    columns: ClassVar[dict[str, list[str]]] = {}
    watermarks: ClassVar[dict[tuple[str, str], str]] = {}
    schemas: ClassVar[dict[str, tuple[str, str]]] = {}
    quality: ClassVar[dict[str, tuple[list[Any], list[Any]]]] = {}

    def __init__(self, dsn: str) -> None:
        assert dsn == "postgresql://portfolio-proof"
        self.dataset: str | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    @classmethod
    def reset(cls) -> None:
        cls.trusted = {}
        cls.columns = {}
        cls.watermarks = {}
        cls.schemas = {}
        cls.quality = {}

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        self.dataset = values["dataset"]

    def finish_run(self, **values: Any) -> None:
        if values["status"] == "SUCCEEDED":
            assert self.dataset is not None
            self.schemas[self.dataset] = (
                values["raw_schema_json"],
                values["canonical_schema_json"],
            )

    def get_previous_successful_schemas(self, dataset: str) -> tuple[str | None, str | None]:
        return self.schemas.get(dataset, (None, None))

    def record_schema_drift(self, **values: Any) -> None:
        return None

    def get_watermark(self, dataset: str, watermark_column: str) -> str | None:
        return self.watermarks.get((dataset, watermark_column))

    def record_quality_results(self, **values: Any) -> None:
        self.quality[values["dataset"]] = (
            list(values["summaries"]),
            list(values["quarantine_records"]),
        )

    def publish_full(self, **values: Any) -> LoadMetrics:
        target = values["target_table"]
        rows = list(values["rows"])
        self.trusted[target] = rows
        self.columns[target] = [name for name, _ in values["columns"]]
        return LoadMetrics(len(rows), len(rows), 0)

    def publish_incremental(self, **values: Any) -> LoadMetrics:
        target = values["target_table"]
        rows = list(values["rows"])
        self.trusted.setdefault(target, []).extend(rows)
        self.columns[target] = [name for name, _ in values["columns"]]
        watermark = values["watermark_after"]
        if watermark is not None:
            self.watermarks[(values["dataset"], values["watermark_column"])] = watermark
        return LoadMetrics(len(rows), len(rows), 0)

    def publish_upsert(self, **values: Any) -> LoadMetrics:
        target = values["target_table"]
        rows = list(values["rows"])
        names = [name for name, _ in values["columns"]]
        key_indexes = [names.index(key) for key in values["keys"]]
        merged = {
            tuple(row[index] for index in key_indexes): row for row in self.trusted.get(target, [])
        }
        inserted = 0
        updated = 0
        for row in rows:
            key = tuple(row[index] for index in key_indexes)
            if key in merged:
                updated += 1
            else:
                inserted += 1
            merged[key] = row
        self.trusted[target] = list(merged.values())
        self.columns[target] = names
        return LoadMetrics(len(rows), inserted, updated)


@pytest.mark.parametrize(
    ("config_name", "source_type", "load_strategy", "operators", "contracts"),
    [
        (
            "portfolio_orders.yaml",
            "json",
            "upsert",
            {"cast", "map", "filter", "derive"},
            {"not_null", "unique", "range"},
        ),
        (
            "students_quality.yaml",
            "csv",
            "full",
            {"derive"},
            {"not_null", "unique", "range", "regex"},
        ),
        (
            "portfolio_sensor_telemetry.yaml",
            "parquet",
            "incremental",
            {"cast", "map", "filter", "deduplicate", "derive"},
            {"not_null", "unique", "range"},
        ),
    ],
)
def test_three_domains_use_valid_generic_configuration(
    config_name: str,
    source_type: str,
    load_strategy: str,
    operators: set[str],
    contracts: set[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(ROOT)
    config = load_config(ROOT / "configs" / config_name)

    validate_plan(config)

    assert config.source_type == source_type
    assert config.load_strategy == load_strategy
    assert {item.type for item in config.transformations} == operators
    assert {item.type for item in config.contracts} == contracts


def _copy_demo_config(tmp_path: Path, name: str, dataset: str) -> Path:
    config = yaml.safe_load((ROOT / "configs" / name).read_text(encoding="utf-8"))
    source_path = ROOT / config["source"]["path"]
    config["dataset"]["name"] = dataset
    config["source"]["path"] = str(source_path)
    config["runtime"]["raw_root"] = str(tmp_path / "raw")
    config["orchestration"] = {"enabled": False}
    config["load"]["staging_table"] = f"{dataset}_staging"
    config["load"]["target_table"] = dataset
    path = tmp_path / f"{dataset}.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_three_unrelated_domains_execute_through_the_same_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://portfolio-proof")
    monkeypatch.setattr(pipeline, "PostgresStore", PortfolioProofStore)
    PortfolioProofStore.reset()

    orders = _copy_demo_config(tmp_path, "portfolio_orders.yaml", "proof_orders")
    students = _copy_demo_config(tmp_path, "students_quality.yaml", "proof_students")
    telemetry = _copy_demo_config(tmp_path, "portfolio_sensor_telemetry.yaml", "proof_telemetry")

    order_first = pipeline.run_pipeline(orders)
    order_repeat = pipeline.run_pipeline(orders)
    student_run = pipeline.run_pipeline(students)
    telemetry_first = pipeline.run_pipeline(telemetry)
    telemetry_repeat = pipeline.run_pipeline(telemetry)

    assert (
        order_first.rows_extracted,
        order_first.rows_transformed,
        order_first.rows_quarantined,
        order_first.rows_loaded,
    ) == (4, 3, 1, 2)
    assert order_repeat.rows_inserted == 0
    assert len(PortfolioProofStore.trusted["proof_orders"]) == 2

    assert (
        student_run.rows_extracted,
        student_run.rows_transformed,
        student_run.rows_quarantined,
        student_run.rows_loaded,
    ) == (5, 5, 4, 1)
    student_id_index = PortfolioProofStore.columns["proof_students"].index("student_id")
    assert PortfolioProofStore.trusted["proof_students"][0][student_id_index] == "00123"

    assert (
        telemetry_first.rows_extracted,
        telemetry_first.rows_transformed,
        telemetry_first.rows_quarantined,
        telemetry_first.rows_loaded,
    ) == (5, 3, 1, 2)
    assert telemetry_repeat.rows_extracted == 0
    assert telemetry_repeat.rows_loaded == 0
    assert len(PortfolioProofStore.trusted["proof_telemetry"]) == 2


def test_demo_script_delegates_processing_to_the_public_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(arguments: list[str], **options: Any) -> subprocess.CompletedProcess[str]:
        captured["arguments"] = arguments
        captured["options"] = options
        return subprocess.CompletedProcess(arguments, 0, stdout="validated\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert run_portfolio_demo.invoke_cli("validate-all", "configs") == "validated"
    assert captured["arguments"][1:3] == ["-m", "metadata_etl.cli"]
    assert captured["arguments"][3:] == ["validate-all", "configs"]
    assert captured["options"]["cwd"] == ROOT


def test_no_portfolio_domain_names_are_embedded_in_engine_code() -> None:
    engine_source = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "src" / "metadata_etl").rglob("*.py")
    ).lower()

    assert "portfolio_order_lines" not in engine_source
    assert "students_quality" not in engine_source
    assert "portfolio_sensor_telemetry" not in engine_source
