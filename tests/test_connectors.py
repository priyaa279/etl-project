from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, Self

import duckdb
import pytest
import yaml

from metadata_etl import pipeline
from metadata_etl.config import load_config
from metadata_etl.connectors import default_connector_registry
from metadata_etl.errors import ConfigError, ExtractionError
from metadata_etl.postgres import LoadMetrics
from metadata_etl.schema import raw_json_schema_fingerprint

ROOT = Path(__file__).parents[1]


class SinkStore:
    rows: ClassVar[list[tuple[Any, ...]]] = []

    def __init__(self, dsn: str) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        return None

    def finish_run(self, **values: Any) -> None:
        return None

    def get_previous_successful_schemas(self, dataset: str) -> tuple[None, None]:
        return None, None

    def record_schema_drift(self, **values: Any) -> None:
        return None

    def record_quality_results(self, **values: Any) -> None:
        return None

    def publish_full(self, **values: Any) -> LoadMetrics:
        type(self).rows = list(values["rows"])
        count = len(type(self).rows)
        return LoadMetrics(count, count, 0)


def _flat_json_config(tmp_path: Path, source: Path) -> Path:
    config = {
        "config_schema_version": "1.0",
        "dataset": {"name": "flat_json"},
        "review": {"required": True, "approved": True, "approved_by": "tester"},
        "source": {"type": "json", "path": str(source)},
        "runtime": {"raw_root": str(tmp_path / "raw")},
        "normalization": {"trim_strings": True, "null_tokens": [""]},
        "columns": {
            "record_id": {"source": "id", "type": "string", "nullable": False},
            "value": {"type": "integer", "nullable": False},
        },
        "transformations": [],
        "contracts": [],
        "load": {
            "strategy": "full",
            "connection_env": "ETL_POSTGRES_DSN",
            "schema": "public",
            "staging_table": "flat_json_staging",
            "target_table": "flat_json",
        },
    }
    path = tmp_path / "flat.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_connector_registry_contains_only_supported_source_types() -> None:
    assert default_connector_registry().names == ("csv", "json", "parquet", "postgres")


def test_flat_json_is_read_and_preserved_exactly(tmp_path: Path) -> None:
    source = tmp_path / "flat.json"
    original = '[{"id":"A","value":1},{"id":"B","value":2}]\n'
    source.write_text(original, encoding="utf-8")
    config = load_config(_flat_json_config(tmp_path, source))

    extracted = (
        default_connector_registry().get("json").extract(config, "RUN_JSON", tmp_path / "raw")
    )
    with duckdb.connect(":memory:") as connection:
        rows = connection.execute(f"SELECT * FROM {extracted.relation_sql}").fetchall()

    assert extracted.artifact.path.read_text(encoding="utf-8") == original
    assert rows == [("A", "1"), ("B", "2")]
    assert len(extracted.raw_schema.hash) == 64


def test_newline_delimited_json_is_supported(tmp_path: Path) -> None:
    source = tmp_path / "records.ndjson"
    source.write_text('{"id":"A","value":1}\n{"id":"B","value":2}\n', encoding="utf-8")
    config = load_config(_flat_json_config(tmp_path, source))

    extracted = (
        default_connector_registry().get("json").extract(config, "RUN_NDJSON", tmp_path / "raw")
    )
    with duckdb.connect(":memory:") as connection:
        count = connection.execute(f"SELECT count(*) FROM {extracted.relation_sql}").fetchone()

    assert count == (2,)


def test_nested_json_fields_and_array_explosion_produce_canonical_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    config = load_config(ROOT / "configs" / "nested_orders.yaml")
    extracted = default_connector_registry().get("json").extract(config, "RUN_NESTED", tmp_path)
    with duckdb.connect(":memory:") as connection:
        cursor = connection.execute(
            pipeline.compile_transform_sql(config, source_relation=extracted.relation_sql)
        )
        rows = cursor.fetchall()
        names = [column[0] for column in cursor.description]

    assert len(rows) == 3
    assert names == ["order_id", "customer_id", "order_date", "product_id", "quantity"]
    assert rows[0][names.index("product_id")] == "P100"
    assert rows[0][names.index("quantity")] == 2


def test_unconfigured_nested_json_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "nested.json"
    source.write_text('[{"id":"A","nested":{"value":1}}]', encoding="utf-8")
    config = load_config(_flat_json_config(tmp_path, source))

    with pytest.raises(ExtractionError, match="requires explicit normalization.json"):
        default_connector_registry().get("json").extract(config, "RUN_BAD", tmp_path / "raw")


def test_json_shape_changes_produce_different_raw_hashes() -> None:
    first = raw_json_schema_fingerprint({"records": [{"id": "A"}]})
    second = raw_json_schema_fingerprint({"records": [{"id": "A", "value": 1}]})

    assert first.hash != second.hash


def test_parquet_connector_and_full_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", SinkStore)
    SinkStore.rows = []
    config_path = ROOT / "configs" / "parquet_weather.yaml"
    config = load_config(config_path)
    extracted = default_connector_registry().get("parquet").extract(config, "RUN_PARQUET", tmp_path)

    assert extracted.artifact.path.read_bytes() == config.source_path.read_bytes()  # type: ignore[union-attr]
    assert [field.name for field in extracted.raw_schema.fields] == [
        "Station ID",
        "Temperature C",
        "Observed At",
    ]
    result = pipeline.run_pipeline(config_path)
    assert result.source_type == "parquet"
    assert result.rows_loaded == 2
    assert len(SinkStore.rows) == 2


class FakeSourceCursor:
    def __init__(self, rows: list[tuple[Any, ...]], description: list[Any] | None = None) -> None:
        self._rows = rows
        self.description = description

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class FakeSourceConnection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: Any, parameters: Any = None) -> FakeSourceCursor:
        if isinstance(statement, str) and "pg_type" in statement:
            return FakeSourceCursor([(25, "text"), (1082, "date")])
        description = [
            SimpleNamespace(name="asset_id", type_code=25),
            SimpleNamespace(name="asset_name", type_code=25),
            SimpleNamespace(name="acquired_on", type_code=1082),
        ]
        return FakeSourceCursor(
            [("A002", "Valve", "2026-02-01"), ("A001", "Pump", "2026-01-01")],
            description,
        )


def test_postgres_source_uses_environment_and_preserves_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("SOURCE_POSTGRES_DSN", "postgresql://source")
    config = load_config(ROOT / "configs" / "postgres_assets.yaml")
    monkeypatch.setattr(
        "metadata_etl.connectors.postgres.psycopg.connect", lambda dsn: FakeSourceConnection()
    )

    extracted = (
        default_connector_registry().get("postgres").extract(config, "RUN_POSTGRES", tmp_path)
    )

    assert config.source_dsn == "postgresql://source"
    assert extracted.artifact.path.read_text(encoding="utf-8").startswith(
        "asset_id,asset_name,acquired_on\nA001,Pump,2026-01-01"
    )
    assert [field.datatype for field in extracted.raw_schema.fields] == ["text", "text", "date"]


def test_missing_postgres_source_credentials_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.delenv("SOURCE_POSTGRES_DSN", raising=False)
    config = load_config(ROOT / "configs" / "postgres_assets.yaml")

    with pytest.raises(ConfigError, match="SOURCE_POSTGRES_DSN"):
        _ = config.source_dsn
