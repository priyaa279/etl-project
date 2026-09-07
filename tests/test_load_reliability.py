from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, Self

import pytest
import yaml

from metadata_etl import pipeline
from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError, LoadError, SchemaDriftError
from metadata_etl.postgres import LoadMetrics, PostgresStore


class StatefulStore:
    trusted: ClassVar[list[tuple[Any, ...]]] = []
    columns: ClassVar[list[str]] = []
    watermark: ClassVar[str | None] = None
    raw_schema_json: ClassVar[str | None] = None
    canonical_schema_json: ClassVar[str | None] = None
    drift_events: ClassVar[list[Any]] = []
    finishes: ClassVar[list[dict[str, Any]]] = []
    fail_next_publish: ClassVar[bool] = False

    def __init__(self, dsn: str) -> None:
        assert dsn == "postgresql://test"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    @classmethod
    def reset(cls) -> None:
        cls.trusted = []
        cls.columns = []
        cls.watermark = None
        cls.raw_schema_json = None
        cls.canonical_schema_json = None
        cls.drift_events = []
        cls.finishes = []
        cls.fail_next_publish = False

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        return None

    def finish_run(self, **values: Any) -> None:
        type(self).finishes.append(values)
        if values["status"] == "SUCCEEDED":
            type(self).raw_schema_json = values["raw_schema_json"]
            type(self).canonical_schema_json = values["canonical_schema_json"]

    def get_previous_successful_schemas(self, dataset: str) -> tuple[str | None, str | None]:
        return type(self).raw_schema_json, type(self).canonical_schema_json

    def record_schema_drift(self, **values: Any) -> None:
        type(self).drift_events.extend(values["events"])

    def get_watermark(self, dataset: str, watermark_column: str) -> str | None:
        return type(self).watermark

    def record_quality_results(self, **values: Any) -> None:
        return None

    def _maybe_fail(self) -> None:
        if type(self).fail_next_publish:
            type(self).fail_next_publish = False
            raise LoadError("simulated publish failure")

    def publish_full(self, **values: Any) -> LoadMetrics:
        self._maybe_fail()
        type(self).trusted = list(values["rows"])
        type(self).columns = [item[0] for item in values["columns"]]
        count = len(type(self).trusted)
        return LoadMetrics(count, count, 0)

    def publish_incremental(self, **values: Any) -> LoadMetrics:
        self._maybe_fail()
        rows = list(values["rows"])
        type(self).trusted.extend(rows)
        type(self).columns = [item[0] for item in values["columns"]]
        if values["watermark_after"] is not None:
            type(self).watermark = values["watermark_after"]
        return LoadMetrics(len(rows), len(rows), 0)

    def publish_upsert(self, **values: Any) -> LoadMetrics:
        self._maybe_fail()
        rows = list(values["rows"])
        names = [item[0] for item in values["columns"]]
        indexes = [names.index(key) for key in values["keys"]]
        existing = {tuple(row[index] for index in indexes): row for row in type(self).trusted}
        inserted = 0
        updated = 0
        for row in rows:
            key = tuple(row[index] for index in indexes)
            if key in existing:
                updated += 1
            else:
                inserted += 1
            existing[key] = row
        type(self).trusted = list(existing.values())
        type(self).columns = names
        return LoadMetrics(len(rows), inserted, updated)


def _write_config(
    tmp_path: Path,
    *,
    dataset: str,
    source: Path,
    strategy: str,
    keys: list[str] | None = None,
    added_policy: str = "warn",
) -> Path:
    config: dict[str, Any] = {
        "config_schema_version": "1.0",
        "dataset": {"name": dataset},
        "review": {"required": True, "approved": True, "approved_by": "tester"},
        "source": {"type": "csv", "path": str(source)},
        "runtime": {"raw_root": str(tmp_path / "raw")},
        "normalization": {"trim_strings": True, "null_tokens": [""]},
        "columns": {
            "entity_id": {"source": "ID", "type": "string", "nullable": False},
            "value": {"source": "Value", "type": "string", "nullable": False},
            "updated_at": {
                "source": "Updated At",
                "type": "timestamp",
                "nullable": False,
            },
        },
        "transformations": [],
        "contracts": [{"id": "DQ001", "type": "not_null", "column": "entity_id"}],
        "schema_drift": {
            "added_columns": added_policy,
            "removed_columns": "fail",
            "datatype_change": "fail",
            "canonical_change": "fail",
        },
        "load": {
            "strategy": strategy,
            "connection_env": "ETL_POSTGRES_DSN",
            "schema": "public",
            "staging_table": f"{dataset}_staging",
            "target_table": dataset,
        },
    }
    if strategy == "incremental":
        config["load"]["watermark"] = {"column": "updated_at", "type": "timestamp"}
    if strategy == "upsert":
        config["load"]["keys"] = keys
    path = tmp_path / f"{dataset}.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def _setup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", StatefulStore)
    StatefulStore.reset()


def test_incremental_first_later_and_no_new_data_runs_are_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(monkeypatch)
    source = tmp_path / "events.csv"
    source.write_text(
        "ID,Value,Updated At\nA,one,2026-09-01 09:00:00\nB,two,2026-09-02 10:00:00\n",
        encoding="utf-8",
    )
    config = _write_config(tmp_path, dataset="events", source=source, strategy="incremental")

    first = pipeline.run_pipeline(config)
    source.write_text(
        "ID,Value,Updated At\nA,one,2026-09-01 09:00:00\nB,two,2026-09-02 10:00:00\n"
        "C,three,2026-09-03 11:00:00\n",
        encoding="utf-8",
    )
    second = pipeline.run_pipeline(config)
    third = pipeline.run_pipeline(config)

    assert (first.rows_extracted, second.rows_extracted, third.rows_extracted) == (2, 1, 0)
    assert first.watermark_before is None
    assert first.watermark_after == "2026-09-02 10:00:00"
    assert second.watermark_before == first.watermark_after
    assert second.watermark_after == "2026-09-03 11:00:00"
    assert third.watermark_before == third.watermark_after
    assert len(StatefulStore.trusted) == 3


def test_failed_incremental_publish_does_not_advance_watermark_and_retry_is_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(monkeypatch)
    source = tmp_path / "events.csv"
    source.write_text("ID,Value,Updated At\nA,one,2026-09-01 09:00:00\n", encoding="utf-8")
    config = _write_config(tmp_path, dataset="events", source=source, strategy="incremental")
    pipeline.run_pipeline(config)
    original_watermark = StatefulStore.watermark
    source.write_text(
        "ID,Value,Updated At\nA,one,2026-09-01 09:00:00\nB,two,2026-09-02 10:00:00\n",
        encoding="utf-8",
    )
    StatefulStore.fail_next_publish = True

    with pytest.raises(LoadError, match="simulated"):
        pipeline.run_pipeline(config)

    assert StatefulStore.watermark == original_watermark
    retry = pipeline.run_pipeline(config)
    assert retry.rows_extracted == 1
    assert len(StatefulStore.trusted) == 2
    assert StatefulStore.watermark == "2026-09-02 10:00:00"


def test_upsert_inserts_updates_and_does_not_duplicate_on_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(monkeypatch)
    source = tmp_path / "entities.csv"
    source.write_text(
        "ID,Value,Updated At\nA,one,2026-09-01 09:00:00\nB,two,2026-09-01 09:00:00\n",
        encoding="utf-8",
    )
    config = _write_config(
        tmp_path, dataset="entities", source=source, strategy="upsert", keys=["entity_id"]
    )
    first = pipeline.run_pipeline(config)
    source.write_text(
        "ID,Value,Updated At\nA,changed,2026-09-02 09:00:00\n"
        "B,two,2026-09-01 09:00:00\nC,three,2026-09-02 10:00:00\n",
        encoding="utf-8",
    )
    second = pipeline.run_pipeline(config)
    third = pipeline.run_pipeline(config)

    assert (first.rows_inserted, first.rows_updated) == (2, 0)
    assert (second.rows_inserted, second.rows_updated) == (1, 2)
    assert (third.rows_inserted, third.rows_updated) == (0, 3)
    assert len(StatefulStore.trusted) == 3
    a_row = next(row for row in StatefulStore.trusted if row[0] == "A")
    assert a_row[1] == "changed"


def test_full_rerun_replaces_the_same_trusted_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(monkeypatch)
    source = tmp_path / "snapshot.csv"
    source.write_text("ID,Value,Updated At\nA,one,2026-09-01 09:00:00\n", encoding="utf-8")
    config = _write_config(tmp_path, dataset="snapshot", source=source, strategy="full")

    pipeline.run_pipeline(config)
    pipeline.run_pipeline(config)

    assert len(StatefulStore.trusted) == 1


def test_fail_policy_stops_before_publish_and_persists_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _setup(monkeypatch)
    source = tmp_path / "drift.csv"
    source.write_text("ID,Value,Updated At\nA,one,2026-09-01 09:00:00\n", encoding="utf-8")
    config = _write_config(
        tmp_path, dataset="drift", source=source, strategy="full", added_policy="fail"
    )
    pipeline.run_pipeline(config)
    source.write_text(
        "ID,Value,Extra,Updated At\nA,one,new,2026-09-01 09:00:00\n",
        encoding="utf-8",
    )

    with pytest.raises(SchemaDriftError, match="column_added"):
        pipeline.run_pipeline(config)

    assert len(StatefulStore.trusted) == 1
    assert any(event.drift_type == "column_added" for event in StatefulStore.drift_events)
    assert StatefulStore.finishes[-1]["status"] == "FAILED"


@pytest.mark.parametrize(
    ("keys", "message"),
    [
        (None, "non-empty list"),
        ([], "non-empty list"),
        (["entity_id", "entity_id"], "duplicate"),
        (["missing"], "unknown post-transformation"),
    ],
)
def test_invalid_upsert_keys_are_rejected(
    tmp_path: Path, keys: list[str] | None, message: str
) -> None:
    source = tmp_path / "entities.csv"
    source.write_text("ID,Value,Updated At\nA,one,2026-09-01 09:00:00\n", encoding="utf-8")
    config = _write_config(
        tmp_path, dataset="entities", source=source, strategy="upsert", keys=keys
    )

    with pytest.raises(ConfigError, match=message):
        load_config(config)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("nullable", "nullable: false"),
        ("initial", "not a valid timestamp"),
    ],
)
def test_invalid_watermark_configuration_is_rejected(
    tmp_path: Path, change: str, message: str
) -> None:
    source = tmp_path / "events.csv"
    source.write_text("ID,Value,Updated At\nA,one,2026-09-01 09:00:00\n", encoding="utf-8")
    path = _write_config(tmp_path, dataset="events", source=source, strategy="incremental")
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if change == "nullable":
        config["columns"]["updated_at"]["nullable"] = True
    else:
        config["load"]["watermark"]["initial_value"] = "not-a-timestamp"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(ConfigError, match=message):
        load_config(path)


@pytest.mark.parametrize(
    "rows",
    [
        [("A",), ("A",)],
        [(None,)],
    ],
)
def test_upsert_rejects_duplicate_or_null_runtime_keys(rows: list[tuple[Any, ...]]) -> None:
    store = PostgresStore("unused")

    with pytest.raises(LoadError, match="business keys"):
        store.publish_upsert(
            destination_schema="public",
            staging_table="example_staging",
            target_table="example",
            columns=[("entity_id", "VARCHAR")],
            rows=rows,
            run_id="RUN_TEST",
            keys=["entity_id"],
        )
