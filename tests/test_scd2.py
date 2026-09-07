from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar, Self

import pytest
import yaml

from metadata_etl import pipeline
from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError, LoadError
from metadata_etl.postgres import LoadMetrics

ROOT = Path(__file__).parents[1]


class HistoryStore:
    history: ClassVar[list[dict[str, Any]]] = []
    schemas: ClassVar[tuple[str | None, str | None]] = (None, None)
    failed: ClassVar[bool] = False

    def __init__(self, dsn: str) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    @classmethod
    def reset(cls) -> None:
        cls.history = []
        cls.schemas = (None, None)
        cls.failed = False

    def ensure_metadata_tables(self) -> None:
        return None

    def start_run(self, **values: Any) -> None:
        return None

    def finish_run(self, **values: Any) -> None:
        if values["status"] == "SUCCEEDED":
            type(self).schemas = (values["raw_schema_json"], values["canonical_schema_json"])

    def get_previous_successful_schemas(self, dataset: str) -> tuple[str | None, str | None]:
        return type(self).schemas

    def record_schema_drift(self, **values: Any) -> None:
        return None

    def record_quality_results(self, **values: Any) -> None:
        return None

    def publish_scd2(self, **values: Any) -> LoadMetrics:
        if type(self).failed:
            type(self).failed = False
            raise LoadError("simulated SCD2 transaction failure")
        names = [column[0] for column in values["columns"]]
        scd2 = values["scd2"]
        positions = {name: index for index, name in enumerate(names)}
        inserted = expired = history_inserted = 0
        for row in sorted(
            values["rows"], key=lambda item: item[positions[scd2.effective_timestamp]]
        ):
            key = tuple(row[positions[name]] for name in scd2.keys)
            tracked = tuple(row[positions[name]] for name in scd2.tracked_columns)
            effective = row[positions[scd2.effective_timestamp]]
            exact = next(
                (
                    item
                    for item in type(self).history
                    if item["key"] == key and item["valid_from"] == effective
                ),
                None,
            )
            if exact:
                if exact["tracked"] != tracked:
                    raise LoadError("conflicting version")
                continue
            current = next(
                (item for item in type(self).history if item["key"] == key and item["is_current"]),
                None,
            )
            if current and current["tracked"] == tracked:
                continue
            if current:
                current["valid_to"] = effective
                current["is_current"] = False
                expired += 1
            else:
                inserted += 1
            type(self).history.append(
                {
                    "key": key,
                    "tracked": tracked,
                    "valid_from": effective,
                    "valid_to": None,
                    "is_current": True,
                }
            )
            history_inserted += 1
        return LoadMetrics(history_inserted, inserted, expired, expired, history_inserted)


def _config_with_source(tmp_path: Path, base_name: str, source_text: str) -> Path:
    config = yaml.safe_load((ROOT / "configs" / base_name).read_text(encoding="utf-8"))
    source = tmp_path / f"{base_name}.csv"
    source.write_text(source_text, encoding="utf-8")
    config["source"]["path"] = str(source)
    config["runtime"]["raw_root"] = str(tmp_path / "raw")
    path = tmp_path / base_name
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_scd2_first_change_and_unchanged_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", HistoryStore)
    HistoryStore.reset()
    first = _config_with_source(
        tmp_path,
        "scd2_regions_run1.yaml",
        "Entity ID,Region,Segment,Updated At\nK001,CA,STANDARD,2026-01-01 00:00:00\n",
    )
    second = _config_with_source(
        tmp_path,
        "scd2_regions_run2.yaml",
        "Entity ID,Region,Segment,Updated At\nK001,TX,STANDARD,2026-09-06 00:00:00\n",
    )

    initial = pipeline.run_pipeline(first)
    changed = pipeline.run_pipeline(second)
    unchanged = pipeline.run_pipeline(second)
    third = _config_with_source(
        tmp_path,
        "scd2_regions_run2.yaml",
        "Entity ID,Region,Segment,Updated At\nK001,FL,PREMIUM,2026-10-01 00:00:00\n",
    )
    third_change = pipeline.run_pipeline(third)
    third_rerun = pipeline.run_pipeline(third)

    assert (initial.rows_history_inserted, changed.rows_expired) == (1, 1)
    assert unchanged.rows_history_inserted == 0
    assert third_change.rows_expired == 1
    assert third_rerun.rows_history_inserted == 0
    assert len(HistoryStore.history) == 3
    assert sum(item["is_current"] for item in HistoryStore.history) == 1
    assert HistoryStore.history[0]["valid_to"].isoformat() == "2026-09-06T00:00:00"
    assert HistoryStore.history[1]["valid_to"].isoformat() == "2026-10-01T00:00:00"
    assert HistoryStore.history[2]["tracked"][0] == "FL"


def test_quality_failure_is_excluded_from_scd2_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", HistoryStore)
    HistoryStore.reset()
    config = _config_with_source(
        tmp_path,
        "scd2_regions_run1.yaml",
        "Entity ID,Region,Segment,Updated At\n,CA,STANDARD,2026-01-01 00:00:00\n",
    )

    result = pipeline.run_pipeline(config)

    assert result.rows_quarantined == 1
    assert result.rows_history_inserted == 0
    assert HistoryStore.history == []


def test_scd2_failure_leaves_existing_history_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", "postgresql://test")
    monkeypatch.setattr(pipeline, "PostgresStore", HistoryStore)
    HistoryStore.reset()
    first = _config_with_source(
        tmp_path,
        "scd2_regions_run1.yaml",
        "Entity ID,Region,Segment,Updated At\nK001,CA,STANDARD,2026-01-01 00:00:00\n",
    )
    second = _config_with_source(
        tmp_path,
        "scd2_regions_run2.yaml",
        "Entity ID,Region,Segment,Updated At\nK001,TX,STANDARD,2026-09-06 00:00:00\n",
    )
    pipeline.run_pipeline(first)
    before = [dict(item) for item in HistoryStore.history]
    HistoryStore.failed = True

    with pytest.raises(LoadError, match="transaction failure"):
        pipeline.run_pipeline(second)

    assert HistoryStore.history == before


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (("keys", []), "non-empty list"),
        (("keys", ["entity_id", "entity_id"]), "duplicate"),
        (("tracked_columns", []), "non-empty list"),
        (("tracked_columns", ["missing"]), "unknown column"),
        (("effective_timestamp", {"column": "region"}), "date or timestamp"),
        (
            (
                "history_columns",
                {"valid_from": "valid_from", "valid_to": "valid_from", "is_current": "current"},
            ),
            "must be distinct",
        ),
    ],
)
def test_invalid_scd2_configuration_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: tuple[str, Any], message: str
) -> None:
    monkeypatch.chdir(ROOT)
    config = yaml.safe_load(
        (ROOT / "configs" / "scd2_regions_run1.yaml").read_text(encoding="utf-8")
    )
    config["load"][change[0]] = change[1]
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(ConfigError, match=message):
        load_config(path)
