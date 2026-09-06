from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from metadata_etl.config import load_config
from metadata_etl.errors import ConfigError
from metadata_etl.sql_compiler import compile_transform_sql
from metadata_etl.transformations.registry import default_registry


def _write_config(
    tmp_path: Path,
    *,
    dataset: str,
    csv_text: str,
    columns: dict[str, dict[str, Any]],
    transformations: list[dict[str, Any]],
) -> Path:
    source_path = tmp_path / f"{dataset}.csv"
    source_path.write_text(csv_text, encoding="utf-8")
    config = {
        "config_schema_version": "1.0",
        "dataset": {"name": dataset},
        "review": {"required": True, "approved": True, "approved_by": "tester"},
        "source": {"type": "csv", "path": str(source_path), "options": {"delimiter": ","}},
        "runtime": {"raw_root": str(tmp_path / "raw")},
        "normalization": {"trim_strings": True, "null_tokens": ["", "NULL"]},
        "columns": columns,
        "transformations": transformations,
        "load": {
            "strategy": "full",
            "connection_env": "ETL_POSTGRES_DSN",
            "schema": "public",
            "staging_table": f"{dataset}_staging",
            "target_table": dataset,
        },
    }
    config_path = tmp_path / f"{dataset}.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def _execute(config_path: Path) -> tuple[list[str], list[tuple[Any, ...]]]:
    config = load_config(config_path)
    with duckdb.connect(":memory:") as connection:
        cursor = connection.execute(compile_transform_sql(config))
        rows = cursor.fetchall()
        names = [item[0] for item in cursor.description]
    return names, rows


def _all_operators() -> list[dict[str, Any]]:
    return [
        {"id": "T001", "type": "cast", "column": "measure", "datatype": "decimal"},
        {
            "id": "T002",
            "type": "map",
            "column": "category",
            "mappings": {"a": "ALPHA", "b": "BETA", "ok": "GOOD", "bad": "REJECT"},
        },
        {
            "id": "T003",
            "type": "derive",
            "target_column": "adjusted_measure",
            "datatype": "decimal",
            "expression": "measure * 2",
        },
        {"id": "T004", "type": "filter", "condition": "measure >= 10"},
        {
            "id": "T005",
            "type": "deduplicate",
            "keys": ["entity_id"],
            "order_by": {"event_time": "desc"},
        },
    ]


@pytest.mark.parametrize(
    ("dataset", "csv_text", "source_names", "expected"),
    [
        (
            "workers",
            (
                "Worker ID,Team,Pay,Updated At\nW1,a,9,2026-01-01 09:00:00\n"
                "W1,b,12,2026-01-02 09:00:00\nW2,a,20,2026-01-03 09:00:00\n"
            ),
            ("Worker ID", "Team", "Pay", "Updated At"),
            [("W1", "BETA", "24.000000"), ("W2", "ALPHA", "40.000000")],
        ),
        (
            "devices",
            (
                "Device,Quality,Reading,Observed At\nD1,bad,8,2026-02-01 09:00:00\n"
                "D1,ok,11,2026-02-01 10:00:00\nD2,ok,15,2026-02-01 11:00:00\n"
            ),
            ("Device", "Quality", "Reading", "Observed At"),
            [("D1", "GOOD", "22.000000"), ("D2", "GOOD", "30.000000")],
        ),
    ],
)
def test_all_operators_are_dataset_agnostic(
    tmp_path: Path,
    dataset: str,
    csv_text: str,
    source_names: tuple[str, str, str, str],
    expected: list[tuple[str, str, str]],
) -> None:
    id_source, category_source, measure_source, event_source = source_names
    config_path = _write_config(
        tmp_path,
        dataset=dataset,
        csv_text=csv_text,
        columns={
            "entity_id": {"source": id_source, "type": "string", "nullable": False},
            "category": {"source": category_source, "type": "string", "nullable": False},
            "measure": {"source": measure_source, "type": "string", "nullable": False},
            "event_time": {"source": event_source, "type": "timestamp", "nullable": False},
        },
        transformations=_all_operators(),
    )

    names, rows = _execute(config_path)
    selected = sorted(
        (
            row[names.index("entity_id")],
            row[names.index("category")],
            str(row[names.index("adjusted_measure")]),
        )
        for row in rows
    )

    assert selected == expected


def test_registry_exposes_only_milestone_two_operators() -> None:
    assert default_registry().names == ("cast", "deduplicate", "derive", "filter", "map")


def test_registry_rejects_unknown_operator() -> None:
    with pytest.raises(ConfigError, match="Unsupported transformation type"):
        default_registry().get("customer_cleanup")


@pytest.mark.parametrize(
    ("transformation", "message"),
    [
        (
            {"id": "T001", "type": "cast", "column": "missing", "datatype": "integer"},
            "unknown column",
        ),
        (
            {"id": "T001", "type": "filter", "condition": "measure > 1; DROP TABLE x"},
            "forbidden SQL",
        ),
        (
            {
                "id": "T001",
                "type": "derive",
                "target_column": "measure",
                "datatype": "decimal",
                "expression": "measure * 2",
            },
            "already exists",
        ),
        ({"id": "T001", "type": "map", "column": "category", "mappings": {}}, "at least one"),
        (
            {
                "id": "T001",
                "type": "deduplicate",
                "keys": ["entity_id"],
                "order_by": {"event_time": "sideways"},
            },
            "asc or desc",
        ),
    ],
)
def test_each_operator_rejects_invalid_configuration(
    tmp_path: Path, transformation: dict[str, Any], message: str
) -> None:
    config_path = _write_config(
        tmp_path,
        dataset="validation_case",
        csv_text="ID,Category,Measure,Event Time\n1,a,10,2026-01-01 00:00:00\n",
        columns={
            "entity_id": {"source": "ID", "type": "string"},
            "category": {"source": "Category", "type": "string"},
            "measure": {"source": "Measure", "type": "string"},
            "event_time": {"source": "Event Time", "type": "timestamp"},
        },
        transformations=[transformation],
    )

    with pytest.raises(ConfigError, match=message):
        load_config(config_path)
