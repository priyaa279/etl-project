from pathlib import Path

import duckdb

from metadata_etl.config import load_config
from metadata_etl.sql_compiler import compile_transform_sql

ROOT = Path(__file__).parents[1]


def test_compiled_plan_preserves_ids_and_applies_transforms(monkeypatch) -> None:
    monkeypatch.chdir(ROOT)
    config = load_config(ROOT / "configs" / "customers.yaml")

    with duckdb.connect(":memory:") as connection:
        cursor = connection.execute(compile_transform_sql(config))
        rows = cursor.fetchall()
        names = [item[0] for item in cursor.description]

    assert len(rows) == 3
    assert names[-1] == "customer_label"
    assert rows[0][names.index("customer_id")] == "00123"
    assert rows[0][-1] == "00123 - Ada Lovelace"
