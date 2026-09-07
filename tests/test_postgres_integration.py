from __future__ import annotations

import os
import uuid
from pathlib import Path

import psycopg
import pytest
import yaml

from metadata_etl.pipeline import run_pipeline

ROOT = Path(__file__).parents[1]


@pytest.mark.skipif(
    not os.getenv("ETL_TEST_POSTGRES_DSN"),
    reason="ETL_TEST_POSTGRES_DSN is required for PostgreSQL integration verification",
)
def test_full_publish_and_ledger_against_postgres(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn = os.environ["ETL_TEST_POSTGRES_DSN"]
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("ETL_POSTGRES_DSN", dsn)
    config = yaml.safe_load((ROOT / "configs" / "customers.yaml").read_text(encoding="utf-8"))
    config["dataset"]["name"] = "ci_customers"
    config["source"]["path"] = str(ROOT / "data" / "incoming" / "customers.csv")
    config["runtime"]["raw_root"] = str(tmp_path / "raw")
    config["load"]["staging_table"] = "ci_customers_staging"
    config["load"]["target_table"] = "ci_customers"
    config_path = tmp_path / "ci_customers.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    correlation_id = f"ci-{uuid.uuid4()}"
    result = run_pipeline(config_path, correlation_id=correlation_id)

    with psycopg.connect(dsn) as connection:
        trusted = connection.execute("SELECT count(*) FROM public.ci_customers").fetchone()
        ledger = connection.execute(
            "SELECT status, rows_loaded, load_strategy, correlation_id "
            "FROM etl_meta.etl_run_ledger WHERE run_id = %s",
            (result.run_id,),
        ).fetchone()
    assert trusted == (3,)
    assert ledger == ("SUCCEEDED", 3, "full", correlation_id)
