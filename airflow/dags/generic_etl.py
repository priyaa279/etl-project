from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from metadata_etl.orchestration import discover_scheduled_configs, run_cli_stage

CONFIG_DIR = Path(os.getenv("ETL_CONFIG_DIR", "/opt/airflow/configs"))


def run_etl_from_dag_run(config_path: str, dag_run=None) -> None:
    """Forward optional upload context without relying on Jinja rendering."""
    conf = dag_run.conf if dag_run is not None else {}
    run_cli_stage(
        "run",
        config_path,
        source_override=conf.get("source_override"),
        correlation_id=conf.get("correlation_id"),
        git_commit_sha=conf.get("git_commit_sha"),
    )


def build_dag(spec):
    dag = DAG(
        dag_id=spec.dag_id,
        description=f"Config-driven ETL for {spec.dataset}",
        schedule=spec.schedule,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        tags=["metadata-etl", spec.dataset],
    )
    with dag:
        validate = PythonOperator(
            task_id="validate_config",
            python_callable=run_cli_stage,
            op_args=["validate", str(spec.path)],
            retries=0,
        )
        run = PythonOperator(
            task_id="run_etl",
            python_callable=run_etl_from_dag_run,
            op_args=[str(spec.path)],
            retries=spec.retries,
            retry_delay=timedelta(minutes=spec.retry_delay_minutes),
        )
        validate >> run
    return dag


for scheduled_config in discover_scheduled_configs(CONFIG_DIR):
    globals()[scheduled_config.dag_id] = build_dag(scheduled_config)
