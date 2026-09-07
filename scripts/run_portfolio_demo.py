from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg

ROOT = Path(__file__).resolve().parents[1]
PRIMARY_CONFIGS = (
    ("E-commerce", "configs/portfolio_orders.yaml"),
    ("Higher education", "configs/students_quality.yaml"),
    ("IoT telemetry", "configs/portfolio_sensor_telemetry.yaml"),
)


def invoke_cli(*arguments: str) -> str:
    """Invoke the public ETL CLI; this script contains no ETL processing logic."""
    result = subprocess.run(
        [sys.executable, "-m", "metadata_etl.cli", *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def run_config(path: str) -> dict[str, Any]:
    return json.loads(invoke_cli("run", path))


def print_run(label: str, result: dict[str, Any]) -> None:
    print(
        f"{label:<28} {result['run_id']:<30} "
        f"extracted={result['rows_extracted']:<3} "
        f"transformed={result['rows_transformed']:<3} "
        f"quarantined={result['rows_quarantined']:<3} "
        f"loaded={result['rows_loaded']:<3} status={result['status']}"
    )


def database_proof(dsn: str) -> None:
    with psycopg.connect(dsn) as connection:
        counts = connection.execute(
            """
            SELECT 'portfolio_order_lines', count(*) FROM public.portfolio_order_lines
            UNION ALL
            SELECT 'students_quality', count(*) FROM public.students_quality
            UNION ALL
            SELECT 'portfolio_sensor_telemetry', count(*)
              FROM public.portfolio_sensor_telemetry
            UNION ALL
            SELECT 'scd2_regions', count(*) FROM public.scd2_regions
            UNION ALL
            SELECT 'reliability_events', count(*) FROM public.reliability_events
            ORDER BY 1
            """
        ).fetchall()
        quality = connection.execute(
            """
            SELECT dataset, count(*) AS failed_rules, sum(records_failed) AS rule_failures
            FROM etl_observability.quality_summary
            WHERE records_failed > 0
              AND dataset IN (
                  'portfolio_order_lines', 'students_quality', 'portfolio_sensor_telemetry'
              )
            GROUP BY dataset
            ORDER BY dataset
            """
        ).fetchall()
        history = connection.execute(
            """
            SELECT count(*) AS versions,
                   count(*) FILTER (WHERE is_current) AS current_versions
            FROM public.scd2_regions
            """
        ).fetchone()
        drift = connection.execute(
            """
            SELECT drift_type, policy, action_taken
            FROM etl_observability.schema_drift_summary
            WHERE dataset = 'portfolio_schema_drift'
            ORDER BY detected_at DESC
            LIMIT 1
            """
        ).fetchone()
        watermark = connection.execute(
            """
            SELECT last_successful_value
            FROM etl_observability.watermark_status
            WHERE dataset = 'portfolio_sensor_telemetry'
            """
        ).fetchone()

    print("\nTrusted table counts")
    for table, count in counts:
        print(f"  {table:<30} {count}")
    print("\nQuality failures")
    for dataset, failed_rules, failures in quality:
        print(f"  {dataset:<30} failed_rules={failed_rules} rule_failures={failures}")
    print(f"\nSCD2 history: versions={history[0]} current_versions={history[1]}")
    print(f"Schema drift: {drift or 'no event'}")
    print(f"IoT watermark: {watermark[0] if watermark else 'none'}")


def main() -> int:
    dsn = os.getenv("ETL_POSTGRES_DSN")
    if not dsn:
        print("ETL_POSTGRES_DSN is required", file=sys.stderr)
        return 2

    invoke_cli("validate-all", "configs")
    print("All runnable configs validated.\n")

    print("Three-domain runs")
    for domain, config in PRIMARY_CONFIGS:
        first = run_config(config)
        repeat = run_config(config)
        print_run(domain, first)
        print_run(f"{domain} rerun", repeat)

    print("\nReliability proofs")
    scd2_initial = run_config("configs/scd2_regions_run1.yaml")
    scd2_change = run_config("configs/scd2_regions_run2.yaml")
    scd2_repeat = run_config("configs/scd2_regions_run2.yaml")
    print_run("SCD2 initial", scd2_initial)
    print_run("SCD2 change", scd2_change)
    print_run("SCD2 unchanged rerun", scd2_repeat)

    incremental = run_config("configs/reliability_incremental.yaml")
    print_run("Incremental normal", incremental)
    for label in ("Backfill", "Backfill rerun"):
        result = json.loads(
            invoke_cli(
                "run",
                "configs/reliability_incremental.yaml",
                "--from",
                "2026-09-02 00:00:00",
                "--to",
                "2026-09-03 23:59:59",
            )
        )
        print_run(label, result)

    drift_base = run_config("configs/portfolio_schema_drift_base.yaml")
    drift_added = run_config("configs/portfolio_schema_drift_added.yaml")
    print_run("Drift baseline", drift_base)
    print_run("Drift added column", drift_added)

    database_proof(dsn)
    print("\nCurrent health\n")
    print(invoke_cli("status"))
    print("\nRecent runs\n")
    print(invoke_cli("runs", "--limit", "10"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
