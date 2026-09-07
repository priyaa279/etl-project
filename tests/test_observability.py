from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from metadata_etl.cli import main
from metadata_etl.observability import (
    OBSERVABILITY_VIEW_NAMES,
    MonitoringStore,
)
from metadata_etl.postgres import PostgresStore

pytestmark = pytest.mark.skipif(
    not os.getenv("ETL_TEST_POSTGRES_DSN"),
    reason="ETL_TEST_POSTGRES_DSN is required for observability integration tests",
)


@pytest.fixture(scope="module")
def observability_data() -> dict[str, Any]:
    dsn = os.environ["ETL_TEST_POSTGRES_DSN"]
    token = uuid.uuid4().hex[:10]
    datasets = {
        name: f"obs_{name}_{token}"
        for name in ("healthy", "quarantine", "drift", "failed", "daily", "zero", "unknown")
    }
    base = datetime(2026, 9, 7, tzinfo=UTC)

    with PostgresStore(dsn) as store:
        store.ensure_metadata_tables()
        store.ensure_metadata_tables()

    runs = [
        ("healthy_1", "healthy", "SUCCEEDED", 0, "NONE", 10.0, 10, 10),
        ("quarantine_1", "quarantine", "SUCCEEDED", 2, "NONE", 12.0, 12, 10),
        ("drift_1", "drift", "SUCCEEDED", 0, "WARN", 14.0, 14, 14),
        ("failed_success", "failed", "SUCCEEDED", 0, "NONE", 5.0, 5, 5),
        ("failed_1", "failed", "FAILED", 0, "NONE", 6.0, 6, 0),
        ("failed_2", "failed", "FAILED", 0, "NONE", 7.0, 7, 0),
        ("daily_1", "daily", "SUCCEEDED", 0, "NONE", 10.0, 10, 8),
        ("daily_2", "daily", "FAILED", 0, "NONE", 20.0, 20, 0),
        ("daily_3", "daily", "SUCCEEDED", 3, "NONE", 30.0, 30, 27),
        ("zero_1", "zero", "SUCCEEDED", 0, "NONE", 0.0, 1, 1),
    ]
    run_ids: list[str] = []
    with psycopg.connect(dsn) as connection:
        for index, (
            suffix,
            dataset_key,
            status,
            quarantined,
            drift,
            duration,
            extracted,
            loaded,
        ) in enumerate(runs):
            run_id = f"OBS_{token}_{suffix}"
            run_ids.append(run_id)
            started = base + timedelta(minutes=index)
            connection.execute(
                """
                INSERT INTO etl_meta.etl_run_ledger (
                    run_id, dataset, config_schema_version, config_hash, git_commit_sha,
                    status, started_at, finished_at, duration_seconds, source_type,
                    load_strategy, run_mode, rows_extracted, rows_transformed,
                    rows_contract_passed, rows_quarantined, rows_loaded, rows_inserted,
                    rows_updated, rows_expired, rows_history_inserted, raw_schema_hash,
                    canonical_schema_hash, drift_status
                ) VALUES (
                    %s, %s, '1.0', 'test-config-hash', 'test-git-sha', %s, %s, %s, %s,
                    'csv', 'full', 'normal', %s, %s, %s, %s, %s, %s, 0, 0, 0,
                    'raw-hash', 'canonical-hash', %s
                )
                """,
                (
                    run_id,
                    datasets[dataset_key],
                    status,
                    started,
                    started + timedelta(seconds=duration),
                    duration,
                    extracted,
                    extracted,
                    extracted - quarantined,
                    quarantined,
                    loaded,
                    loaded,
                    drift,
                ),
            )

        connection.execute(
            """
            INSERT INTO etl_meta.etl_watermarks (
                dataset, watermark_column, last_successful_value, updated_at, run_id
            ) VALUES (%s, 'updated_at', '2026-09-07T00:00:00Z', %s, 'NO_RUN')
            """,
            (datasets["unknown"], base),
        )
        connection.execute(
            """
            INSERT INTO etl_meta.etl_watermarks (
                dataset, watermark_column, last_successful_value, updated_at, run_id
            ) VALUES (%s, 'updated_at', '2026-09-07T00:10:00Z', %s, %s)
            """,
            (datasets["healthy"], base, run_ids[0]),
        )
        connection.execute(
            """
            INSERT INTO etl_meta.data_quality_results (
                run_id, dataset, rule_id, rule_type, records_checked,
                records_failed, status, timestamp
            ) VALUES
                (%s, %s, 'DQ_TEST', 'range', 10, 2, 'FAILED', %s),
                (%s, %s, 'DQ_TEST', 'range', 0, 0, 'PASSED', %s)
            """,
            (
                run_ids[1],
                datasets["quarantine"],
                base,
                run_ids[8],
                datasets["quarantine"],
                base + timedelta(hours=1),
            ),
        )
        for identifier, failed_value in (
            ("record-a", "raw-secret"),
            ("record-a", "masked"),
            ("record-b", "hash"),
        ):
            connection.execute(
                """
                INSERT INTO etl_meta.etl_quarantine (
                    run_id, dataset, rule_id, rule_type, failure_reason,
                    record_identifier, failed_column, failed_value, quarantined_at
                ) VALUES (%s, %s, 'DQ_TEST', 'range', 'outside range', %s, 'score', %s, %s)
                """,
                (run_ids[1], datasets["quarantine"], identifier, failed_value, base),
            )
        connection.execute(
            """
            INSERT INTO etl_meta.schema_drift_history (
                run_id, dataset, schema_level, old_schema_hash, new_schema_hash,
                drift_type, change_description, policy, action_taken, detected_at
            ) VALUES (%s, %s, 'raw', 'old-hash', 'new-hash', 'column_added',
                      'test change', 'warn', 'WARNED', %s)
            """,
            (run_ids[2], datasets["drift"], base),
        )

    values: dict[str, Any] = {
        "dsn": dsn,
        "datasets": datasets,
        "run_ids": run_ids,
        "base": base,
    }
    yield values

    dataset_values = list(datasets.values())
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "DELETE FROM etl_meta.etl_quarantine WHERE dataset = ANY(%s)",
            (dataset_values,),
        )
        connection.execute(
            "DELETE FROM etl_meta.data_quality_results WHERE dataset = ANY(%s)",
            (dataset_values,),
        )
        connection.execute(
            "DELETE FROM etl_meta.schema_drift_history WHERE dataset = ANY(%s)",
            (dataset_values,),
        )
        connection.execute(
            "DELETE FROM etl_meta.etl_watermarks WHERE dataset = ANY(%s)",
            (dataset_values,),
        )
        connection.execute(
            "DELETE FROM etl_meta.etl_run_ledger WHERE dataset = ANY(%s)",
            (dataset_values,),
        )


def test_observability_installation_is_complete_and_idempotent(
    observability_data: dict[str, Any],
) -> None:
    with psycopg.connect(observability_data["dsn"]) as connection:
        schema = connection.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = 'etl_observability'"
        ).fetchone()
        views = connection.execute(
            "SELECT table_name FROM information_schema.views "
            "WHERE table_schema = 'etl_observability'"
        ).fetchall()

    assert schema == ("etl_observability",)
    assert {row[0] for row in views} == set(OBSERVABILITY_VIEW_NAMES)


def test_pipeline_runs_exposes_expected_fields_and_safe_throughput(
    observability_data: dict[str, Any],
) -> None:
    datasets = observability_data["datasets"]
    with psycopg.connect(observability_data["dsn"]) as connection:
        healthy = connection.execute(
            "SELECT source_type, load_strategy, run_mode, rows_extracted, rows_loaded, "
            "rows_loaded_per_second, config_hash, git_commit_sha "
            "FROM etl_observability.pipeline_runs WHERE dataset = %s",
            (datasets["healthy"],),
        ).fetchone()
        zero = connection.execute(
            "SELECT rows_loaded_per_second FROM etl_observability.pipeline_runs WHERE dataset = %s",
            (datasets["zero"],),
        ).fetchone()

    assert healthy == ("csv", "full", "normal", 10, 10, 1.0, "test-config-hash", "test-git-sha")
    assert zero == (None,)


def test_dataset_health_states_latest_run_and_failure_streak(
    observability_data: dict[str, Any],
) -> None:
    datasets = observability_data["datasets"]
    with MonitoringStore(observability_data["dsn"]) as monitoring:
        healthy = monitoring.status(datasets["healthy"])[0]
        quarantine = monitoring.status(datasets["quarantine"])[0]
        drift = monitoring.status(datasets["drift"])[0]
        failed = monitoring.status(datasets["failed"])[0]
        unknown = monitoring.status(datasets["unknown"])[0]

    assert healthy["health_status"] == "HEALTHY"
    assert healthy["latest_watermark"] == "2026-09-07T00:10:00Z"
    assert quarantine["latest_run_status"] == "SUCCEEDED"
    assert quarantine["health_status"] == "WARNING"
    assert drift["health_status"] == "WARNING"
    assert failed["latest_run_id"].endswith("failed_2")
    assert failed["latest_run_status"] == "FAILED"
    assert failed["health_status"] == "FAILED"
    assert failed["consecutive_failure_count"] == 2
    assert failed["last_successful_run"] is not None
    assert unknown["latest_run_status"] is None
    assert unknown["health_status"] == "UNKNOWN"


def test_daily_pipeline_summary_counts_rates_and_durations(
    observability_data: dict[str, Any],
) -> None:
    dataset = observability_data["datasets"]["daily"]
    with psycopg.connect(observability_data["dsn"]) as connection:
        row = connection.execute(
            """
            SELECT total_runs, successful_runs, failed_runs, success_rate,
                   rows_extracted, rows_loaded, rows_quarantined,
                   average_duration_seconds, maximum_duration_seconds
            FROM etl_observability.daily_pipeline_summary
            WHERE dataset = %s
            """,
            (dataset,),
        ).fetchone()

    assert row == (3, 2, 1, Decimal("66.67"), 60, 35, 3, 20.0, 30.0)


def test_quality_views_aggregate_and_handle_zero_checked_rows(
    observability_data: dict[str, Any],
) -> None:
    dataset = observability_data["datasets"]["quarantine"]
    with psycopg.connect(observability_data["dsn"]) as connection:
        summaries = connection.execute(
            "SELECT records_checked, records_failed, failure_rate "
            "FROM etl_observability.quality_summary WHERE dataset = %s "
            "ORDER BY records_checked",
            (dataset,),
        ).fetchall()
        trend = connection.execute(
            "SELECT records_checked, records_failed, failure_rate, runs_affected "
            "FROM etl_observability.quality_daily_trend WHERE dataset = %s",
            (dataset,),
        ).fetchone()

    assert summaries == [(0, 0, 0), (10, 2, 20.00)]
    assert trend == (10, 2, 20.00, 1)


def test_quarantine_view_is_aggregate_only_and_privacy_safe(
    observability_data: dict[str, Any],
) -> None:
    dataset = observability_data["datasets"]["quarantine"]
    with psycopg.connect(observability_data["dsn"]) as connection:
        columns = connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'etl_observability' AND table_name = 'quarantine_summary'",
        ).fetchall()
        summary = connection.execute(
            "SELECT failure_count, affected_records "
            "FROM etl_observability.quarantine_summary WHERE dataset = %s",
            (dataset,),
        ).fetchone()

    names = {row[0] for row in columns}
    assert summary == (3, 2)
    assert names == {"date", "dataset", "rule_id", "rule_type", "failure_count", "affected_records"}
    assert names.isdisjoint(
        {"failed_value", "record_identifier", "failure_reason", "failed_column"}
    )


def test_schema_drift_and_watermark_views_surface_current_metadata(
    observability_data: dict[str, Any],
) -> None:
    datasets = observability_data["datasets"]
    with psycopg.connect(observability_data["dsn"]) as connection:
        drift = connection.execute(
            "SELECT schema_level, drift_type, policy, action_taken, old_schema_hash, new_schema_hash "
            "FROM etl_observability.schema_drift_summary WHERE dataset = %s",
            (datasets["drift"],),
        ).fetchone()
        watermark = connection.execute(
            "SELECT watermark_column, last_successful_value, run_id "
            "FROM etl_observability.watermark_status WHERE dataset = %s",
            (datasets["healthy"],),
        ).fetchone()

    assert drift == ("raw", "column_added", "warn", "WARNED", "old-hash", "new-hash")
    assert watermark == ("updated_at", "2026-09-07T00:10:00Z", observability_data["run_ids"][0])


def test_monitoring_cli_status_runs_limit_and_missing_dataset(
    observability_data: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    datasets = observability_data["datasets"]
    monkeypatch.setenv("ETL_POSTGRES_DSN", observability_data["dsn"])

    assert main(["observability-install"]) == 0
    assert "installed" in capsys.readouterr().out

    assert main(["status"]) == 0
    assert datasets["healthy"] in capsys.readouterr().out

    assert main(["status", "--dataset", datasets["quarantine"]]) == 0
    detail = capsys.readouterr().out
    assert "WARNING" in detail
    assert "latest_run_id" in detail
    assert "last_successful_run" in detail

    assert main(["runs", "--dataset", datasets["daily"], "--limit", "1"]) == 0
    recent = capsys.readouterr().out
    assert recent.count(f"OBS_{observability_data['run_ids'][0].split('_')[1]}_daily_") == 1

    assert main(["status", "--dataset", f"missing_{uuid.uuid4().hex}"]) == 2
    assert "was not found" in capsys.readouterr().err
