CREATE SCHEMA IF NOT EXISTS etl_observability;

CREATE INDEX IF NOT EXISTS idx_etl_run_ledger_dataset_started_at
    ON etl_meta.etl_run_ledger (dataset, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_run_ledger_started_at
    ON etl_meta.etl_run_ledger (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_quality_dataset_timestamp
    ON etl_meta.data_quality_results (dataset, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_quarantine_dataset_time_rule
    ON etl_meta.etl_quarantine (dataset, quarantined_at DESC, rule_id);

CREATE INDEX IF NOT EXISTS idx_drift_dataset_detected_at
    ON etl_meta.schema_drift_history (dataset, detected_at DESC);

CREATE OR REPLACE VIEW etl_observability.pipeline_runs AS
SELECT
    run_id,
    dataset,
    status,
    started_at,
    finished_at AS completed_at,
    duration_seconds,
    source_type,
    load_strategy,
    run_mode,
    backfill_from,
    backfill_to,
    rows_extracted,
    rows_transformed,
    rows_contract_passed,
    rows_quarantined,
    rows_loaded,
    rows_inserted,
    rows_updated,
    rows_expired,
    rows_history_inserted,
    CASE
        WHEN duration_seconds > 0
            THEN rows_loaded::DOUBLE PRECISION / duration_seconds
        ELSE NULL
    END AS rows_loaded_per_second,
    raw_schema_hash,
    canonical_schema_hash,
    drift_status,
    watermark_before,
    watermark_after,
    git_commit_sha,
    config_hash,
    correlation_id
FROM etl_meta.etl_run_ledger;

CREATE OR REPLACE VIEW etl_observability.dataset_health AS
WITH datasets AS (
    SELECT dataset FROM etl_meta.etl_run_ledger
    UNION
    SELECT dataset FROM etl_meta.data_quality_results
    UNION
    SELECT dataset FROM etl_meta.etl_quarantine
    UNION
    SELECT dataset FROM etl_meta.schema_drift_history
    UNION
    SELECT dataset FROM etl_meta.etl_watermarks
),
ranked_runs AS (
    SELECT
        ledger.*,
        ROW_NUMBER() OVER (
            PARTITION BY dataset
            ORDER BY started_at DESC, run_id DESC
        ) AS run_rank,
        COUNT(*) FILTER (WHERE status <> 'FAILED') OVER (
            PARTITION BY dataset
            ORDER BY started_at DESC, run_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS newer_nonfailure_count
    FROM etl_meta.etl_run_ledger AS ledger
),
latest_runs AS (
    SELECT * FROM ranked_runs WHERE run_rank = 1
),
last_successes AS (
    SELECT
        dataset,
        MAX(COALESCE(finished_at, started_at)) AS last_successful_run
    FROM etl_meta.etl_run_ledger
    WHERE status = 'SUCCEEDED'
    GROUP BY dataset
),
failure_streaks AS (
    SELECT dataset, COUNT(*)::BIGINT AS consecutive_failure_count
    FROM ranked_runs
    WHERE status = 'FAILED' AND newer_nonfailure_count = 0
    GROUP BY dataset
)
SELECT
    datasets.dataset,
    latest.run_id AS latest_run_id,
    latest.status AS latest_run_status,
    latest.started_at AS latest_run_time,
    latest.duration_seconds AS latest_duration_seconds,
    latest.rows_extracted AS latest_rows_extracted,
    latest.rows_loaded AS latest_rows_loaded,
    latest.rows_quarantined AS latest_rows_quarantined,
    latest.drift_status AS latest_drift_status,
    watermarks.last_successful_value AS latest_watermark,
    successes.last_successful_run,
    COALESCE(streaks.consecutive_failure_count, 0)::BIGINT AS consecutive_failure_count,
    CASE
        WHEN successes.last_successful_run IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - successes.last_successful_run)) / 3600.0
    END AS hours_since_last_success,
    CASE
        WHEN latest.run_id IS NULL THEN 'UNKNOWN'
        WHEN latest.status = 'FAILED' THEN 'FAILED'
        WHEN latest.status = 'SUCCEEDED'
             AND (
                 COALESCE(latest.rows_quarantined, 0) > 0
                 OR latest.drift_status = 'WARN'
             ) THEN 'WARNING'
        WHEN latest.status = 'SUCCEEDED' THEN 'HEALTHY'
        ELSE 'UNKNOWN'
    END AS health_status
FROM datasets
LEFT JOIN latest_runs AS latest ON latest.dataset = datasets.dataset
LEFT JOIN last_successes AS successes ON successes.dataset = datasets.dataset
LEFT JOIN failure_streaks AS streaks ON streaks.dataset = datasets.dataset
LEFT JOIN etl_meta.etl_watermarks AS watermarks ON watermarks.dataset = datasets.dataset;

CREATE OR REPLACE VIEW etl_observability.daily_pipeline_summary AS
SELECT
    started_at::DATE AS run_date,
    dataset,
    COUNT(*)::BIGINT AS total_runs,
    COUNT(*) FILTER (WHERE status = 'SUCCEEDED')::BIGINT AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'FAILED')::BIGINT AS failed_runs,
    COALESCE(
        ROUND(
            COUNT(*) FILTER (WHERE status = 'SUCCEEDED')::NUMERIC * 100.0
            / NULLIF(COUNT(*), 0),
            2
        ),
        0
    ) AS success_rate,
    SUM(COALESCE(rows_extracted, 0))::BIGINT AS rows_extracted,
    SUM(COALESCE(rows_loaded, 0))::BIGINT AS rows_loaded,
    SUM(COALESCE(rows_quarantined, 0))::BIGINT AS rows_quarantined,
    AVG(duration_seconds) AS average_duration_seconds,
    MAX(duration_seconds) AS maximum_duration_seconds
FROM etl_meta.etl_run_ledger
GROUP BY started_at::DATE, dataset;

CREATE OR REPLACE VIEW etl_observability.quality_summary AS
SELECT
    run_id,
    dataset,
    rule_id,
    rule_type,
    records_checked,
    records_failed,
    CASE
        WHEN records_checked = 0 THEN 0::NUMERIC
        ELSE ROUND(records_failed::NUMERIC * 100.0 / records_checked, 2)
    END AS failure_rate,
    status,
    timestamp
FROM etl_meta.data_quality_results;

CREATE OR REPLACE VIEW etl_observability.quality_daily_trend AS
SELECT
    timestamp::DATE AS date,
    dataset,
    rule_id,
    rule_type,
    SUM(records_checked)::BIGINT AS records_checked,
    SUM(records_failed)::BIGINT AS records_failed,
    CASE
        WHEN SUM(records_checked) = 0 THEN 0::NUMERIC
        ELSE ROUND(SUM(records_failed)::NUMERIC * 100.0 / SUM(records_checked), 2)
    END AS failure_rate,
    COUNT(DISTINCT run_id) FILTER (WHERE records_failed > 0)::BIGINT AS runs_affected
FROM etl_meta.data_quality_results
GROUP BY timestamp::DATE, dataset, rule_id, rule_type;

CREATE OR REPLACE VIEW etl_observability.quarantine_summary AS
SELECT
    quarantined_at::DATE AS date,
    dataset,
    rule_id,
    rule_type,
    COUNT(*)::BIGINT AS failure_count,
    COUNT(DISTINCT record_identifier)::BIGINT AS affected_records
FROM etl_meta.etl_quarantine
GROUP BY quarantined_at::DATE, dataset, rule_id, rule_type;

CREATE OR REPLACE VIEW etl_observability.schema_drift_summary AS
SELECT
    dataset,
    run_id,
    schema_level,
    drift_type,
    policy,
    action_taken,
    detected_at,
    old_schema_hash,
    new_schema_hash
FROM etl_meta.schema_drift_history;

CREATE OR REPLACE VIEW etl_observability.watermark_status AS
SELECT
    dataset,
    watermark_column,
    last_successful_value,
    updated_at,
    run_id
FROM etl_meta.etl_watermarks;
