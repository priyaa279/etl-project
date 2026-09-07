CREATE SCHEMA IF NOT EXISTS etl_meta;

CREATE TABLE IF NOT EXISTS etl_meta.etl_run_ledger (
    run_id TEXT PRIMARY KEY,
    dataset TEXT NOT NULL,
    config_schema_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    git_commit_sha TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    raw_path TEXT,
    raw_sha256 TEXT,
    rows_extracted BIGINT,
    rows_transformed BIGINT,
    rows_contract_passed BIGINT,
    rows_quarantined BIGINT,
    rows_loaded BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    raw_schema_hash TEXT,
    canonical_schema_hash TEXT,
    raw_schema_json TEXT,
    canonical_schema_json TEXT,
    drift_status TEXT,
    watermark_before TEXT,
    watermark_after TEXT,
    source_type TEXT,
    run_mode TEXT,
    backfill_from TEXT,
    backfill_to TEXT,
    rows_expired BIGINT,
    rows_history_inserted BIGINT,
    duration_seconds DOUBLE PRECISION,
    error_message TEXT
);

ALTER TABLE etl_meta.etl_run_ledger
    ADD COLUMN IF NOT EXISTS rows_contract_passed BIGINT;

ALTER TABLE etl_meta.etl_run_ledger
    ADD COLUMN IF NOT EXISTS rows_quarantined BIGINT;

ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS rows_inserted BIGINT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS rows_updated BIGINT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS raw_schema_hash TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS canonical_schema_hash TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS raw_schema_json TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS canonical_schema_json TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS drift_status TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS watermark_before TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS watermark_after TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS source_type TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS run_mode TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS backfill_from TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS backfill_to TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS rows_expired BIGINT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS rows_history_inserted BIGINT;

CREATE TABLE IF NOT EXISTS etl_meta.data_quality_results (
    run_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    records_checked BIGINT NOT NULL,
    records_failed BIGINT NOT NULL,
    status TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, rule_id)
);

CREATE TABLE IF NOT EXISTS etl_meta.etl_quarantine (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    failure_reason TEXT NOT NULL,
    record_identifier TEXT NOT NULL,
    failed_column TEXT,
    failed_value TEXT,
    quarantined_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_meta.schema_drift_history (
    drift_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset TEXT NOT NULL,
    schema_level TEXT NOT NULL,
    old_schema_hash TEXT NOT NULL,
    new_schema_hash TEXT NOT NULL,
    drift_type TEXT NOT NULL,
    change_description TEXT NOT NULL,
    policy TEXT NOT NULL,
    action_taken TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_meta.etl_watermarks (
    dataset TEXT PRIMARY KEY,
    watermark_column TEXT NOT NULL,
    last_successful_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    run_id TEXT NOT NULL
);
