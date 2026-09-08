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
    load_strategy TEXT,
    run_mode TEXT,
    backfill_from TEXT,
    backfill_to TEXT,
    rows_expired BIGINT,
    rows_history_inserted BIGINT,
    correlation_id TEXT,
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
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS load_strategy TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS run_mode TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS backfill_from TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS backfill_to TEXT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS rows_expired BIGINT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS rows_history_inserted BIGINT;
ALTER TABLE etl_meta.etl_run_ledger ADD COLUMN IF NOT EXISTS correlation_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS etl_run_ledger_correlation_id_uq
    ON etl_meta.etl_run_ledger (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE SCHEMA IF NOT EXISTS etl_app;

CREATE TABLE IF NOT EXISTS etl_app.upload_sessions (
    upload_id TEXT PRIMARY KEY,
    correlation_id TEXT NOT NULL UNIQUE,
    dataset TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    source_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256 TEXT NOT NULL,
    landing_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN (
        'UPLOADED', 'VALIDATING', 'READY', 'WARNING', 'BLOCKED',
        'TRIGGERING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED'
    )),
    uploaded_at TIMESTAMPTZ NOT NULL,
    validated_at TIMESTAMPTZ,
    preflight_result JSONB,
    triggered_at TIMESTAMPTZ,
    airflow_dag_id TEXT,
    airflow_run_id TEXT UNIQUE,
    airflow_state TEXT,
    etl_run_id TEXT,
    completed_at TIMESTAMPTZ,
    safe_error TEXT
);

CREATE INDEX IF NOT EXISTS upload_sessions_dataset_uploaded_idx
    ON etl_app.upload_sessions (dataset, uploaded_at DESC);

CREATE TABLE IF NOT EXISTS etl_app.onboarding_sessions (
    onboarding_id TEXT PRIMARY KEY,
    proposed_dataset_name TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL CHECK (source_type IN ('csv', 'json', 'parquet')),
    original_filename TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    sha256 TEXT NOT NULL,
    landing_key TEXT NOT NULL UNIQUE,
    draft_key TEXT UNIQUE,
    status TEXT NOT NULL CHECK (status IN (
        'UPLOADED', 'PROFILING', 'NEEDS_REVIEW',
        'REVIEW_IN_PROGRESS', 'REVIEW_COMPLETE', 'CONFIGURING',
        'READY_FOR_VALIDATION', 'VALIDATION_FAILED',
        'READY_FOR_APPROVAL', 'FAILED'
    )),
    uploaded_at TIMESTAMPTZ NOT NULL,
    profiled_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    profile_result JSONB,
    key_decisions JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_hash TEXT,
    validation_errors JSONB,
    validated_at TIMESTAMPTZ,
    safe_error TEXT
);

ALTER TABLE etl_app.onboarding_sessions
    ADD COLUMN IF NOT EXISTS validation_hash TEXT,
    ADD COLUMN IF NOT EXISTS validation_errors JSONB,
    ADD COLUMN IF NOT EXISTS validated_at TIMESTAMPTZ;

ALTER TABLE etl_app.onboarding_sessions
    DROP CONSTRAINT IF EXISTS onboarding_sessions_status_check;

ALTER TABLE etl_app.onboarding_sessions
    ADD CONSTRAINT onboarding_sessions_status_check CHECK (status IN (
        'UPLOADED', 'PROFILING', 'NEEDS_REVIEW', 'REVIEW_IN_PROGRESS',
        'REVIEW_COMPLETE', 'CONFIGURING', 'READY_FOR_VALIDATION',
        'VALIDATION_FAILED', 'READY_FOR_APPROVAL', 'FAILED'
    ));

CREATE INDEX IF NOT EXISTS onboarding_sessions_updated_idx
    ON etl_app.onboarding_sessions (updated_at DESC);

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
