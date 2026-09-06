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
    duration_seconds DOUBLE PRECISION,
    error_message TEXT
);

ALTER TABLE etl_meta.etl_run_ledger
    ADD COLUMN IF NOT EXISTS rows_contract_passed BIGINT;

ALTER TABLE etl_meta.etl_run_ledger
    ADD COLUMN IF NOT EXISTS rows_quarantined BIGINT;

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
