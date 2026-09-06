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
    rows_loaded BIGINT,
    duration_seconds DOUBLE PRECISION,
    error_message TEXT
);

