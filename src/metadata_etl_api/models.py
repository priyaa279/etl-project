from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel

HealthStatus = Literal["HEALTHY", "WARNING", "FAILED", "UNKNOWN"]
RunStatus = Literal["SUCCEEDED", "FAILED", "RUNNING"]


class HealthResponse(BaseModel):
    status: Literal["ok"]


class OverviewResponse(BaseModel):
    time_scope_hours: int
    total_datasets: int
    healthy_datasets: int
    warning_datasets: int
    failed_datasets: int
    unknown_datasets: int
    total_recent_runs: int
    success_rate: float
    total_rows_loaded: int
    total_rows_quarantined: int
    datasets_with_drift_warnings: int


class DatasetSummary(BaseModel):
    dataset: str
    health_status: HealthStatus
    latest_run_status: RunStatus | None
    latest_run_id: str | None
    latest_run_time: datetime | None
    latest_duration_seconds: float | None
    latest_rows_extracted: int | None
    latest_rows_loaded: int | None
    latest_rows_quarantined: int | None
    latest_drift_status: str | None
    latest_watermark: str | None
    last_successful_run: datetime | None
    source_type: str | None
    load_strategy: str | None


class RunDetail(BaseModel):
    run_id: str
    dataset: str
    status: RunStatus
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float | None
    source_type: str | None
    load_strategy: str | None
    run_mode: str | None
    backfill_from: str | None
    backfill_to: str | None
    rows_extracted: int | None
    rows_transformed: int | None
    rows_contract_passed: int | None
    rows_quarantined: int | None
    rows_loaded: int | None
    rows_inserted: int | None
    rows_updated: int | None
    rows_expired: int | None
    rows_history_inserted: int | None
    raw_schema_hash: str | None
    canonical_schema_hash: str | None
    drift_status: str | None
    watermark_before: str | None
    watermark_after: str | None
    git_commit_sha: str
    config_hash: str


class QualitySummary(BaseModel):
    run_id: str
    dataset: str
    rule_id: str
    rule_type: str
    records_checked: int
    records_failed: int
    failure_rate: float
    status: str
    timestamp: datetime


class QualityBreakdown(BaseModel):
    label: str
    records_checked: int
    records_failed: int
    failure_rate: float


class QualityTrendPoint(BaseModel):
    date: date
    records_checked: int
    records_failed: int
    failure_rate: float
    runs_affected: int


class QualityOverview(BaseModel):
    time_scope_days: int
    total_records_checked: int
    total_records_failed: int
    failure_rate: float
    rows_quarantined: int
    by_dataset: list[QualityBreakdown]
    by_rule: list[QualityBreakdown]
    by_rule_type: list[QualityBreakdown]
    recent_trend: list[QualityTrendPoint]


class SchemaDriftEvent(BaseModel):
    dataset: str
    run_id: str
    schema_level: str
    drift_type: str
    policy: str
    action_taken: str
    detected_at: datetime
    old_schema_hash: str
    new_schema_hash: str


class WatermarkState(BaseModel):
    dataset: str
    watermark_column: str
    last_successful_value: str
    updated_at: datetime
    run_id: str


class DatasetWatermarkResponse(BaseModel):
    dataset: str
    watermark: WatermarkState | None
