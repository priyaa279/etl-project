from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

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
    correlation_id: str | None = None


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


class DatasetUploadCapability(BaseModel):
    dataset: str
    source_type: str
    load_strategy: str
    config_approved: bool
    upload_eligible: bool
    reason: str | None


class OperationRunSummary(BaseModel):
    run_id: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float | None
    rows_extracted: int | None
    rows_transformed: int | None
    rows_contract_passed: int | None
    rows_quarantined: int | None
    rows_loaded: int | None
    drift_status: str | None


class UploadOperation(BaseModel):
    upload_id: str
    dataset: str
    original_filename: str
    source_type: str
    size_bytes: int
    sha256: str
    status: str
    uploaded_at: datetime
    validated_at: datetime | None = None
    triggered_at: datetime | None = None
    airflow_dag_id: str | None = None
    airflow_run_id: str | None = None
    airflow_state: str | None = None
    etl_run_id: str | None = None
    completed_at: datetime | None = None
    safe_error: str | None = None
    preflight_result: dict[str, object] | None = None
    etl_run: OperationRunSummary | None = None


class OnboardingCapability(BaseModel):
    enabled: bool
    source_types: list[str]
    supported_datatypes: list[str]
    reason: str | None


class OnboardingProfileSummary(BaseModel):
    rows_scanned: int | None
    rows_profiled: int | None
    exact_duplicate_count: int | None
    column_count: int
    possible_key_candidates: int
    required_decisions: int
    nested_structure_detected: bool


class OnboardingSession(BaseModel):
    onboarding_id: str
    proposed_dataset_name: str
    source_type: Literal["csv", "json", "parquet"]
    original_filename: str
    size_bytes: int
    sha256: str
    status: str
    uploaded_at: datetime
    profiled_at: datetime | None
    updated_at: datetime
    safe_error: str | None
    profile_summary: OnboardingProfileSummary | None
    approved_at: datetime | None = None
    approved_by: str | None = None
    git_commit_sha: str | None = None
    dag_id: str | None = None
    first_run_correlation_id: str | None = None
    airflow_run_id: str | None = None
    etl_run_id: str | None = None
    completed_at: datetime | None = None


class SchemaDecision(BaseModel):
    canonical_name: str
    datatype: str
    nullable: bool
    format: str | None = None


class KeyCandidateDecision(BaseModel):
    field: str
    decision: Literal["accepted", "rejected"]


class OnboardingReview(BaseModel):
    onboarding_id: str
    dataset: str
    source_type: str
    original_filename: str
    size_bytes: int
    status: str
    rows_scanned: int | None
    rows_profiled: int | None
    exact_duplicate_count: int | None
    column_count: int
    nested_fields: list[str]
    fields: list[dict[str, Any]]
    unresolved: list[dict[str, str]]
    progress: dict[str, int]
    final_approved: bool


class DraftYAML(BaseModel):
    onboarding_id: str
    yaml: str


class MoveDirection(BaseModel):
    direction: Literal["up", "down"]


class ColumnPrivacyDecision(BaseModel):
    classification: str | None = None
    quarantine_value: Literal["full", "masked", "hashed", "none"]


class OnboardingConfiguration(BaseModel):
    onboarding_id: str
    dataset: str
    source_type: str
    status: str
    review_complete: bool
    normalization_complete: bool
    activation_ready: bool
    columns: list[dict[str, Any]]
    post_transformation_columns: list[dict[str, str]]
    accepted_key_candidates: list[str]
    normalization: dict[str, Any] | None
    transformations: list[dict[str, Any]]
    contracts: list[dict[str, Any]]
    load: dict[str, Any]
    schema_drift: dict[str, str]
    orchestration: dict[str, Any]
    validation: dict[str, Any]
    final_approved: bool
    approval: dict[str, Any]


class ApprovalRequest(BaseModel):
    expected_hash: str
    approved_by: str
    acknowledged: bool


class FirstRunRequest(BaseModel):
    retry: bool = False


class OnboardingCompletion(BaseModel):
    onboarding_id: str
    dataset: str
    status: str
    safe_error: str | None
    approval: dict[str, Any]
    first_run: dict[str, Any]
