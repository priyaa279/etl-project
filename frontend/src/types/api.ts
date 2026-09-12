export type HealthStatus = "HEALTHY" | "WARNING" | "FAILED" | "UNKNOWN";
export type RunStatus = "SUCCEEDED" | "FAILED" | "RUNNING";

export interface Overview {
  time_scope_hours: number;
  total_datasets: number;
  healthy_datasets: number;
  warning_datasets: number;
  failed_datasets: number;
  unknown_datasets: number;
  total_recent_runs: number;
  success_rate: number;
  total_rows_loaded: number;
  total_rows_quarantined: number;
  datasets_with_drift_warnings: number;
}

export interface DatasetSummary {
  dataset: string;
  health_status: HealthStatus;
  latest_run_status: RunStatus | null;
  latest_run_id: string | null;
  latest_run_time: string | null;
  latest_duration_seconds: number | null;
  latest_rows_extracted: number | null;
  latest_rows_loaded: number | null;
  latest_rows_quarantined: number | null;
  latest_drift_status: string | null;
  latest_watermark: string | null;
  last_successful_run: string | null;
  source_type: string | null;
  load_strategy: string | null;
}

export interface RunDetail {
  run_id: string;
  dataset: string;
  status: RunStatus;
  started_at: string;
  completed_at: string | null;
  duration_seconds: number | null;
  source_type: string | null;
  load_strategy: string | null;
  run_mode: string | null;
  backfill_from: string | null;
  backfill_to: string | null;
  rows_extracted: number | null;
  rows_transformed: number | null;
  rows_contract_passed: number | null;
  rows_quarantined: number | null;
  rows_loaded: number | null;
  rows_inserted: number | null;
  rows_updated: number | null;
  rows_expired: number | null;
  rows_history_inserted: number | null;
  raw_schema_hash: string | null;
  canonical_schema_hash: string | null;
  drift_status: string | null;
  watermark_before: string | null;
  watermark_after: string | null;
  git_commit_sha: string;
  config_hash: string;
  correlation_id?: string | null;
}

export type RunSortField =
  | "run_id"
  | "dataset"
  | "status"
  | "started_at"
  | "duration_seconds"
  | "source_type"
  | "load_strategy"
  | "rows_loaded"
  | "rows_quarantined";

export type QualitySortField =
  | "dataset"
  | "rule_id"
  | "rule_type"
  | "records_checked"
  | "records_failed"
  | "failure_rate"
  | "status"
  | "timestamp";

export type SchemaDriftSortField =
  | "dataset"
  | "schema_level"
  | "drift_type"
  | "policy"
  | "action_taken"
  | "detected_at";

export interface QualitySummary {
  run_id: string;
  dataset: string;
  rule_id: string;
  rule_type: string;
  records_checked: number;
  records_failed: number;
  failure_rate: number;
  status: string;
  timestamp: string;
}

export interface QualityBreakdown {
  label: string;
  records_checked: number;
  records_failed: number;
  failure_rate: number;
}

export interface QualityTrendPoint {
  date: string;
  records_checked: number;
  records_failed: number;
  failure_rate: number;
  runs_affected: number;
}

export interface QualityOverview {
  time_scope_days: number;
  total_records_checked: number;
  total_records_failed: number;
  failure_rate: number;
  rows_quarantined: number;
  by_dataset: QualityBreakdown[];
  by_rule: QualityBreakdown[];
  by_rule_type: QualityBreakdown[];
  recent_trend: QualityTrendPoint[];
}

export interface SchemaDriftEvent {
  dataset: string;
  run_id: string;
  schema_level: string;
  drift_type: string;
  policy: string;
  action_taken: string;
  detected_at: string;
  old_schema_hash: string;
  new_schema_hash: string;
}

export interface WatermarkState {
  dataset: string;
  watermark_column: string;
  last_successful_value: string;
  updated_at: string;
  run_id: string;
}

export interface DatasetWatermarkResponse {
  dataset: string;
  watermark: WatermarkState | null;
}

export interface TrustedDataColumn {
  name: string;
  data_type: string;
  classification: string | null;
  redacted: boolean;
}

export interface TrustedDataPreview {
  dataset: string;
  target: string;
  state: "AVAILABLE" | "EMPTY" | "NOT_PUBLISHED";
  columns: TrustedDataColumn[];
  rows: Record<string, unknown>[];
  total_rows: number;
  limit: number;
  offset: number;
  has_more: boolean;
  sort: string | null;
  direction: "asc" | "desc";
  latest_successful_run_id: string | null;
  last_updated: string | null;
}

export interface PipelineTransformation {
  position: number;
  id: string;
  type: "cast" | "filter" | "derive" | "map" | "deduplicate";
  details: Record<string, unknown>;
}

export interface PipelineSummary {
  dataset: string;
  label: "Current Transformation Plan";
  transformations: PipelineTransformation[];
}

export interface DatasetUploadCapability {
  dataset: string;
  source_type: string;
  load_strategy: string;
  config_approved: boolean;
  upload_eligible: boolean;
  reason: string | null;
}

export interface PreflightEvent {
  type: string;
  description: string;
  policy: string;
  action: string;
}

export interface PreflightResult {
  status: "READY" | "WARNING" | "BLOCKED";
  config_approved: boolean;
  source_valid: boolean;
  canonical_compatible: boolean;
  message?: string;
  schema?: {
    raw_hash: string;
    canonical_hash: string;
    fields: { name: string; datatype: string }[];
  };
  drift: {
    detected: boolean;
    status: string;
    events: PreflightEvent[];
  };
}

export interface OperationRunSummary {
  run_id: string;
  status: string;
  started_at: string;
  completed_at: string | null;
  duration_seconds: number | null;
  rows_extracted: number | null;
  rows_transformed: number | null;
  rows_contract_passed: number | null;
  rows_quarantined: number | null;
  rows_loaded: number | null;
  drift_status: string | null;
}

export interface UploadOperation {
  upload_id: string;
  dataset: string;
  original_filename: string;
  source_type: string;
  size_bytes: number;
  sha256: string;
  status: string;
  uploaded_at: string;
  validated_at: string | null;
  triggered_at: string | null;
  airflow_dag_id: string | null;
  airflow_run_id: string | null;
  airflow_state: string | null;
  etl_run_id: string | null;
  completed_at: string | null;
  safe_error: string | null;
  preflight_result: PreflightResult | null;
  etl_run: OperationRunSummary | null;
}

export interface OnboardingCapability {
  enabled: boolean;
  source_types: ("csv" | "json" | "parquet")[];
  supported_datatypes: string[];
  reason: string | null;
}

export interface OnboardingProfileSummary {
  rows_scanned: number | null;
  rows_profiled: number | null;
  exact_duplicate_count: number | null;
  column_count: number;
  possible_key_candidates: number;
  required_decisions: number;
  nested_structure_detected: boolean;
}

export interface OnboardingSession {
  onboarding_id: string;
  proposed_dataset_name: string;
  source_type: "csv" | "json" | "parquet";
  original_filename: string;
  size_bytes: number;
  sha256: string;
  status: string;
  uploaded_at: string;
  profiled_at: string | null;
  updated_at: string;
  safe_error: string | null;
  profile_summary: OnboardingProfileSummary | null;
}

export interface OnboardingField {
  source_name: string;
  canonical_name: string;
  datatype: string;
  nullable: boolean;
  format: string | null;
  confidence: string | null;
  reason: string | null;
  review_required: boolean;
  review_resolved: boolean;
  review_reasons: string[];
  profile: {
    null_percentage?: number;
    distinct_count?: number;
    cardinality_ratio?: number;
    leading_zeros_detected?: boolean;
    possible_key_candidate?: boolean;
    observed_examples?: string[];
  };
  key_candidate: boolean;
  key_decision: "accepted" | "rejected" | null;
  editable: boolean;
}

export interface OnboardingReview {
  onboarding_id: string;
  dataset: string;
  source_type: string;
  original_filename: string;
  size_bytes: number;
  status: string;
  rows_scanned: number | null;
  rows_profiled: number | null;
  exact_duplicate_count: number | null;
  column_count: number;
  nested_fields: string[];
  fields: OnboardingField[];
  unresolved: { kind: string; field: string; reason: string }[];
  progress: { reviewed: number; total: number; remaining: number };
  final_approved: false;
}

export interface DraftYAML {
  onboarding_id: string;
  yaml: string;
}

export interface SchemaDecision {
  canonical_name: string;
  datatype: string;
  nullable: boolean;
  format: string | null;
}

export interface ConfigurationColumn {
  name: string;
  source: string;
  datatype: string;
  nullable: boolean;
  format: string | null;
  classification: string | null;
  quarantine_value: "full" | "masked" | "hashed" | "none";
}

export interface ConfigurationValidation {
  result: "VALID" | "INVALID" | "NOT_VALIDATED";
  draft_hash: string;
  validated_hash: string | null;
  validated_at: string | null;
  errors: { section: string; field?: string; message: string }[];
}

export interface OnboardingConfiguration {
  onboarding_id: string;
  dataset: string;
  source_type: string;
  status: string;
  review_complete: boolean;
  normalization_complete: boolean;
  activation_ready: boolean;
  columns: ConfigurationColumn[];
  post_transformation_columns: { name: string; datatype: string }[];
  accepted_key_candidates: string[];
  normalization: Record<string, unknown> | null;
  transformations: Record<string, unknown>[];
  contracts: Record<string, unknown>[];
  load: Record<string, unknown>;
  schema_drift: Record<string, string>;
  orchestration: Record<string, unknown>;
  validation: ConfigurationValidation;
  final_approved: boolean;
  approval: ApprovalMetadata;
}

export interface ApprovalMetadata {
  approved_at: string | null;
  approved_by: string | null;
  validation_hash: string | null;
  approved_config_hash: string | null;
  git_commit_sha: string | null;
  git_push_status: string | null;
  dag_id: string | null;
  activation_checked_at: string | null;
}

export interface OnboardingCompletion {
  onboarding_id: string;
  dataset: string;
  status: string;
  safe_error: string | null;
  approval: ApprovalMetadata;
  first_run: {
    attempt: number;
    correlation_id: string | null;
    airflow_dag_id: string | null;
    airflow_run_id: string | null;
    airflow_state: string | null;
    etl_run_id: string | null;
    completed_at: string | null;
    etl_run: OperationRunSummary | null;
  };
}
