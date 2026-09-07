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
}

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
