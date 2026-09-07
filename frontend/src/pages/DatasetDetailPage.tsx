import { ArrowLeft, Database, Droplets, GitCompareArrows, Upload } from "lucide-react";
import { useCallback, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { DetailGrid } from "../components/DetailGrid";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { QualityTable } from "../components/QualityTable";
import { RunsTable } from "../components/RunsTable";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import type {
  DatasetSummary,
  DatasetUploadCapability,
  DatasetWatermarkResponse,
  QualitySummary,
  RunDetail,
  SchemaDriftEvent,
} from "../types/api";
import { formatDateTime, formatDuration, formatLabel, formatNumber, shortHash } from "../utils/format";

type Tab = "overview" | "runs" | "quality" | "schema" | "watermark";

interface DatasetDetailData {
  detail: DatasetSummary;
  runs: RunDetail[];
  quality: QualitySummary[];
  drift: SchemaDriftEvent[];
  watermark: DatasetWatermarkResponse;
  uploadCapability: DatasetUploadCapability;
}

const tabs: { id: Tab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "runs", label: "Runs" },
  { id: "quality", label: "Quality" },
  { id: "schema", label: "Schema" },
  { id: "watermark", label: "Watermark" },
];

export function DatasetDetailPage() {
  const { dataset = "" } = useParams();
  const [tab, setTab] = useState<Tab>("overview");
  const loader = useCallback(async (): Promise<DatasetDetailData> => {
    const [detail, runs, quality, drift, watermark, uploadCapability] = await Promise.all([
      api.dataset(dataset),
      api.datasetRuns(dataset),
      api.datasetQuality(dataset),
      api.datasetSchemaDrift(dataset),
      api.datasetWatermark(dataset),
      api.datasetUploadCapability(dataset).catch(() => null),
    ]);
    return {
      detail,
      runs,
      quality,
      drift,
      watermark,
      uploadCapability: uploadCapability ?? {
        dataset,
        source_type: detail.source_type ?? "unknown",
        load_strategy: detail.load_strategy ?? "unknown",
        config_approved: false,
        upload_eligible: false,
        reason: "Operational upload is not available for this dataset.",
      },
    };
  }, [dataset]);
  const { data, error, loading, reload } = useApi(loader);

  return (
    <>
      <Link to="/datasets" className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700 hover:text-cyan-900">
        <ArrowLeft className="h-4 w-4" aria-hidden="true" />
        All datasets
      </Link>
      <PageHeader
        eyebrow="Dataset detail"
        title={dataset}
        description="Current operational state, execution history, quality, schema, and watermark metadata."
        action={data ? (
          <div className="flex flex-wrap items-center gap-3">
            <StatusBadge status={data.detail.health_status} />
            {data.uploadCapability.upload_eligible && (
              <Link className="primary-button" to={`/datasets/${encodeURIComponent(dataset)}/upload`}>
                <Upload className="h-4 w-4" aria-hidden="true" />
                Upload new data
              </Link>
            )}
          </div>
        ) : undefined}
      />
      {loading && <LoadingState label={`Loading ${dataset}`} />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && (
        <div className="space-y-6">
          <div className="panel overflow-x-auto p-2">
            <div className="flex min-w-max gap-1" role="tablist" aria-label="Dataset details">
              {tabs.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  role="tab"
                  aria-selected={tab === item.id}
                  aria-controls={`dataset-${item.id}`}
                  className={`rounded-lg px-4 py-2.5 text-sm font-bold transition ${
                    tab === item.id ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-100"
                  }`}
                  onClick={() => setTab(item.id)}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>

          <section id={`dataset-${tab}`} role="tabpanel" tabIndex={0}>
            {tab === "overview" && (
              <div className="space-y-6">
                {!data.uploadCapability.upload_eligible && data.uploadCapability.reason && (
                  <div className="info-panel" role="note">{data.uploadCapability.reason}</div>
                )}
                <DetailGrid
                  items={[
                    { label: "Health", value: <StatusBadge status={data.detail.health_status} /> },
                    { label: "Latest status", value: <StatusBadge status={data.detail.latest_run_status} /> },
                    { label: "Source type", value: formatLabel(data.detail.source_type) },
                    { label: "Load strategy", value: formatLabel(data.detail.load_strategy) },
                    { label: "Last successful run", value: formatDateTime(data.detail.last_successful_run) },
                    { label: "Latest duration", value: formatDuration(data.detail.latest_duration_seconds) },
                    { label: "Rows loaded", value: formatNumber(data.detail.latest_rows_loaded) },
                    { label: "Rows quarantined", value: formatNumber(data.detail.latest_rows_quarantined) },
                    { label: "Drift", value: <StatusBadge status={data.detail.latest_drift_status} /> },
                    { label: "Watermark", value: data.detail.latest_watermark ?? "Not applicable" },
                  ]}
                />
                <div className="panel p-5">
                  <SectionHeader title="Latest run" description="Most recent recorded execution for this dataset." />
                  {data.runs.length ? <RunsTable runs={data.runs.slice(0, 1)} /> : <EmptyState title="No runs recorded" message="This dataset has no execution history." />}
                </div>
              </div>
            )}

            {tab === "runs" && (
              data.runs.length ? <RunsTable runs={data.runs} caption={`Runs for ${dataset}`} /> : <EmptyState title="No runs recorded" message="This dataset has no execution history." />
            )}

            {tab === "quality" && (
              <div>
                <SectionHeader title="Quality history" description="Rule-level summaries only; quarantine values are never returned." />
                {data.quality.length ? <QualityTable results={data.quality} showDataset={false} /> : <EmptyState title="No quality results recorded" message="No quality-rule summaries exist for this dataset." />}
              </div>
            )}

            {tab === "schema" && (
              <div className="space-y-6">
                <div className="grid gap-4 lg:grid-cols-2">
                  <div className="panel p-5">
                    <div className="flex items-start gap-3">
                      <Database className="mt-0.5 h-5 w-5 text-cyan-700" aria-hidden="true" />
                      <div><h2 className="font-bold text-slate-900">Raw schema</h2><p className="mt-1 text-sm text-slate-500">Did the physical source change?</p></div>
                    </div>
                    <p className="hash-value mt-4">{shortHash(data.runs[0]?.raw_schema_hash)}</p>
                  </div>
                  <div className="panel p-5">
                    <div className="flex items-start gap-3">
                      <GitCompareArrows className="mt-0.5 h-5 w-5 text-cyan-700" aria-hidden="true" />
                      <div><h2 className="font-bold text-slate-900">Canonical schema</h2><p className="mt-1 text-sm text-slate-500">Does normalized structure still match the contract?</p></div>
                    </div>
                    <p className="hash-value mt-4">{shortHash(data.runs[0]?.canonical_schema_hash)}</p>
                  </div>
                </div>
                <SectionHeader title="Drift events" description="Most recent source and canonical changes with their configured action." />
                {data.drift.length ? (
                  <div className="table-shell"><table className="data-table"><caption className="sr-only">Schema drift for {dataset}</caption><thead><tr><th scope="col">Level</th><th scope="col">Type</th><th scope="col">Policy</th><th scope="col">Action</th><th scope="col">Detected</th></tr></thead><tbody>{data.drift.map((event) => <tr key={`${event.run_id}-${event.schema_level}-${event.drift_type}`}><td>{formatLabel(event.schema_level)}</td><td>{formatLabel(event.drift_type)}</td><td>{event.policy}</td><td><StatusBadge status={event.action_taken} /></td><td>{formatDateTime(event.detected_at)}</td></tr>)}</tbody></table></div>
                ) : <EmptyState title="No schema drift recorded" message="No drift events exist for this dataset." />}
              </div>
            )}

            {tab === "watermark" && (
              data.watermark.watermark ? (
                <div className="panel p-6">
                  <div className="mb-5 flex items-center gap-3"><Droplets className="h-5 w-5 text-cyan-700" aria-hidden="true" /><div><h2 className="font-bold text-slate-900">Current watermark</h2><p className="text-sm text-slate-500">Latest successfully published incremental position.</p></div></div>
                  <DetailGrid items={[
                    { label: "Column", value: data.watermark.watermark.watermark_column },
                    { label: "Value", value: data.watermark.watermark.last_successful_value },
                    { label: "Updated", value: formatDateTime(data.watermark.watermark.updated_at) },
                    { label: "Run ID", value: <span className="font-mono text-xs">{data.watermark.watermark.run_id}</span> },
                  ]} />
                </div>
              ) : <EmptyState title="No watermark exists for this dataset" message="Watermarks apply only to configured incremental datasets." />
            )}
          </section>
        </div>
      )}
    </>
  );
}
