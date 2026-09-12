import { ArrowLeft, Database, Droplets, GitCompareArrows, LockKeyhole, Upload } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { ApiError, api } from "../api/client";
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
  PipelineSummary,
  PipelineTransformation,
  QualitySummary,
  RunDetail,
  SchemaDriftEvent,
  TrustedDataPreview,
} from "../types/api";
import { formatDateTime, formatDuration, formatLabel, formatNumber, shortHash } from "../utils/format";

type Tab = "overview" | "runs" | "quality" | "schema" | "watermark" | "trusted";

interface DatasetDetailData {
  detail: DatasetSummary;
  runs: RunDetail[];
  quality: QualitySummary[];
  drift: SchemaDriftEvent[];
  watermark: DatasetWatermarkResponse;
  uploadCapability: DatasetUploadCapability;
  pipelineSummary: PipelineSummary | null;
}

const tabs: { id: Tab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "runs", label: "Runs" },
  { id: "quality", label: "Quality" },
  { id: "schema", label: "Schema" },
  { id: "watermark", label: "Watermark" },
  { id: "trusted", label: "Trusted Data" },
];

const previewLimit = 25;

function detailRows(transformation: PipelineTransformation): [string, string][] {
  const details = transformation.details;
  if (transformation.type === "cast") {
    return [["Column", String(details.column)], ["Datatype", String(details.datatype)]];
  }
  if (transformation.type === "filter") {
    return [["Condition", String(details.condition)]];
  }
  if (transformation.type === "derive") {
    return [
      ["Target", String(details.target_column)],
      ["Datatype", String(details.datatype)],
      ["Expression", String(details.expression)],
    ];
  }
  if (transformation.type === "map") {
    const mappings = Object.entries((details.mappings ?? {}) as Record<string, unknown>)
      .map(([source, target]) => `${source} → ${String(target)}`)
      .join(", ");
    return [["Column", String(details.column)], ["Mappings", mappings]];
  }
  const order = Object.entries((details.order_by ?? {}) as Record<string, unknown>)
    .map(([column, direction]) => `${column} ${String(direction).toUpperCase()}`)
    .join(", ");
  return [
    ["Keys", ((details.keys ?? []) as unknown[]).map(String).join(", ")],
    ["Order", order],
  ];
}

function displayValue(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

export function DatasetDetailPage() {
  const { dataset = "" } = useParams();
  const [tab, setTab] = useState<Tab>("overview");
  const [trustedOffset, setTrustedOffset] = useState(0);
  const [trustedData, setTrustedData] = useState<TrustedDataPreview | null>(null);
  const [trustedError, setTrustedError] = useState<{ message: string; disabled: boolean } | null>(null);
  const [trustedLoading, setTrustedLoading] = useState(false);
  const [trustedRevision, setTrustedRevision] = useState(0);
  const loader = useCallback(async (): Promise<DatasetDetailData> => {
    const [detail, runs, quality, drift, watermark, uploadCapability, pipelineSummary] = await Promise.all([
      api.dataset(dataset),
      api.datasetRuns(dataset),
      api.datasetQuality(dataset),
      api.datasetSchemaDrift(dataset),
      api.datasetWatermark(dataset),
      api.datasetUploadCapability(dataset).catch(() => null),
      api.pipelineSummary(dataset).catch(() => null),
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
      pipelineSummary,
    };
  }, [dataset]);
  const { data, error, loading, reload } = useApi(loader);

  useEffect(() => {
    if (tab !== "trusted") return undefined;
    let active = true;
    Promise.resolve()
      .then(() => {
        if (active) {
          setTrustedLoading(true);
          setTrustedError(null);
        }
        return api.trustedData(dataset, previewLimit, trustedOffset);
      })
      .then((result) => {
        if (active) setTrustedData(result);
      })
      .catch((reason: unknown) => {
        if (!active) return;
        setTrustedData(null);
        setTrustedError({
          message: reason instanceof Error ? reason.message : "Unable to load trusted data.",
          disabled: reason instanceof ApiError && reason.status === 403,
        });
      })
      .finally(() => {
        if (active) setTrustedLoading(false);
      });
    return () => {
      active = false;
    };
  }, [dataset, tab, trustedOffset, trustedRevision]);

  return (
    <>
      <Link to="/datasets" className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700 hover:text-cyan-900">
        <ArrowLeft className="h-4 w-4" aria-hidden="true" />
        All datasets
      </Link>
      <PageHeader
        eyebrow="Dataset detail"
        title={dataset}
        description="Current operational state, execution history, quality, schema, watermark metadata, and published trusted output."
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
                <div className="panel p-5">
                  <SectionHeader
                    title="Current Transformation Plan"
                    description="Approved transformations currently configured for future executions, shown in execution order. This is not a historical reconstruction of an earlier run."
                  />
                  {data.pipelineSummary === null ? (
                    <p className="text-sm text-slate-500">The current transformation plan is temporarily unavailable.</p>
                  ) : data.pipelineSummary.transformations.length ? (
                    <ol className="grid gap-3">
                      {data.pipelineSummary.transformations.map((transformation) => (
                        <li key={transformation.id} className="rounded-xl border border-slate-200 bg-slate-50 p-4">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="example-chip">{transformation.position}</span>
                            <h3 className="font-bold text-slate-900">{formatLabel(transformation.type)}</h3>
                            <span className="font-mono text-xs text-slate-500">{transformation.id}</span>
                          </div>
                          <dl className="mt-3 grid gap-2 text-sm md:grid-cols-2">
                            {detailRows(transformation).map(([label, value]) => (
                              <div key={label} className="min-w-0">
                                <dt className="font-semibold text-slate-500">{label}</dt>
                                <dd className="mt-0.5 break-words font-mono text-xs text-slate-800">{value}</dd>
                              </div>
                            ))}
                          </dl>
                        </li>
                      ))}
                    </ol>
                  ) : (
                    <p className="text-sm text-slate-500">No transformations are configured. Records proceed from approved normalization to quality checks.</p>
                  )}
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

            {tab === "trusted" && (
              <div className="space-y-6">
                <div>
                  <SectionHeader
                    title="Trusted Data"
                    description="Validated records currently published for downstream consumption. These records passed the configured transformation and data-quality pipeline before publication."
                  />
                  <div className="info-panel flex items-start gap-3" role="note">
                    <LockKeyhole className="mt-0.5 h-5 w-5 shrink-0" aria-hidden="true" />
                    <p>This is a read-only inspection of the current trusted output, not an analytics workspace. Classified PII and restricted values are suppressed.</p>
                  </div>
                </div>
                {trustedLoading && <LoadingState label="Loading trusted records" />}
                {trustedError?.disabled && (
                  <div className="info-panel" role="note">{trustedError.message} Enable it only in a trusted local environment.</div>
                )}
                {trustedError && !trustedError.disabled && (
                  <ErrorState message={trustedError.message} onRetry={() => setTrustedRevision((value) => value + 1)} />
                )}
                {!trustedLoading && !trustedError && trustedData && (
                  <DetailGrid items={[
                    { label: "Dataset", value: trustedData.dataset },
                    { label: "Target", value: <span className="font-mono text-xs">{trustedData.target}</span> },
                    { label: "Rows", value: formatNumber(trustedData.total_rows) },
                    { label: "Last updated", value: formatDateTime(trustedData.last_updated) },
                  ]} />
                )}
                {!trustedLoading && !trustedError && trustedData?.state === "NOT_PUBLISHED" && (
                  <EmptyState title="No trusted output has been published for this dataset yet" message="Run the approved pipeline successfully before inspecting trusted records." />
                )}
                {!trustedLoading && !trustedError && trustedData?.state === "EMPTY" && (
                  <EmptyState title="No trusted records are currently published" message="The trusted target exists and currently contains zero rows." />
                )}
                {!trustedLoading && !trustedError && trustedData?.state === "AVAILABLE" && (
                  <>
                    {trustedData.latest_successful_run_id && (
                      <div className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-slate-200 bg-white p-4">
                        <div><p className="text-xs font-bold uppercase tracking-wide text-slate-500">Latest successful run</p><p className="mt-1 font-mono text-sm text-slate-800">{trustedData.latest_successful_run_id}</p></div>
                        <Link className="secondary-button" to={`/runs/${encodeURIComponent(trustedData.latest_successful_run_id)}`}>View Run Details</Link>
                      </div>
                    )}
                    <div className="table-shell" data-testid="trusted-data-table-container">
                      <table className="data-table trusted-data-table">
                        <caption className="sr-only">Trusted data for {dataset}</caption>
                        <thead><tr>{trustedData.columns.map((column) => <th key={column.name} scope="col" title={column.classification ? `Classification: ${column.classification}` : undefined}>{column.name}{column.redacted && <span className="ml-2 text-amber-700">Protected</span>}</th>)}</tr></thead>
                        <tbody>{trustedData.rows.map((row, rowIndex) => <tr key={`${trustedData.offset}-${rowIndex}`}>{trustedData.columns.map((column) => {
                          const rendered = column.redacted ? "REDACTED" : displayValue(row[column.name]);
                          return <td key={column.name}><span className={column.redacted ? "text-xs font-bold text-amber-700" : row[column.name] === null ? "text-xs italic text-slate-400" : "trusted-cell"} title={rendered}>{rendered}</span></td>;
                        })}</tr>)}</tbody>
                      </table>
                    </div>
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <p className="text-sm text-slate-600">Showing {trustedData.offset + 1}–{Math.min(trustedData.offset + trustedData.rows.length, trustedData.total_rows)} of {trustedData.total_rows}</p>
                      <div className="flex gap-2">
                        <button type="button" className="secondary-button" disabled={trustedData.offset === 0} onClick={() => setTrustedOffset(Math.max(0, trustedData.offset - trustedData.limit))}>Previous</button>
                        <button type="button" className="secondary-button" disabled={!trustedData.has_more} onClick={() => setTrustedOffset(trustedData.offset + trustedData.limit)}>Next</button>
                      </div>
                    </div>
                    <div className="panel p-5">
                      <SectionHeader title="Using this dataset" description="The trusted layer is designed for controlled downstream consumption." />
                      <p className="text-sm text-slate-600">Trusted target: <span className="font-mono text-xs text-slate-800">{trustedData.target}</span></p>
                      <p className="mt-2 text-sm text-slate-600">Typical consumers include SQL analytics, Power BI, Tableau, and downstream applications using separately managed access.</p>
                    </div>
                  </>
                )}
              </div>
            )}
          </section>
        </div>
      )}
    </>
  );
}
