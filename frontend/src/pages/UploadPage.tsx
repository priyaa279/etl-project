import { ArrowLeft, Check, FileUp, Play, RefreshCw, ShieldCheck } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { DetailGrid } from "../components/DetailGrid";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { ErrorState, LoadingState } from "../components/PageState";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import type { UploadOperation } from "../types/api";
import { formatDuration, formatLabel, formatNumber } from "../utils/format";

const activeStates = new Set(["TRIGGERING", "QUEUED", "RUNNING"]);

function fileSize(bytes: number) {
  if (bytes < 1024) return `${bytes} bytes`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function UploadPage() {
  const { dataset = "" } = useParams();
  const capabilityLoader = useCallback(() => api.datasetUploadCapability(dataset), [dataset]);
  const capability = useApi(capabilityLoader);
  const [file, setFile] = useState<File | null>(null);
  const [operation, setOperation] = useState<UploadOperation | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  useEffect(() => {
    if (!operation || !activeStates.has(operation.status)) return;
    const timer = window.setInterval(() => {
      void api.uploadStatus(operation.upload_id).then(setOperation).catch(() => undefined);
    }, 3000);
    return () => window.clearInterval(timer);
  }, [operation]);

  const runAction = async (action: () => Promise<UploadOperation>) => {
    setBusy(true);
    setActionError(null);
    try {
      setOperation(await action());
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "The operation could not continue.");
    } finally {
      setBusy(false);
    }
  };

  const steps = useMemo(() => {
    const status = operation?.status;
    return [
      ["1", "File", Boolean(file || operation)],
      ["2", "Validate", Boolean(operation?.validated_at)],
      ["3", "Review", status === "READY" || status === "WARNING" || status === "BLOCKED" || activeStates.has(status ?? "") || status === "SUCCEEDED" || status === "FAILED"],
      ["4", "Run", Boolean(operation?.triggered_at)],
      ["5", "Monitor", activeStates.has(status ?? "") || Boolean(operation?.etl_run)],
      ["6", "Result", status === "SUCCEEDED" || status === "FAILED"],
    ] as const;
  }, [file, operation]);

  if (capability.loading) return <LoadingState label="Loading upload capability" />;
  if (capability.error) return <ErrorState message={capability.error} onRetry={capability.reload} />;
  if (!capability.data) return null;

  return (
    <>
      <Link to={`/datasets/${encodeURIComponent(dataset)}`} className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700 hover:text-cyan-900">
        <ArrowLeft className="h-4 w-4" aria-hidden="true" />
        Back to {dataset}
      </Link>
      <PageHeader eyebrow="Existing dataset operation" title="Upload new data" description="Validate one source artifact, explicitly start the approved pipeline, and monitor its real execution." />

      <ol className="workflow-steps" aria-label="Upload workflow">
        {steps.map(([number, label, complete]) => (
          <li key={number} className={complete ? "workflow-step workflow-step-complete" : "workflow-step"}>
            <span>{complete ? <Check className="h-4 w-4" aria-hidden="true" /> : number}</span>{label}
          </li>
        ))}
      </ol>

      <div className="space-y-6">
        <DetailGrid items={[
          { label: "Dataset", value: dataset },
          { label: "Expected source", value: formatLabel(capability.data.source_type) },
          { label: "Load strategy", value: formatLabel(capability.data.load_strategy) },
          { label: "Configuration", value: capability.data.config_approved ? "Approved" : "Not approved" },
        ]} />

        {!capability.data.upload_eligible ? (
          <div className="info-panel" role="note">{capability.data.reason}</div>
        ) : (
          <section className="panel p-6">
            <SectionHeader title="1. Select file" description="The browser sends file bytes; server storage locations are never accepted from the client." />
            <label className="file-picker">
              <FileUp className="h-7 w-7 text-cyan-700" aria-hidden="true" />
              <span className="font-bold text-slate-900">Choose {formatLabel(capability.data.source_type)} file</span>
              <input type="file" accept={capability.data.source_type === "json" ? ".json,.jsonl,.ndjson" : `.${capability.data.source_type}`} onChange={(event) => { setFile(event.target.files?.[0] ?? null); setOperation(null); setActionError(null); }} />
            </label>
            {file && !operation && (
              <div className="mt-4 flex flex-wrap items-center justify-between gap-4">
                <div><p className="font-semibold text-slate-900">{file.name}</p><p className="text-sm text-slate-500">{fileSize(file.size)} · {dataset} · {formatLabel(capability.data.source_type)}</p></div>
                <button className="primary-button" type="button" disabled={busy} onClick={() => void runAction(() => api.upload(dataset, file))}>{busy ? "Uploading…" : "Upload file"}</button>
              </div>
            )}
          </section>
        )}

        {actionError && <div className="state-panel border-rose-200 bg-rose-50 text-rose-900" role="alert">{actionError}</div>}

        {operation && (
          <section className="panel p-6">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <SectionHeader title="2. Validate and review" description={`${operation.original_filename} · ${fileSize(operation.size_bytes)} · SHA-256 ${operation.sha256.slice(0, 12)}…`} />
              <StatusBadge status={operation.status} />
            </div>
            {operation.status === "UPLOADED" && (
              <button className="primary-button" type="button" disabled={busy} onClick={() => void runAction(() => api.validateUpload(operation.upload_id))}>
                <ShieldCheck className="h-4 w-4" aria-hidden="true" />{busy ? "Validating…" : "Validate / Preflight"}
              </button>
            )}
            {operation.preflight_result && (
              <div className="space-y-4">
                <DetailGrid items={[
                  { label: "Source", value: operation.preflight_result.source_valid ? `${formatLabel(operation.source_type)} compatible` : "Not compatible" },
                  { label: "Configuration", value: operation.preflight_result.config_approved ? "Approved" : "Not approved" },
                  { label: "Canonical structure", value: operation.preflight_result.canonical_compatible ? "Compatible" : "Blocked" },
                  { label: "Drift", value: operation.preflight_result.drift.detected ? formatLabel(operation.preflight_result.drift.status) : "None" },
                ]} />
                {operation.preflight_result.message && <p className="info-panel">{operation.preflight_result.message}</p>}
                {operation.preflight_result.drift.events.length > 0 && (
                  <ul className="event-list">{operation.preflight_result.drift.events.map((event, index) => <li key={`${event.type}-${index}`}><strong>{formatLabel(event.type)}</strong><span>{event.description}</span><span>Policy: {event.policy.toUpperCase()}</span></li>)}</ul>
                )}
                {operation.status === "BLOCKED" ? (
                  <p className="blocked-panel">Cannot run this upload. The file has not been processed by ETL.</p>
                ) : (operation.status === "READY" || operation.status === "WARNING") && (
                  <div className="flex flex-wrap items-center justify-between gap-4 rounded-xl border border-slate-200 bg-slate-50 p-4">
                    <div><p className="font-bold text-slate-900">{operation.status === "READY" ? "Ready to run" : "Ready with warning"}</p><p className="text-sm text-slate-600">The real ETL run will enforce the approved drift policy again.</p></div>
                    <button className="primary-button" type="button" disabled={busy} onClick={() => void runAction(() => api.runUpload(operation.upload_id))}><Play className="h-4 w-4" aria-hidden="true" />{busy ? "Starting…" : "Run ETL"}</button>
                  </div>
                )}
              </div>
            )}
          </section>
        )}

        {operation && (activeStates.has(operation.status) || operation.etl_run || operation.status === "FAILED") && (
          <section className="panel p-6" aria-live="polite">
            <div className="flex flex-wrap items-start justify-between gap-4"><SectionHeader title="5. Monitor and result" description="Airflow orchestrates; the existing ETL engine processes and publishes." /><StatusBadge status={operation.status} /></div>
            <DetailGrid items={[
              { label: "Airflow", value: operation.airflow_state ? formatLabel(operation.airflow_state) : "Waiting" },
              { label: "ETL", value: operation.etl_run?.status ? formatLabel(operation.etl_run.status) : "Waiting" },
              { label: "Extracted", value: formatNumber(operation.etl_run?.rows_extracted) },
              { label: "Transformed", value: formatNumber(operation.etl_run?.rows_transformed) },
              { label: "Quality passed", value: formatNumber(operation.etl_run?.rows_contract_passed) },
              { label: "Quarantined", value: formatNumber(operation.etl_run?.rows_quarantined) },
              { label: "Loaded", value: formatNumber(operation.etl_run?.rows_loaded) },
              { label: "Duration", value: formatDuration(operation.etl_run?.duration_seconds) },
            ]} />
            {activeStates.has(operation.status) && <p className="mt-4 inline-flex items-center gap-2 text-sm font-semibold text-sky-800"><RefreshCw className="h-4 w-4 animate-spin" aria-hidden="true" />Checking every few seconds</p>}
            {operation.status === "FAILED" && <p className="blocked-panel mt-4">{operation.safe_error ?? "The ETL run failed. Review the safe run summary or server logs."}</p>}
            {operation.etl_run_id && <Link className="secondary-button mt-4" to={`/runs/${encodeURIComponent(operation.etl_run_id)}`}>View Run Details</Link>}
          </section>
        )}
      </div>
    </>
  );
}
