import { ArrowDown, ArrowLeft, ShieldAlert } from "lucide-react";
import { useCallback } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { DetailGrid } from "../components/DetailGrid";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { QualityTable } from "../components/QualityTable";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import type { QualitySummary, RunDetail } from "../types/api";
import { formatDateTime, formatDuration, formatLabel, formatNumber } from "../utils/format";

interface RunData {
  run: RunDetail;
  quality: QualitySummary[];
}

function RowFlow({ run }: { run: RunDetail }) {
  const stages = [
    ["Extracted", run.rows_extracted],
    ["Transformed", run.rows_transformed],
    ["Contract passed", run.rows_contract_passed],
    ["Loaded", run.rows_loaded],
  ] as const;
  return (
    <div className="grid gap-4 lg:grid-cols-[1fr_auto_220px] lg:items-center">
      <div className="grid gap-2 sm:grid-cols-4 lg:gap-0">
        {stages.map(([label, value], index) => (
          <div key={label} className="flex items-center sm:contents">
            <div className="relative min-w-0 flex-1 rounded-xl border border-slate-200 bg-white p-4 text-center shadow-sm">
              <p className="text-xs font-bold uppercase tracking-wide text-slate-500">{label}</p>
              <p className="mt-2 text-2xl font-black text-slate-950">{formatNumber(value)}</p>
            </div>
            {index < stages.length - 1 && (
              <ArrowDown className="mx-2 h-4 w-4 shrink-0 text-cyan-600 sm:rotate-[-90deg]" aria-hidden="true" />
            )}
          </div>
        ))}
      </div>
      <div className="hidden h-px w-8 bg-slate-300 lg:block" />
      <div className="rounded-xl border border-amber-200 bg-amber-50 p-4 text-center">
        <ShieldAlert className="mx-auto h-5 w-5 text-amber-700" aria-hidden="true" />
        <p className="mt-2 text-xs font-bold uppercase tracking-wide text-amber-800">Quarantined</p>
        <p className="mt-1 text-2xl font-black text-amber-950">{formatNumber(run.rows_quarantined)}</p>
      </div>
    </div>
  );
}

export function RunDetailPage() {
  const { runId = "" } = useParams();
  const loader = useCallback(async (): Promise<RunData> => {
    const [run, quality] = await Promise.all([api.run(runId), api.runQuality(runId)]);
    return { run, quality };
  }, [runId]);
  const { data, error, loading, reload } = useApi(loader);

  return (
    <>
      <Link to="/runs" className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700 hover:text-cyan-900">
        <ArrowLeft className="h-4 w-4" aria-hidden="true" />
        Run history
      </Link>
      <PageHeader
        eyebrow="Run detail"
        title={runId}
        description={data ? `Execution details for ${data.run.dataset}.` : "Execution metadata and row flow."}
        action={data ? <StatusBadge status={data.run.status} /> : undefined}
      />
      {loading && <LoadingState label={`Loading run ${runId}`} />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && (
        <div className="space-y-8">
          <DetailGrid items={[
            { label: "Status", value: <StatusBadge status={data.run.status} /> },
            { label: "Dataset", value: <Link className="table-link" to={`/datasets/${data.run.dataset}`}>{data.run.dataset}</Link> },
            { label: "Started", value: formatDateTime(data.run.started_at) },
            { label: "Completed", value: formatDateTime(data.run.completed_at) },
            { label: "Duration", value: formatDuration(data.run.duration_seconds) },
            { label: "Source type", value: formatLabel(data.run.source_type) },
            { label: "Load strategy", value: formatLabel(data.run.load_strategy) },
            { label: "Run mode", value: formatLabel(data.run.run_mode) },
          ]} />

          <section>
            <SectionHeader title="Row flow" description="Counts are shown only when the run recorded that stage." />
            <RowFlow run={data.run} />
          </section>

          <section>
            <SectionHeader title="Technical metadata" description="Reproducibility, schema, drift, and incremental state." />
            <div className="panel grid gap-px overflow-hidden bg-slate-200 md:grid-cols-2">
              {[
                ["Raw schema hash", data.run.raw_schema_hash],
                ["Canonical schema hash", data.run.canonical_schema_hash],
                ["Git commit SHA", data.run.git_commit_sha],
                ["Config hash", data.run.config_hash],
                ["Drift status", data.run.drift_status],
                ["Watermark before", data.run.watermark_before],
                ["Watermark after", data.run.watermark_after],
                ["Backfill bounds", data.run.backfill_from && data.run.backfill_to ? `${data.run.backfill_from} → ${data.run.backfill_to}` : null],
              ].map(([label, value]) => (
                <div key={label} className="bg-white p-4"><p className="text-xs font-bold uppercase tracking-wide text-slate-500">{label}</p><p className="hash-value mt-2">{value ?? "Not recorded"}</p></div>
              ))}
            </div>
          </section>

          <section>
            <SectionHeader title="Quality results" description="Rule summaries only. Quarantine values and record identifiers are not exposed." />
            {data.quality.length ? <QualityTable results={data.quality} showDataset={false} /> : <EmptyState title="No quality results recorded" message="This run has no quality-rule summaries." />}
          </section>
        </div>
      )}
    </>
  );
}
