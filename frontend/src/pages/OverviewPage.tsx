import {
  Activity,
  CheckCircle2,
  Database,
  GitCompareArrows,
  ListChecks,
  Rows3,
  TriangleAlert,
  XCircle,
} from "lucide-react";
import { useCallback } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { DatasetTable } from "../components/DatasetTable";
import { MetricCard } from "../components/MetricCard";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { RunsTable } from "../components/RunsTable";
import { useApi } from "../hooks/useApi";
import type { DatasetSummary, Overview, RunDetail } from "../types/api";
import { formatNumber, formatPercent } from "../utils/format";

interface OverviewData {
  overview: Overview;
  datasets: DatasetSummary[];
  runs: RunDetail[];
}

export function OverviewPage() {
  const loader = useCallback(async (): Promise<OverviewData> => {
    const [overview, datasets, runs] = await Promise.all([
      api.overview(),
      api.datasets(),
      api.runs({ limit: 8 }),
    ]);
    return { overview, datasets, runs };
  }, []);
  const { data, error, loading, reload } = useApi(loader);

  return (
    <>
      <PageHeader
        eyebrow="Command center"
        title="System overview"
        description="Current dataset health and the last 24 hours of ETL activity."
        action={
          <span className="inline-flex items-center gap-2 rounded-full border border-emerald-200 bg-white px-3 py-2 text-sm font-bold text-emerald-700">
            <span className="h-2 w-2 rounded-full bg-emerald-500" aria-hidden="true" />
            Read-only live view
          </span>
        }
      />

      {loading && <LoadingState label="Loading command center" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && (
        <div className="space-y-7">
          <section aria-labelledby="kpi-heading">
            <h2 id="kpi-heading" className="sr-only">Operational summary</h2>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <MetricCard label="Total datasets" value={formatNumber(data.overview.total_datasets)} icon={Database} />
              <MetricCard label="Healthy" value={formatNumber(data.overview.healthy_datasets)} icon={CheckCircle2} tone="good" />
              <MetricCard label="Warning" value={formatNumber(data.overview.warning_datasets)} icon={TriangleAlert} tone="warning" />
              <MetricCard label="Failed" value={formatNumber(data.overview.failed_datasets)} icon={XCircle} tone="danger" />
              <MetricCard label="Success rate" value={formatPercent(data.overview.success_rate)} detail={`${data.overview.time_scope_hours}-hour window`} icon={Activity} tone="good" />
              <MetricCard label="Rows loaded" value={formatNumber(data.overview.total_rows_loaded)} detail={`${data.overview.time_scope_hours}-hour window`} icon={Rows3} />
              <MetricCard label="Rows quarantined" value={formatNumber(data.overview.total_rows_quarantined)} detail={`${data.overview.time_scope_hours}-hour window`} icon={ListChecks} tone="warning" />
              <MetricCard label="Drift warnings" value={formatNumber(data.overview.datasets_with_drift_warnings)} detail="Current dataset state" icon={GitCompareArrows} tone="warning" />
            </div>
          </section>

          <section aria-labelledby="dataset-health-heading">
            <div className="flex items-end justify-between gap-4">
              <SectionHeader title="Dataset health" description="Latest operational state for every known dataset." />
              <Link to="/datasets" className="mb-4 text-sm font-bold text-cyan-700 hover:text-cyan-900">View all datasets</Link>
            </div>
            {data.datasets.length ? (
              <DatasetTable datasets={data.datasets} caption="Current dataset health" />
            ) : (
              <EmptyState title="No datasets found" message="Operational metadata will appear after the first ETL run." />
            )}
          </section>

          <section aria-labelledby="recent-runs-heading">
            <div className="flex items-end justify-between gap-4">
              <SectionHeader title="Recent runs" description="Newest ETL executions across all datasets." />
              <Link to="/runs" className="mb-4 text-sm font-bold text-cyan-700 hover:text-cyan-900">View run history</Link>
            </div>
            {data.runs.length ? (
              <RunsTable runs={data.runs} />
            ) : (
              <EmptyState title="No runs recorded" message="Run history will appear after an ETL execution." />
            )}
          </section>
        </div>
      )}
    </>
  );
}
