import { Activity, ListChecks, ShieldAlert, Sigma } from "lucide-react";
import { useCallback } from "react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "../api/client";
import { MetricCard } from "../components/MetricCard";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { useApi } from "../hooks/useApi";
import type { QualityBreakdown } from "../types/api";
import { formatNumber, formatPercent } from "../utils/format";

function BreakdownTable({ title, rows }: { title: string; rows: QualityBreakdown[] }) {
  return (
    <section>
      <SectionHeader title={title} />
      {rows.length ? (
        <div className="table-shell"><table className="data-table"><caption className="sr-only">{title}</caption><thead><tr><th scope="col">Name</th><th scope="col">Checked</th><th scope="col">Failed</th><th scope="col">Failure rate</th></tr></thead><tbody>{rows.map((row) => <tr key={row.label}><td className="font-semibold text-slate-800">{row.label}</td><td>{formatNumber(row.records_checked)}</td><td>{formatNumber(row.records_failed)}</td><td>{formatPercent(row.failure_rate)}</td></tr>)}</tbody></table></div>
      ) : <EmptyState title="No quality failures recorded" message="No quality summaries exist for this time window." />}
    </section>
  );
}

export function QualityPage() {
  const loader = useCallback(() => api.quality(30), []);
  const { data, error, loading, reload } = useApi(loader);

  return (
    <>
      <PageHeader eyebrow="Trust signals" title="Data quality" description="Rule evaluations and quarantine totals from the last 30 days. No row-level failed values are exposed." />
      {loading && <LoadingState label="Loading quality metrics" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && (
        <div className="space-y-8">
          <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Rule checks" value={formatNumber(data.total_records_checked)} detail="Rule evaluations performed" icon={ListChecks} />
            <MetricCard label="Failed checks" value={formatNumber(data.total_records_failed)} detail="Contract failures" icon={ShieldAlert} tone="warning" />
            <MetricCard label="Failure rate" value={formatPercent(data.failure_rate)} icon={Activity} tone="warning" />
            <MetricCard label="Rows quarantined" value={formatNumber(data.rows_quarantined)} detail="Distinct records prevented from trusted publication" icon={Sigma} tone="warning" />
          </div>

          <section className="panel p-5" aria-labelledby="quality-trend-heading">
            <SectionHeader title="Recent quality trend" description="Daily records checked versus rule failures." />
            {data.recent_trend.length ? (
              <div className="h-72" role="img" aria-label="Bar chart of checked records and failed rule evaluations by day">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={data.recent_trend} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                    <XAxis dataKey="date" tick={{ fill: "#64748b", fontSize: 12 }} />
                    <YAxis tick={{ fill: "#64748b", fontSize: 12 }} width={45} />
                    <Tooltip />
                    <Bar dataKey="records_checked" name="Checked" fill="#0e7490" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="records_failed" name="Failed" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : <EmptyState title="No quality trend recorded" message="Daily quality data will appear after evaluated runs." />}
          </section>

          <BreakdownTable title="Failures by dataset" rows={data.by_dataset} />
          <div className="grid gap-8 xl:grid-cols-2">
            <BreakdownTable title="Failures by rule" rows={data.by_rule} />
            <BreakdownTable title="Failures by rule type" rows={data.by_rule_type} />
          </div>
        </div>
      )}
    </>
  );
}
