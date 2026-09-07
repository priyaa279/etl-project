import { Search } from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import { api } from "../api/client";
import { PageHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { RunsTable } from "../components/RunsTable";
import { useApi } from "../hooks/useApi";

export function RunsPage() {
  const loader = useCallback(() => api.runs({ limit: 100 }), []);
  const { data, error, loading, reload } = useApi(loader);
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("");
  const [strategy, setStrategy] = useState("");

  const filtered = useMemo(
    () =>
      (data ?? []).filter(
        (run) =>
          (run.run_id.toLowerCase().includes(search.toLowerCase()) ||
            run.dataset.toLowerCase().includes(search.toLowerCase())) &&
          (!status || run.status === status) &&
          (!strategy || run.load_strategy === strategy),
      ),
    [data, search, status, strategy],
  );
  const strategies = [...new Set((data ?? []).map((run) => run.load_strategy).filter(Boolean))];

  return (
    <>
      <PageHeader
        eyebrow="Execution history"
        title="Runs"
        description="Inspect recent executions across datasets, sources, and loading strategies."
      />
      <div className="panel mb-5 grid gap-3 p-4 md:grid-cols-[minmax(250px,1fr)_180px_190px]">
        <label className="relative">
          <span className="sr-only">Search runs</span>
          <Search className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-slate-400" aria-hidden="true" />
          <input className="field-control w-full pl-9" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Run ID or dataset" />
        </label>
        <label>
          <span className="sr-only">Filter by run status</span>
          <select className="field-control w-full" value={status} onChange={(event) => setStatus(event.target.value)}>
            <option value="">All statuses</option>
            {['SUCCEEDED', 'FAILED', 'RUNNING'].map((value) => <option key={value}>{value}</option>)}
          </select>
        </label>
        <label>
          <span className="sr-only">Filter by load strategy</span>
          <select className="field-control w-full" value={strategy} onChange={(event) => setStrategy(event.target.value)}>
            <option value="">All load strategies</option>
            {strategies.map((value) => <option key={value ?? ''} value={value ?? ''}>{value}</option>)}
          </select>
        </label>
      </div>
      {loading && <LoadingState label="Loading run history" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && filtered.length > 0 && <RunsTable runs={filtered} />}
      {data && filtered.length === 0 && (
        <EmptyState title="No runs match" message="Clear or change the current filters." />
      )}
    </>
  );
}
