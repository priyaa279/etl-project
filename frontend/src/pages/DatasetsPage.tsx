import { Search } from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import { api } from "../api/client";
import { DatasetTable } from "../components/DatasetTable";
import { PageHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { useApi } from "../hooks/useApi";

export function DatasetsPage() {
  const loader = useCallback(() => api.datasets(), []);
  const { data, error, loading, reload } = useApi(loader);
  const [search, setSearch] = useState("");
  const [health, setHealth] = useState("");
  const [source, setSource] = useState("");
  const [strategy, setStrategy] = useState("");

  const filtered = useMemo(
    () =>
      (data ?? []).filter(
        (item) =>
          item.dataset.toLowerCase().includes(search.toLowerCase()) &&
          (!health || item.health_status === health) &&
          (!source || item.source_type === source) &&
          (!strategy || item.load_strategy === strategy),
      ),
    [data, health, search, source, strategy],
  );
  const sources = [...new Set((data ?? []).map((item) => item.source_type).filter(Boolean))];
  const strategies = [...new Set((data ?? []).map((item) => item.load_strategy).filter(Boolean))];

  return (
    <>
      <PageHeader
        eyebrow="Inventory"
        title="Datasets"
        description="Search and compare the latest operational state across every dataset."
      />
      <div className="panel mb-5 grid gap-3 p-4 md:grid-cols-[minmax(220px,1fr)_repeat(3,minmax(140px,0.35fr))]">
        <label className="relative">
          <span className="sr-only">Search datasets</span>
          <Search className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-slate-400" aria-hidden="true" />
          <input className="field-control w-full pl-9" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search datasets" />
        </label>
        <label>
          <span className="sr-only">Filter by health</span>
          <select className="field-control w-full" value={health} onChange={(event) => setHealth(event.target.value)}>
            <option value="">All health states</option>
            {['HEALTHY', 'WARNING', 'FAILED', 'UNKNOWN'].map((value) => <option key={value}>{value}</option>)}
          </select>
        </label>
        <label>
          <span className="sr-only">Filter by source type</span>
          <select className="field-control w-full" value={source} onChange={(event) => setSource(event.target.value)}>
            <option value="">All source types</option>
            {sources.map((value) => <option key={value ?? ''} value={value ?? ''}>{value}</option>)}
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
      {loading && <LoadingState label="Loading datasets" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && filtered.length > 0 && <DatasetTable datasets={filtered} />}
      {data && filtered.length === 0 && (
        <EmptyState title="No datasets match" message="Clear or change the current filters." />
      )}
    </>
  );
}
