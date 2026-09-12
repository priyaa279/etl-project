import { Search } from "lucide-react";
import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { api } from "../api/client";
import { PageHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { RunsTable } from "../components/RunsTable";
import { useApi } from "../hooks/useApi";
import type { RunSortField } from "../types/api";
import { nextSort, type SortDirection, type SortState } from "../utils/tableSorting";

const runSortFields = new Set<RunSortField>([
  "run_id",
  "dataset",
  "status",
  "started_at",
  "duration_seconds",
  "source_type",
  "load_strategy",
  "rows_loaded",
  "rows_quarantined",
]);

const strategies = ["full", "incremental", "upsert", "scd2"];

export function RunsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const search = searchParams.get("search") ?? "";
  const status = searchParams.get("status") ?? "";
  const strategy = searchParams.get("load_strategy") ?? "";
  const requestedSort = searchParams.get("sort") as RunSortField | null;
  const sort: SortState<RunSortField> = {
    field: requestedSort && runSortFields.has(requestedSort) ? requestedSort : "started_at",
    direction: searchParams.get("direction") === "asc" ? "asc" : "desc",
  };
  const loader = useCallback(
    () =>
      api.runs({
        limit: 100,
        search,
        status: status || undefined,
        load_strategy: strategy || undefined,
        sort: sort.field,
        direction: sort.direction,
      }),
    [search, sort.direction, sort.field, status, strategy],
  );
  const { data, error, loading, reload } = useApi(loader);
  const updateParameter = (name: string, value: string) => {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(name, value);
    else next.delete(name);
    setSearchParams(next);
  };
  const handleSort = (field: RunSortField, preferredDirection: SortDirection) => {
    const nextSortState = nextSort(sort, field, preferredDirection);
    const next = new URLSearchParams(searchParams);
    next.set("sort", nextSortState.field);
    next.set("direction", nextSortState.direction);
    setSearchParams(next);
  };

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
          <input className="field-control w-full pl-9" value={search} onChange={(event) => updateParameter("search", event.target.value)} placeholder="Run ID or dataset" />
        </label>
        <label>
          <span className="sr-only">Filter by run status</span>
          <select className="field-control w-full" value={status} onChange={(event) => updateParameter("status", event.target.value)}>
            <option value="">All statuses</option>
            {['SUCCEEDED', 'FAILED', 'RUNNING'].map((value) => <option key={value}>{value}</option>)}
          </select>
        </label>
        <label>
          <span className="sr-only">Filter by load strategy</span>
          <select className="field-control w-full" value={strategy} onChange={(event) => updateParameter("load_strategy", event.target.value)}>
            <option value="">All load strategies</option>
            {strategies.map((value) => <option key={value} value={value}>{value}</option>)}
          </select>
        </label>
      </div>
      {loading && <LoadingState label="Loading run history" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {!loading && data && data.length > 0 && <RunsTable runs={data} sort={sort} onSort={handleSort} />}
      {!loading && data && data.length === 0 && (
        <EmptyState title="No runs match" message="Clear or change the current filters." />
      )}
    </>
  );
}
