import { Database, GitCompareArrows } from "lucide-react";
import { useCallback, useState } from "react";
import { api } from "../api/client";
import { PageHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { SchemaDriftTable } from "../components/SchemaDriftTable";
import { useApi } from "../hooks/useApi";
import type { SchemaDriftSortField } from "../types/api";
import { nextSort, type SortDirection, type SortState } from "../utils/tableSorting";

export function SchemaDriftPage() {
  const [dataset, setDataset] = useState("");
  const [driftType, setDriftType] = useState("");
  const [action, setAction] = useState("");
  const [sort, setSort] = useState<SortState<SchemaDriftSortField>>({
    field: "detected_at",
    direction: "desc",
  });
  const loader = useCallback(
    () => api.schemaDrift(200, sort.field, sort.direction, dataset, driftType, action),
    [action, dataset, driftType, sort.direction, sort.field],
  );
  const { data, error, loading, reload } = useApi(loader);
  const datasets = [...new Set((data ?? []).map((event) => event.dataset))];
  const types = [...new Set((data ?? []).map((event) => event.drift_type))];
  const actions = [...new Set((data ?? []).map((event) => event.action_taken))];
  const handleSort = (field: SchemaDriftSortField, preferredDirection: SortDirection) => {
    setSort((current) => nextSort(current, field, preferredDirection));
  };

  return (
    <>
      <PageHeader eyebrow="Structural controls" title="Schema drift" description="Recorded raw and canonical schema changes with the policy applied by the engine." />
      <div className="mb-5 grid gap-4 lg:grid-cols-2">
        <div className="panel flex gap-3 p-5"><Database className="h-5 w-5 shrink-0 text-cyan-700" aria-hidden="true" /><div><h2 className="font-bold text-slate-900">Raw schema</h2><p className="mt-1 text-sm text-slate-500">Did the physical source change?</p></div></div>
        <div className="panel flex gap-3 p-5"><GitCompareArrows className="h-5 w-5 shrink-0 text-cyan-700" aria-hidden="true" /><div><h2 className="font-bold text-slate-900">Canonical schema</h2><p className="mt-1 text-sm text-slate-500">Does the normalized structure still match what the pipeline expects?</p></div></div>
      </div>
      <div className="panel mb-5 grid gap-3 p-4 md:grid-cols-3">
        <label><span className="sr-only">Filter by dataset</span><select className="field-control w-full" value={dataset} onChange={(event) => setDataset(event.target.value)}><option value="">All datasets</option>{datasets.map((value) => <option key={value}>{value}</option>)}</select></label>
        <label><span className="sr-only">Filter by drift type</span><select className="field-control w-full" value={driftType} onChange={(event) => setDriftType(event.target.value)}><option value="">All drift types</option>{types.map((value) => <option key={value}>{value}</option>)}</select></label>
        <label><span className="sr-only">Filter by action</span><select className="field-control w-full" value={action} onChange={(event) => setAction(event.target.value)}><option value="">All actions</option>{actions.map((value) => <option key={value}>{value}</option>)}</select></label>
      </div>
      {loading && <LoadingState label="Loading schema drift" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {!loading && data && data.length > 0 && (
        <SchemaDriftTable events={data} sort={sort} onSort={handleSort} />
      )}
      {!loading && data && data.length === 0 && <EmptyState title="No schema drift recorded" message="No events match the current filters." />}
    </>
  );
}
