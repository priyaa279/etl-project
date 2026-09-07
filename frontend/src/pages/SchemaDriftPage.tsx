import { Database, GitCompareArrows } from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { PageHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import { formatDateTime, formatLabel } from "../utils/format";

export function SchemaDriftPage() {
  const loader = useCallback(() => api.schemaDrift(200), []);
  const { data, error, loading, reload } = useApi(loader);
  const [dataset, setDataset] = useState("");
  const [driftType, setDriftType] = useState("");
  const [action, setAction] = useState("");
  const filtered = useMemo(
    () =>
      (data ?? []).filter(
        (event) =>
          (!dataset || event.dataset === dataset) &&
          (!driftType || event.drift_type === driftType) &&
          (!action || event.action_taken === action),
      ),
    [action, data, dataset, driftType],
  );
  const datasets = [...new Set((data ?? []).map((event) => event.dataset))];
  const types = [...new Set((data ?? []).map((event) => event.drift_type))];
  const actions = [...new Set((data ?? []).map((event) => event.action_taken))];

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
      {data && filtered.length > 0 && (
        <div className="table-shell"><table className="data-table"><caption className="sr-only">Recent schema drift events</caption><thead><tr><th scope="col">Dataset</th><th scope="col">Level</th><th scope="col">Drift type</th><th scope="col">Policy</th><th scope="col">Action</th><th scope="col">Detected</th></tr></thead><tbody>{filtered.map((event) => <tr key={`${event.run_id}-${event.schema_level}-${event.drift_type}`}><td><Link className="table-link" to={`/datasets/${event.dataset}`}>{event.dataset}</Link></td><td>{formatLabel(event.schema_level)}</td><td>{formatLabel(event.drift_type)}</td><td>{event.policy}</td><td><StatusBadge status={event.action_taken} /></td><td>{formatDateTime(event.detected_at)}</td></tr>)}</tbody></table></div>
      )}
      {data && filtered.length === 0 && <EmptyState title="No schema drift recorded" message="No events match the current filters." />}
    </>
  );
}
