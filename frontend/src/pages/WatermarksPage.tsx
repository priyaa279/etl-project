import { Droplets } from "lucide-react";
import { useCallback, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { PageHeader } from "../components/PageHeader";
import { EmptyState, ErrorState, LoadingState } from "../components/PageState";
import { SortableHeader } from "../components/SortableHeader";
import { useApi } from "../hooks/useApi";
import { nextSort, sortRows, type SortDirection, type SortState } from "../utils/tableSorting";
import { formatDateTime } from "../utils/format";

export function WatermarksPage() {
  const loader = useCallback(() => api.watermarks(), []);
  const { data, error, loading, reload } = useApi(loader);
  type WatermarkSortField = "dataset" | "column" | "value" | "updated_at" | "run_id";
  const [sort, setSort] = useState<SortState<WatermarkSortField>>({
    field: "dataset",
    direction: "asc",
  });
  const sorted = useMemo(
    () =>
      sortRows(data ?? [], sort, {
        dataset: { value: (row) => row.dataset },
        column: { value: (row) => row.watermark_column },
        value: { value: (row) => row.last_successful_value },
        updated_at: { value: (row) => row.updated_at, kind: "date" },
        run_id: { value: (row) => row.run_id },
      }),
    [data, sort],
  );
  const handleSort = (field: WatermarkSortField, preferredDirection: SortDirection) => {
    setSort((current) => nextSort(current, field, preferredDirection));
  };
  return (
    <>
      <PageHeader eyebrow="Incremental state" title="Watermarks" description="Watermarks track the latest successfully published incremental position and advance only after a successful publish. Datasets without incremental loading do not appear here." action={<span className="rounded-xl bg-cyan-50 p-3 text-cyan-700"><Droplets className="h-5 w-5" aria-hidden="true" /></span>} />
      {loading && <LoadingState label="Loading watermarks" />}
      {error && <ErrorState message={error} onRetry={reload} />}
      {data && data.length > 0 && (
        <div className="table-shell"><table className="data-table"><caption className="sr-only">Current incremental watermarks</caption><thead><tr><SortableHeader label="Dataset" field="dataset" sort={sort} onSort={handleSort} /><SortableHeader label="Column" field="column" sort={sort} onSort={handleSort} /><SortableHeader label="Last successful value" field="value" sort={sort} onSort={handleSort} /><SortableHeader label="Updated" field="updated_at" sort={sort} preferredDirection="desc" onSort={handleSort} /><SortableHeader label="Run ID" field="run_id" sort={sort} onSort={handleSort} /></tr></thead><tbody>{sorted.map((item) => <tr key={item.dataset}><td><Link className="table-link" to={`/datasets/${item.dataset}`}>{item.dataset}</Link></td><td className="font-mono text-xs">{item.watermark_column}</td><td className="font-mono text-xs">{item.last_successful_value}</td><td>{formatDateTime(item.updated_at)}</td><td><Link className="table-link font-mono text-xs" to={`/runs/${item.run_id}`}>{item.run_id}</Link></td></tr>)}</tbody></table></div>
      )}
      {data && data.length === 0 && <EmptyState title="No watermarks recorded" message="Watermarks appear after a successful incremental publication." />}
    </>
  );
}
