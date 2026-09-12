import { ArrowUpRight } from "lucide-react";
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import type { DatasetSummary } from "../types/api";
import {
  nextSort,
  sortRows,
  type SortDirection,
  type SortState,
} from "../utils/tableSorting";
import { formatDateTime, formatDuration, formatLabel, formatNumber } from "../utils/format";
import { SortableHeader } from "./SortableHeader";
import { StatusBadge } from "./StatusBadge";

type DatasetSortField =
  | "dataset"
  | "health"
  | "status"
  | "source"
  | "load_strategy"
  | "last_run"
  | "duration"
  | "loaded"
  | "quarantined"
  | "drift";

export function DatasetTable({
  datasets,
  caption = "Dataset operational health",
}: {
  datasets: DatasetSummary[];
  caption?: string;
}) {
  const [sort, setSort] = useState<SortState<DatasetSortField>>({
    field: "dataset",
    direction: "asc",
  });
  const sorted = useMemo(
    () =>
      sortRows(datasets, sort, {
        dataset: { value: (row) => row.dataset },
        health: { value: (row) => row.health_status },
        status: { value: (row) => row.latest_run_status },
        source: { value: (row) => row.source_type },
        load_strategy: { value: (row) => row.load_strategy },
        last_run: { value: (row) => row.latest_run_time, kind: "date" },
        duration: { value: (row) => row.latest_duration_seconds, kind: "number" },
        loaded: { value: (row) => row.latest_rows_loaded, kind: "number" },
        quarantined: { value: (row) => row.latest_rows_quarantined, kind: "number" },
        drift: { value: (row) => row.latest_drift_status },
      }),
    [datasets, sort],
  );
  const handleSort = (field: DatasetSortField, preferredDirection: SortDirection) => {
    setSort((current) => nextSort(current, field, preferredDirection));
  };

  return (
    <div className="table-shell">
      <table className="data-table data-table-datasets">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            <SortableHeader label="Dataset" field="dataset" sort={sort} onSort={handleSort} />
            <SortableHeader label="Health" field="health" sort={sort} onSort={handleSort} />
            <SortableHeader label="Latest status" field="status" sort={sort} onSort={handleSort} />
            <SortableHeader label="Source" field="source" sort={sort} onSort={handleSort} />
            <SortableHeader label="Load strategy" field="load_strategy" sort={sort} onSort={handleSort} />
            <SortableHeader label="Last run" field="last_run" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Duration" field="duration" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Loaded" field="loaded" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Quarantined" field="quarantined" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Drift" field="drift" sort={sort} onSort={handleSort} />
          </tr>
        </thead>
        <tbody>
          {sorted.map((dataset) => (
            <tr key={dataset.dataset}>
              <td>
                <Link className="table-link inline-flex items-center gap-1.5" to={`/datasets/${dataset.dataset}`}>
                  {dataset.dataset}
                  <ArrowUpRight className="h-3.5 w-3.5" aria-hidden="true" />
                </Link>
              </td>
              <td><StatusBadge status={dataset.health_status} /></td>
              <td><StatusBadge status={dataset.latest_run_status} /></td>
              <td>
                <span className="font-semibold text-slate-700">{formatLabel(dataset.source_type)}</span>
              </td>
              <td>{formatLabel(dataset.load_strategy)}</td>
              <td>{formatDateTime(dataset.latest_run_time)}</td>
              <td>{formatDuration(dataset.latest_duration_seconds)}</td>
              <td>{formatNumber(dataset.latest_rows_loaded)}</td>
              <td>{formatNumber(dataset.latest_rows_quarantined)}</td>
              <td><StatusBadge status={dataset.latest_drift_status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
