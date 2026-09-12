import { Link } from "react-router-dom";
import type { RunDetail, RunSortField } from "../types/api";
import type { SortDirection, SortState } from "../utils/tableSorting";
import {
  formatDateTime,
  formatDuration,
  formatLabel,
  formatNumber,
  shortRunId,
} from "../utils/format";
import { StatusBadge } from "./StatusBadge";
import { SortableHeader } from "./SortableHeader";

interface RunsTableProps {
  runs: RunDetail[];
  caption?: string;
  sort?: SortState<RunSortField>;
  onSort?: (field: RunSortField, preferredDirection: SortDirection) => void;
}

export function RunsTable({ runs, caption = "Recent ETL runs", sort, onSort }: RunsTableProps) {
  const header = (
    label: string,
    field: RunSortField,
    preferredDirection: SortDirection = "asc",
  ) =>
    sort && onSort ? (
      <SortableHeader
        label={label}
        field={field}
        sort={sort}
        preferredDirection={preferredDirection}
        onSort={onSort}
      />
    ) : (
      <th scope="col">{label}</th>
    );

  return (
    <div className="table-shell">
      <table className="data-table data-table-runs">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {header("Run ID", "run_id")}
            {header("Dataset", "dataset")}
            {header("Status", "status")}
            {header("Started", "started_at", "desc")}
            {header("Duration", "duration_seconds", "desc")}
            {header("Source", "source_type")}
            {header("Load strategy", "load_strategy")}
            {header("Loaded", "rows_loaded", "desc")}
            {header("Quarantined", "rows_quarantined", "desc")}
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.run_id}>
              <td>
                <Link
                  className="table-link run-id-link font-mono text-xs"
                  to={`/runs/${run.run_id}`}
                  title={run.run_id}
                  aria-label={run.run_id}
                >
                  {shortRunId(run.run_id)}
                </Link>
              </td>
              <td>
                <Link className="font-semibold text-slate-800 hover:text-cyan-700" to={`/datasets/${run.dataset}`}>
                  {run.dataset}
                </Link>
              </td>
              <td><StatusBadge status={run.status} /></td>
              <td>{formatDateTime(run.started_at)}</td>
              <td>{formatDuration(run.duration_seconds)}</td>
              <td>{formatLabel(run.source_type)}</td>
              <td>{formatLabel(run.load_strategy)}</td>
              <td>{formatNumber(run.rows_loaded)}</td>
              <td>{formatNumber(run.rows_quarantined)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
