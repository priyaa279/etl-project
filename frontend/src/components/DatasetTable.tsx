import { ArrowUpRight } from "lucide-react";
import { Link } from "react-router-dom";
import type { DatasetSummary } from "../types/api";
import { formatDateTime, formatDuration, formatLabel, formatNumber } from "../utils/format";
import { StatusBadge } from "./StatusBadge";

export function DatasetTable({
  datasets,
  caption = "Dataset operational health",
}: {
  datasets: DatasetSummary[];
  caption?: string;
}) {
  return (
    <div className="table-shell">
      <table className="data-table data-table-datasets">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            <th scope="col">Dataset</th>
            <th scope="col">Health</th>
            <th scope="col">Latest status</th>
            <th scope="col">Source / load</th>
            <th scope="col">Last run</th>
            <th scope="col">Duration</th>
            <th scope="col">Loaded</th>
            <th scope="col">Quarantined</th>
            <th scope="col">Drift</th>
          </tr>
        </thead>
        <tbody>
          {datasets.map((dataset) => (
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
                <span className="block text-xs text-slate-500">{formatLabel(dataset.load_strategy)}</span>
              </td>
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
