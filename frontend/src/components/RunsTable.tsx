import { Link } from "react-router-dom";
import type { RunDetail } from "../types/api";
import {
  formatDateTime,
  formatDuration,
  formatLabel,
  formatNumber,
  shortRunId,
} from "../utils/format";
import { StatusBadge } from "./StatusBadge";

export function RunsTable({ runs, caption = "Recent ETL runs" }: { runs: RunDetail[]; caption?: string }) {
  return (
    <div className="table-shell">
      <table className="data-table data-table-runs">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            <th scope="col">Run ID</th>
            <th scope="col">Dataset</th>
            <th scope="col">Status</th>
            <th scope="col">Started</th>
            <th scope="col">Duration</th>
            <th scope="col">Source</th>
            <th scope="col">Load strategy</th>
            <th scope="col">Loaded</th>
            <th scope="col">Quarantined</th>
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
