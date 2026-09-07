import type { QualitySummary } from "../types/api";
import { formatDateTime, formatNumber, formatPercent } from "../utils/format";
import { StatusBadge } from "./StatusBadge";

export function QualityTable({
  results,
  caption = "Data-quality rule results",
  showDataset = true,
}: {
  results: QualitySummary[];
  caption?: string;
  showDataset?: boolean;
}) {
  return (
    <div className="table-shell">
      <table className="data-table">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {showDataset && <th scope="col">Dataset</th>}
            <th scope="col">Rule</th>
            <th scope="col">Type</th>
            <th scope="col">Checked</th>
            <th scope="col">Failed</th>
            <th scope="col">Failure rate</th>
            <th scope="col">Status</th>
            <th scope="col">Recorded</th>
          </tr>
        </thead>
        <tbody>
          {results.map((result) => (
            <tr key={`${result.run_id}-${result.rule_id}`}>
              {showDataset && <td className="font-semibold text-slate-800">{result.dataset}</td>}
              <td className="font-mono text-xs">{result.rule_id}</td>
              <td>{result.rule_type}</td>
              <td>{formatNumber(result.records_checked)}</td>
              <td>{formatNumber(result.records_failed)}</td>
              <td>{formatPercent(result.failure_rate)}</td>
              <td><StatusBadge status={result.status} /></td>
              <td>{formatDateTime(result.timestamp)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
