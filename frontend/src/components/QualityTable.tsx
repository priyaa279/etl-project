import { useMemo, useState } from "react";
import type { QualitySortField, QualitySummary } from "../types/api";
import {
  nextSort,
  sortRows,
  type SortDirection,
  type SortState,
} from "../utils/tableSorting";
import { formatDateTime, formatNumber, formatPercent } from "../utils/format";
import { SortableHeader } from "./SortableHeader";
import { StatusBadge } from "./StatusBadge";

export function QualityTable({
  results,
  caption = "Data-quality rule results",
  showDataset = true,
  sort: controlledSort,
  onSort,
}: {
  results: QualitySummary[];
  caption?: string;
  showDataset?: boolean;
  sort?: SortState<QualitySortField>;
  onSort?: (field: QualitySortField, preferredDirection: SortDirection) => void;
}) {
  const [localSort, setLocalSort] = useState<SortState<QualitySortField>>({
    field: "timestamp",
    direction: "desc",
  });
  const sort = controlledSort ?? localSort;
  const sorted = useMemo(
    () =>
      onSort
        ? results
        : sortRows(results, sort, {
            dataset: { value: (row) => row.dataset },
            rule_id: { value: (row) => row.rule_id },
            rule_type: { value: (row) => row.rule_type },
            records_checked: { value: (row) => row.records_checked, kind: "number" },
            records_failed: { value: (row) => row.records_failed, kind: "number" },
            failure_rate: { value: (row) => row.failure_rate, kind: "number" },
            status: { value: (row) => row.status },
            timestamp: { value: (row) => row.timestamp, kind: "date" },
          }),
    [onSort, results, sort],
  );
  const handleSort = (field: QualitySortField, preferredDirection: SortDirection) => {
    if (onSort) onSort(field, preferredDirection);
    else setLocalSort((current) => nextSort(current, field, preferredDirection));
  };

  return (
    <div className="table-shell">
      <table className="data-table">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {showDataset && <SortableHeader label="Dataset" field="dataset" sort={sort} onSort={handleSort} />}
            <SortableHeader label="Rule" field="rule_id" sort={sort} onSort={handleSort} />
            <SortableHeader label="Type" field="rule_type" sort={sort} onSort={handleSort} />
            <SortableHeader label="Checked" field="records_checked" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Failed" field="records_failed" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Failure rate" field="failure_rate" sort={sort} preferredDirection="desc" onSort={handleSort} />
            <SortableHeader label="Status" field="status" sort={sort} onSort={handleSort} />
            <SortableHeader label="Recorded" field="timestamp" sort={sort} preferredDirection="desc" onSort={handleSort} />
          </tr>
        </thead>
        <tbody>
          {sorted.map((result) => (
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
