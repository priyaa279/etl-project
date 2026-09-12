import { Link } from "react-router-dom";
import type { SchemaDriftEvent, SchemaDriftSortField } from "../types/api";
import type { SortDirection, SortState } from "../utils/tableSorting";
import { formatDateTime, formatLabel } from "../utils/format";
import { SortableHeader } from "./SortableHeader";
import { StatusBadge } from "./StatusBadge";

export function SchemaDriftTable({
  events,
  sort,
  onSort,
  showDataset = true,
  caption = "Recent schema drift events",
}: {
  events: SchemaDriftEvent[];
  sort: SortState<SchemaDriftSortField>;
  onSort: (field: SchemaDriftSortField, preferredDirection: SortDirection) => void;
  showDataset?: boolean;
  caption?: string;
}) {
  return (
    <div className="table-shell">
      <table className="data-table">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {showDataset && <SortableHeader label="Dataset" field="dataset" sort={sort} onSort={onSort} />}
            <SortableHeader label="Level" field="schema_level" sort={sort} onSort={onSort} />
            <SortableHeader label="Drift type" field="drift_type" sort={sort} onSort={onSort} />
            <SortableHeader label="Policy" field="policy" sort={sort} onSort={onSort} />
            <SortableHeader label="Action" field="action_taken" sort={sort} onSort={onSort} />
            <SortableHeader label="Detected" field="detected_at" sort={sort} preferredDirection="desc" onSort={onSort} />
          </tr>
        </thead>
        <tbody>
          {events.map((event) => (
            <tr key={`${event.run_id}-${event.schema_level}-${event.drift_type}`}>
              {showDataset && <td><Link className="table-link" to={`/datasets/${event.dataset}`}>{event.dataset}</Link></td>}
              <td>{formatLabel(event.schema_level)}</td>
              <td>{formatLabel(event.drift_type)}</td>
              <td>{event.policy}</td>
              <td><StatusBadge status={event.action_taken} /></td>
              <td>{formatDateTime(event.detected_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
