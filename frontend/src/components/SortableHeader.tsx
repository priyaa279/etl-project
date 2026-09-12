import type { SortDirection, SortState } from "../utils/tableSorting";

export function SortableHeader<Field extends string>({
  label,
  field,
  sort,
  preferredDirection = "asc",
  onSort,
}: {
  label: string;
  field: Field;
  sort: SortState<Field>;
  preferredDirection?: SortDirection;
  onSort: (field: Field, preferredDirection: SortDirection) => void;
}) {
  const active = sort.field === field;
  const direction = active ? sort.direction : null;
  const nextDirection = active && direction === "asc" ? "descending" : preferredDirection === "desc" ? "descending" : "ascending";
  const ariaLabel = active
    ? `Sorted by ${label} ${direction === "asc" ? "ascending" : "descending"}. Activate to sort ${direction === "asc" ? "descending" : "ascending"}.`
    : `Sort by ${label} ${nextDirection}.`;

  return (
    <th scope="col" aria-sort={direction === "asc" ? "ascending" : direction === "desc" ? "descending" : "none"}>
      <button
        type="button"
        className={`sort-header ${active ? "sort-header-active" : ""}`}
        aria-label={ariaLabel}
        onClick={() => onSort(field, preferredDirection)}
      >
        <span>{label}</span>
        <span className="sort-indicator" aria-hidden="true">
          {direction === "asc" ? "↑" : direction === "desc" ? "↓" : "↕"}
        </span>
      </button>
    </th>
  );
}
