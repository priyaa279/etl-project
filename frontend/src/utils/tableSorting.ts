export type SortDirection = "asc" | "desc";

export interface SortState<Field extends string> {
  field: Field;
  direction: SortDirection;
}

export type SortKind = "text" | "number" | "date";
export type SortValue = string | number | null | undefined;

export interface SortColumn<T> {
  value: (row: T) => SortValue;
  kind?: SortKind;
}

export function nextSort<Field extends string>(
  current: SortState<Field>,
  field: Field,
  preferredDirection: SortDirection = "asc",
): SortState<Field> {
  if (current.field !== field) return { field, direction: preferredDirection };
  return { field, direction: current.direction === "asc" ? "desc" : "asc" };
}

function comparePresentValues(left: string | number, right: string | number, kind: SortKind) {
  if (kind === "number") return Number(left) - Number(right);
  if (kind === "date") return Date.parse(String(left)) - Date.parse(String(right));
  return String(left).localeCompare(String(right), undefined, {
    sensitivity: "base",
    numeric: true,
  });
}

export function sortRows<T, Field extends string>(
  rows: T[],
  state: SortState<Field>,
  columns: Record<Field, SortColumn<T>>,
): T[] {
  const column = columns[state.field];
  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const leftValue = column.value(left.row);
      const rightValue = column.value(right.row);
      const leftMissing = leftValue === null || leftValue === undefined || leftValue === "";
      const rightMissing = rightValue === null || rightValue === undefined || rightValue === "";
      if (leftMissing || rightMissing) {
        if (leftMissing && rightMissing) return left.index - right.index;
        return leftMissing ? 1 : -1;
      }
      const compared = comparePresentValues(leftValue, rightValue, column.kind ?? "text");
      if (compared === 0) return left.index - right.index;
      return state.direction === "asc" ? compared : -compared;
    })
    .map(({ row }) => row);
}
