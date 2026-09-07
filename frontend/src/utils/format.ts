export function formatNumber(value: number | null | undefined): string {
  return value == null ? "—" : new Intl.NumberFormat("en-US").format(value);
}

export function formatPercent(value: number | null | undefined): string {
  return value == null ? "—" : `${value.toFixed(1)}%`;
}

export function formatDuration(value: number | null | undefined): string {
  if (value == null) return "—";
  if (value < 60) return `${value.toFixed(2)}s`;
  return `${Math.floor(value / 60)}m ${Math.round(value % 60)}s`;
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

export function formatLabel(value: string | null | undefined): string {
  if (!value) return "—";
  const technicalLabels: Record<string, string> = {
    csv: "CSV",
    json: "JSON",
    parquet: "Parquet",
    postgres: "PostgreSQL",
    postgresql: "PostgreSQL",
    full: "Full",
    incremental: "Incremental",
    upsert: "Upsert",
    scd2: "SCD2",
  };
  const technicalLabel = technicalLabels[value.toLowerCase()];
  if (technicalLabel) return technicalLabel;
  return value.replaceAll("_", " ").replace(/\b\w/g, (character) => character.toUpperCase());
}

export function shortRunId(value: string): string {
  return value.length > 24 ? `${value.slice(0, 12)}…${value.slice(-8)}` : value;
}

export function shortHash(value: string | null | undefined): string {
  if (!value) return "Not recorded";
  return value.length > 18 ? `${value.slice(0, 12)}…${value.slice(-6)}` : value;
}
