import type {
  DatasetSummary,
  DatasetWatermarkResponse,
  Overview,
  QualityOverview,
  QualitySummary,
  RunDetail,
  SchemaDriftEvent,
  WatermarkState,
} from "../types/api";

const apiBaseUrl = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    let message = "The control center could not load this data.";
    try {
      const payload = (await response.json()) as { detail?: string };
      if (payload.detail) message = payload.detail;
    } catch {
      // Keep the safe fallback when an upstream response is not JSON.
    }
    throw new ApiError(message, response.status);
  }
  return (await response.json()) as T;
}

function query(parameters: Record<string, string | number | undefined>): string {
  const values = new URLSearchParams();
  Object.entries(parameters).forEach(([key, value]) => {
    if (value !== undefined && value !== "") values.set(key, String(value));
  });
  const encoded = values.toString();
  return encoded ? `?${encoded}` : "";
}

export const api = {
  overview: () => request<Overview>("/api/overview"),
  datasets: () => request<DatasetSummary[]>("/api/datasets"),
  dataset: (dataset: string) =>
    request<DatasetSummary>(`/api/datasets/${encodeURIComponent(dataset)}`),
  datasetRuns: (dataset: string, limit = 20) =>
    request<RunDetail[]>(
      `/api/datasets/${encodeURIComponent(dataset)}/runs${query({ limit })}`,
    ),
  datasetQuality: (dataset: string, limit = 100) =>
    request<QualitySummary[]>(
      `/api/datasets/${encodeURIComponent(dataset)}/quality${query({ limit })}`,
    ),
  datasetSchemaDrift: (dataset: string, limit = 50) =>
    request<SchemaDriftEvent[]>(
      `/api/datasets/${encodeURIComponent(dataset)}/schema-drift${query({ limit })}`,
    ),
  datasetWatermark: (dataset: string) =>
    request<DatasetWatermarkResponse>(
      `/api/datasets/${encodeURIComponent(dataset)}/watermark`,
    ),
  runs: (filters: {
    limit?: number;
    dataset?: string;
    status?: string;
    load_strategy?: string;
  }) => request<RunDetail[]>(`/api/runs${query(filters)}`),
  run: (runId: string) => request<RunDetail>(`/api/runs/${encodeURIComponent(runId)}`),
  runQuality: (runId: string) =>
    request<QualitySummary[]>(`/api/runs/${encodeURIComponent(runId)}/quality`),
  quality: (days = 30) => request<QualityOverview>(`/api/quality${query({ days })}`),
  schemaDrift: (limit = 100) =>
    request<SchemaDriftEvent[]>(`/api/schema-drift${query({ limit })}`),
  watermarks: () => request<WatermarkState[]>("/api/watermarks"),
};
