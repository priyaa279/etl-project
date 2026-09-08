import type {
  DatasetSummary,
  DatasetUploadCapability,
  DatasetWatermarkResponse,
  Overview,
  QualityOverview,
  QualitySummary,
  RunDetail,
  SchemaDriftEvent,
  WatermarkState,
  UploadOperation,
  DraftYAML,
  OnboardingCapability,
  OnboardingConfiguration,
  OnboardingReview,
  OnboardingSession,
  SchemaDecision,
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

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    ...options,
    headers: { Accept: "application/json", ...options.headers },
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
  datasetUploadCapability: (dataset: string) =>
    request<DatasetUploadCapability>(
      `/api/datasets/${encodeURIComponent(dataset)}/upload-capability`,
    ),
  upload: (dataset: string, file: File) => {
    const body = new FormData();
    body.append("file", file);
    return request<UploadOperation>(
      `/api/datasets/${encodeURIComponent(dataset)}/uploads`,
      { method: "POST", body },
    );
  },
  validateUpload: (uploadId: string) =>
    request<UploadOperation>(`/api/uploads/${encodeURIComponent(uploadId)}/validate`, {
      method: "POST",
    }),
  runUpload: (uploadId: string) =>
    request<UploadOperation>(`/api/uploads/${encodeURIComponent(uploadId)}/run`, {
      method: "POST",
    }),
  uploadStatus: (uploadId: string) =>
    request<UploadOperation>(`/api/uploads/${encodeURIComponent(uploadId)}`),
  onboardingCapability: () => request<OnboardingCapability>("/api/onboarding/capability"),
  createOnboarding: (datasetName: string, sourceType: string, file: File) => {
    const body = new FormData();
    body.append("dataset_name", datasetName);
    body.append("source_type", sourceType);
    body.append("file", file);
    return request<OnboardingSession>("/api/onboarding", { method: "POST", body });
  },
  onboardingSession: (onboardingId: string) =>
    request<OnboardingSession>(`/api/onboarding/${encodeURIComponent(onboardingId)}`),
  profileOnboarding: (onboardingId: string) =>
    request<OnboardingSession>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/profile`,
      { method: "POST" },
    ),
  onboardingReview: (onboardingId: string) =>
    request<OnboardingReview>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/review`,
    ),
  updateOnboardingSchema: (
    onboardingId: string,
    field: string,
    decision: SchemaDecision,
  ) =>
    request<OnboardingReview>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/schema/${encodeURIComponent(field)}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(decision),
      },
    ),
  saveKeyDecision: (onboardingId: string, field: string, decision: "accepted" | "rejected") =>
    request<OnboardingReview>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/key-decisions`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ field, decision }),
      },
    ),
  onboardingYAML: (onboardingId: string) =>
    request<DraftYAML>(`/api/onboarding/${encodeURIComponent(onboardingId)}/yaml`),
  onboardingConfiguration: (onboardingId: string) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/configuration`,
    ),
  updateOnboardingNormalization: (onboardingId: string, value: Record<string, unknown>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/normalization`,
      { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  addOnboardingTransformation: (onboardingId: string, value: Record<string, unknown>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/transformations`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  updateOnboardingTransformation: (onboardingId: string, id: string, value: Record<string, unknown>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/transformations/${encodeURIComponent(id)}`,
      { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  deleteOnboardingTransformation: (onboardingId: string, id: string) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/transformations/${encodeURIComponent(id)}`,
      { method: "DELETE" },
    ),
  moveOnboardingTransformation: (onboardingId: string, id: string, direction: "up" | "down") =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/transformations/${encodeURIComponent(id)}/move`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ direction }) },
    ),
  addOnboardingContract: (onboardingId: string, value: Record<string, unknown>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/contracts`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  updateOnboardingContract: (onboardingId: string, id: string, value: Record<string, unknown>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/contracts/${encodeURIComponent(id)}`,
      { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  deleteOnboardingContract: (onboardingId: string, id: string) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/contracts/${encodeURIComponent(id)}`,
      { method: "DELETE" },
    ),
  updateOnboardingPrivacy: (
    onboardingId: string,
    field: string,
    value: { classification: string | null; quarantine_value: "full" | "masked" | "hashed" | "none" },
  ) => request<OnboardingConfiguration>(
    `/api/onboarding/${encodeURIComponent(onboardingId)}/columns/${encodeURIComponent(field)}/privacy`,
    { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
  ),
  updateOnboardingLoad: (onboardingId: string, value: Record<string, unknown>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/load`,
      { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  updateOnboardingDrift: (onboardingId: string, value: Record<string, string>) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/schema-drift`,
      { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(value) },
    ),
  validateOnboardingConfiguration: (onboardingId: string) =>
    request<OnboardingConfiguration>(
      `/api/onboarding/${encodeURIComponent(onboardingId)}/validate`,
      { method: "POST" },
    ),
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
