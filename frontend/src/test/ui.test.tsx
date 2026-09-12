import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { DatasetDetailPage } from "../pages/DatasetDetailPage";
import { DatasetsPage } from "../pages/DatasetsPage";
import { OverviewPage } from "../pages/OverviewPage";
import { RunDetailPage } from "../pages/RunDetailPage";
import { UploadPage } from "../pages/UploadPage";
import type {
  DatasetSummary,
  DatasetUploadCapability,
  Overview,
  PipelineSummary,
  RunDetail,
  TrustedDataPreview,
  UploadOperation,
} from "../types/api";
import { formatLabel, shortRunId } from "../utils/format";

const dataset: DatasetSummary = {
  dataset: "customers",
  health_status: "HEALTHY",
  latest_run_status: "SUCCEEDED",
  latest_run_id: "RUN_001",
  latest_run_time: "2026-09-07T10:00:00Z",
  latest_duration_seconds: 1.25,
  latest_rows_extracted: 4,
  latest_rows_loaded: 3,
  latest_rows_quarantined: 0,
  latest_drift_status: "NONE",
  latest_watermark: null,
  last_successful_run: "2026-09-07T10:00:01Z",
  source_type: "csv",
  load_strategy: "full",
};

const run: RunDetail = {
  run_id: "RUN_001",
  dataset: "customers",
  status: "SUCCEEDED",
  started_at: "2026-09-07T10:00:00Z",
  completed_at: "2026-09-07T10:00:01Z",
  duration_seconds: 1.25,
  source_type: "csv",
  load_strategy: "full",
  run_mode: "normal",
  backfill_from: null,
  backfill_to: null,
  rows_extracted: 4,
  rows_transformed: 3,
  rows_contract_passed: 3,
  rows_quarantined: 1,
  rows_loaded: 3,
  rows_inserted: 3,
  rows_updated: 0,
  rows_expired: 0,
  rows_history_inserted: 0,
  raw_schema_hash: "raw-hash",
  canonical_schema_hash: "canonical-hash",
  drift_status: "NONE",
  watermark_before: null,
  watermark_after: null,
  git_commit_sha: "git-sha",
  config_hash: "config-hash",
};

const overview: Overview = {
  time_scope_hours: 24,
  total_datasets: 14,
  healthy_datasets: 10,
  warning_datasets: 4,
  failed_datasets: 0,
  unknown_datasets: 0,
  total_recent_runs: 20,
  success_rate: 95,
  total_rows_loaded: 120,
  total_rows_quarantined: 5,
  datasets_with_drift_warnings: 2,
};

const capability: DatasetUploadCapability = {
  dataset: "customers",
  source_type: "csv",
  load_strategy: "full",
  config_approved: true,
  upload_eligible: true,
  reason: null,
};

const pipelineSummary: PipelineSummary = {
  dataset: "customers",
  label: "Current Transformation Plan",
  transformations: [
    {
      position: 1,
      id: "T001",
      type: "map",
      details: { column: "status", mappings: { A: "ACTIVE", I: "INACTIVE" } },
    },
    {
      position: 2,
      id: "T002",
      type: "derive",
      details: {
        target_column: "customer_label",
        datatype: "string",
        expression: "customer_id || '-' || status",
      },
    },
  ],
};

const trustedPreview: TrustedDataPreview = {
  dataset: "customers",
  target: "public.customers",
  state: "AVAILABLE",
  columns: [
    { name: "customer_id", data_type: "text", classification: null, redacted: false },
    { name: "note", data_type: "text", classification: null, redacted: false },
    { name: "email", data_type: "text", classification: "pii", redacted: true },
  ],
  rows: [{ customer_id: "00123", note: null, email: null }],
  total_rows: 1,
  limit: 25,
  offset: 0,
  has_more: false,
  latest_successful_run_id: "RUN_001",
  last_updated: "2026-09-07T10:00:01Z",
};

function mockDatasetDetail(summary: PipelineSummary | null = pipelineSummary) {
  vi.spyOn(api, "dataset").mockResolvedValue(dataset);
  vi.spyOn(api, "datasetRuns").mockResolvedValue([run]);
  vi.spyOn(api, "datasetQuality").mockResolvedValue([]);
  vi.spyOn(api, "datasetSchemaDrift").mockResolvedValue([]);
  vi.spyOn(api, "datasetWatermark").mockResolvedValue({ dataset: "customers", watermark: null });
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  vi.spyOn(api, "pipelineSummary").mockResolvedValue(summary ?? { ...pipelineSummary, transformations: [] });
}

function renderDatasetDetail() {
  return renderAt(
    <Routes><Route path="/datasets/:dataset" element={<DatasetDetailPage />} /></Routes>,
    "/datasets/customers",
  );
}

const uploadOperation: UploadOperation = {
  upload_id: "upload-123",
  dataset: "customers",
  original_filename: "customers_new.csv",
  source_type: "csv",
  size_bytes: 128,
  sha256: "a".repeat(64),
  status: "UPLOADED",
  uploaded_at: "2026-09-07T10:00:00Z",
  validated_at: null,
  triggered_at: null,
  airflow_dag_id: null,
  airflow_run_id: null,
  airflow_state: null,
  etl_run_id: null,
  completed_at: null,
  safe_error: null,
  preflight_result: null,
  etl_run: null,
};

function renderAt(element: React.ReactNode, path = "/") {
  return render(<MemoryRouter initialEntries={[path]}>{element}</MemoryRouter>);
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("status language", () => {
  it("uses text and an accessible label in addition to color", () => {
    render(<StatusBadge status="WARNING" />);

    expect(screen.getByLabelText("Status: WARNING")).toHaveTextContent("WARNING");
  });
});

describe("technical labels", () => {
  it("uses conventional source and load-strategy casing", () => {
    expect(formatLabel("csv")).toBe("CSV");
    expect(formatLabel("json")).toBe("JSON");
    expect(formatLabel("postgres")).toBe("PostgreSQL");
    expect(formatLabel("scd2")).toBe("SCD2");
  });

  it("shortens long run IDs without changing short IDs", () => {
    expect(shortRunId("RUN_001")).toBe("RUN_001");
    expect(shortRunId("RUN_20260907T152722Z_E2918CBB")).toBe("RUN_20260907…E2918CBB");
  });
});

describe("overview states", () => {
  it("shows a loading state", () => {
    vi.spyOn(api, "overview").mockReturnValue(new Promise(() => undefined));
    vi.spyOn(api, "datasets").mockResolvedValue([]);
    vi.spyOn(api, "runs").mockResolvedValue([]);

    renderAt(<OverviewPage />);

    expect(screen.getByRole("status")).toHaveTextContent("Loading command center");
  });

  it("renders live summary, dataset, and run data", async () => {
    vi.spyOn(api, "overview").mockResolvedValue(overview);
    vi.spyOn(api, "datasets").mockResolvedValue([dataset]);
    vi.spyOn(api, "runs").mockResolvedValue([run]);

    renderAt(<OverviewPage />);

    expect(await screen.findByText("System overview")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: /customers/i })).toHaveLength(2);
    expect(screen.getByRole("link", { name: "RUN_001" })).toBeInTheDocument();
    expect(screen.getByText("95.0%")).toBeInTheDocument();
  });

  it("renders a safe error state with retry", async () => {
    vi.spyOn(api, "overview").mockRejectedValue(new Error("Service unavailable"));
    vi.spyOn(api, "datasets").mockResolvedValue([]);
    vi.spyOn(api, "runs").mockResolvedValue([]);

    renderAt(<OverviewPage />);

    expect(await screen.findByRole("alert")).toHaveTextContent("Service unavailable");
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});

it("filters the dataset inventory", async () => {
  vi.spyOn(api, "datasets").mockResolvedValue([
    dataset,
    { ...dataset, dataset: "sensor_readings", source_type: "parquet" },
  ]);
  const user = userEvent.setup();
  renderAt(<DatasetsPage />);

  await screen.findByRole("link", { name: /customers/i });
  await user.type(screen.getByPlaceholderText("Search datasets"), "sensor");

  expect(screen.queryByRole("link", { name: /customers/i })).not.toBeInTheDocument();
  expect(screen.getByRole("link", { name: /sensor_readings/i })).toBeInTheDocument();
});

it("renders dataset detail and a clean empty watermark state", async () => {
  vi.spyOn(api, "dataset").mockResolvedValue(dataset);
  vi.spyOn(api, "datasetRuns").mockResolvedValue([run]);
  vi.spyOn(api, "datasetQuality").mockResolvedValue([]);
  vi.spyOn(api, "datasetSchemaDrift").mockResolvedValue([]);
  vi.spyOn(api, "datasetWatermark").mockResolvedValue({ dataset: "customers", watermark: null });
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  const user = userEvent.setup();

  renderAt(
    <Routes>
      <Route path="/datasets/:dataset" element={<DatasetDetailPage />} />
    </Routes>,
    "/datasets/customers",
  );

  expect(await screen.findByRole("heading", { name: "customers" })).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Upload new data" })).toBeInTheDocument();
  await user.click(screen.getByRole("tab", { name: "Watermark" }));
  expect(screen.getByText("No watermark exists for this dataset")).toBeInTheDocument();
});

it("shows a non-error explanation instead of upload for PostgreSQL sources", async () => {
  vi.spyOn(api, "dataset").mockResolvedValue({ ...dataset, source_type: "postgres" });
  vi.spyOn(api, "datasetRuns").mockResolvedValue([run]);
  vi.spyOn(api, "datasetQuality").mockResolvedValue([]);
  vi.spyOn(api, "datasetSchemaDrift").mockResolvedValue([]);
  vi.spyOn(api, "datasetWatermark").mockResolvedValue({ dataset: "customers", watermark: null });
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue({
    ...capability,
    source_type: "postgres",
    upload_eligible: false,
    reason: "This dataset reads from PostgreSQL and does not accept file uploads.",
  });

  renderAt(
    <Routes><Route path="/datasets/:dataset" element={<DatasetDetailPage />} /></Routes>,
    "/datasets/customers",
  );

  expect(await screen.findByText(/does not accept file uploads/i)).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Upload new data" })).not.toBeInTheDocument();
});

it("keeps read-only dataset detail available when upload capability is unavailable", async () => {
  vi.spyOn(api, "dataset").mockResolvedValue(dataset);
  vi.spyOn(api, "datasetRuns").mockResolvedValue([run]);
  vi.spyOn(api, "datasetQuality").mockResolvedValue([]);
  vi.spyOn(api, "datasetSchemaDrift").mockResolvedValue([]);
  vi.spyOn(api, "datasetWatermark").mockResolvedValue({ dataset: "customers", watermark: null });
  vi.spyOn(api, "datasetUploadCapability").mockRejectedValue(new Error("Not configured"));

  renderAt(
    <Routes><Route path="/datasets/:dataset" element={<DatasetDetailPage />} /></Routes>,
    "/datasets/customers",
  );

  expect(await screen.findByRole("heading", { name: "customers" })).toBeInTheDocument();
  expect(screen.getByText("Operational upload is not available for this dataset.")).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Upload new data" })).not.toBeInTheDocument();
  expect(screen.getByRole("tab", { name: "Runs" })).toBeInTheDocument();
});

it("shows the current approved transformation plan in execution order", async () => {
  mockDatasetDetail();

  renderDatasetDetail();

  expect(await screen.findByText("Current Transformation Plan")).toBeInTheDocument();
  expect(screen.getByText("Map")).toBeInTheDocument();
  expect(screen.getByText("A → ACTIVE, I → INACTIVE")).toBeInTheDocument();
  expect(screen.getByText("Derive")).toBeInTheDocument();
  expect(screen.getByText("customer_label")).toBeInTheDocument();
  expect(screen.getByText(/not a historical reconstruction/i)).toBeInTheDocument();
});

it("renders actual trusted rows, leading zeros, NULL, privacy, and run navigation", async () => {
  mockDatasetDetail();
  vi.spyOn(api, "trustedData").mockResolvedValue(trustedPreview);
  const user = userEvent.setup();
  renderDatasetDetail();

  await user.click(await screen.findByRole("tab", { name: "Trusted Data" }));

  expect(await screen.findByText("00123")).toBeInTheDocument();
  expect(screen.getByText("NULL")).toBeInTheDocument();
  expect(screen.getByText("REDACTED")).toBeInTheDocument();
  expect(screen.getAllByText("public.customers")).toHaveLength(2);
  expect(screen.getByTestId("trusted-data-table-container")).toHaveClass("table-shell");
  expect(screen.getByRole("link", { name: "View Run Details" })).toHaveAttribute("href", "/runs/RUN_001");
  expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
});

it("shows trusted-data loading and advances through server-side pages", async () => {
  mockDatasetDetail();
  const firstRows = Array.from({ length: 25 }, (_, index) => ({
    customer_id: String(index + 1).padStart(5, "0"),
    note: null,
    email: null,
  }));
  let resolveFirstPage: ((value: TrustedDataPreview) => void) | undefined;
  const firstPage = new Promise<TrustedDataPreview>((resolve) => {
    resolveFirstPage = resolve;
  });
  const trusted = vi.spyOn(api, "trustedData")
    .mockReturnValueOnce(firstPage)
    .mockResolvedValueOnce({
      ...trustedPreview,
      rows: [{ customer_id: "00026", note: "last", email: null }],
      total_rows: 26,
      offset: 25,
      has_more: false,
    });
  const user = userEvent.setup();
  renderDatasetDetail();

  await user.click(await screen.findByRole("tab", { name: "Trusted Data" }));
  expect(screen.getByRole("status")).toHaveTextContent("Loading trusted records");
  resolveFirstPage?.({ ...trustedPreview, rows: firstRows, total_rows: 26, has_more: true });
  expect(await screen.findByText("Showing 1–25 of 26")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Next" }));

  expect(await screen.findByText("00026")).toBeInTheDocument();
  expect(screen.getByText("Showing 26–26 of 26")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Previous" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
  expect(trusted).toHaveBeenLastCalledWith("customers", 25, 25);
});

it("shows the conservative feature-disabled state", async () => {
  mockDatasetDetail();
  vi.spyOn(api, "trustedData").mockRejectedValue(
    new ApiError("Trusted data preview is disabled in this environment.", 403),
  );
  const user = userEvent.setup();
  renderDatasetDetail();

  await user.click(await screen.findByRole("tab", { name: "Trusted Data" }));

  expect(await screen.findByText(/preview is disabled/i)).toBeInTheDocument();
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});

it.each([
  ["EMPTY", "No trusted records are currently published"],
  ["NOT_PUBLISHED", "No trusted output has been published for this dataset yet"],
] as const)("renders the %s trusted-data state", async (state, message) => {
  mockDatasetDetail();
  vi.spyOn(api, "trustedData").mockResolvedValue({
    ...trustedPreview,
    state,
    rows: [],
    total_rows: 0,
    latest_successful_run_id: state === "EMPTY" ? "RUN_001" : null,
  });
  const user = userEvent.setup();
  renderDatasetDetail();

  await user.click(await screen.findByRole("tab", { name: "Trusted Data" }));

  expect(await screen.findByText(message)).toBeInTheDocument();
});

it("renders a safe trusted-data API error with retry", async () => {
  mockDatasetDetail();
  vi.spyOn(api, "trustedData").mockRejectedValue(new Error("Trusted data is temporarily unavailable."));
  const user = userEvent.setup();
  renderDatasetDetail();

  await user.click(await screen.findByRole("tab", { name: "Trusted Data" }));

  expect(await screen.findByRole("alert")).toHaveTextContent("Trusted data is temporarily unavailable.");
  expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
});

it("selects, uploads, validates READY, and triggers an existing dataset run", async () => {
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  vi.spyOn(api, "upload").mockResolvedValue(uploadOperation);
  vi.spyOn(api, "validateUpload").mockResolvedValue({
    ...uploadOperation,
    status: "READY",
    validated_at: "2026-09-07T10:00:01Z",
    preflight_result: {
      status: "READY",
      config_approved: true,
      source_valid: true,
      canonical_compatible: true,
      drift: { detected: false, status: "NONE", events: [] },
    },
  });
  vi.spyOn(api, "runUpload").mockResolvedValue({
    ...uploadOperation,
    status: "QUEUED",
    validated_at: "2026-09-07T10:00:01Z",
    triggered_at: "2026-09-07T10:00:02Z",
    airflow_state: "QUEUED",
    preflight_result: {
      status: "READY",
      config_approved: true,
      source_valid: true,
      canonical_compatible: true,
      drift: { detected: false, status: "NONE", events: [] },
    },
  });
  const user = userEvent.setup();
  renderAt(
    <Routes><Route path="/datasets/:dataset/upload" element={<UploadPage />} /></Routes>,
    "/datasets/customers/upload",
  );

  const input = await screen.findByLabelText(/choose csv file/i);
  await user.upload(input, new File(["a,b\n1,2"], "customers_new.csv", { type: "text/csv" }));
  expect(screen.getByText("customers_new.csv")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Upload file" }));
  await user.click(await screen.findByRole("button", { name: /validate/i }));
  expect(await screen.findByText("Ready to run")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Run ETL" }));
  expect((await screen.findAllByLabelText("Status: QUEUED")).length).toBeGreaterThan(0);
});

it.each(["WARNING", "BLOCKED"] as const)("renders a %s preflight safely", async (status) => {
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  vi.spyOn(api, "upload").mockResolvedValue(uploadOperation);
  vi.spyOn(api, "validateUpload").mockResolvedValue({
    ...uploadOperation,
    status,
    validated_at: "2026-09-07T10:00:01Z",
    preflight_result: {
      status,
      config_approved: true,
      source_valid: true,
      canonical_compatible: status !== "BLOCKED",
      drift: {
        detected: true,
        status: status === "WARNING" ? "WARN" : "FAILED",
        events: [{ type: "column_added", description: "Added column: phone", policy: status === "WARNING" ? "warn" : "fail", action: status === "WARNING" ? "WARNED" : "FAILED" }],
      },
    },
  });
  const user = userEvent.setup();
  renderAt(
    <Routes><Route path="/datasets/:dataset/upload" element={<UploadPage />} /></Routes>,
    "/datasets/customers/upload",
  );
  await user.upload(await screen.findByLabelText(/choose csv file/i), new File(["x"], "batch.csv"));
  await user.click(screen.getByRole("button", { name: "Upload file" }));
  await user.click(await screen.findByRole("button", { name: /validate/i }));

  expect(await screen.findByLabelText(`Status: ${status}`)).toBeInTheDocument();
  if (status === "BLOCKED") {
    expect(screen.queryByRole("button", { name: "Run ETL" })).not.toBeInTheDocument();
  } else {
    expect(screen.getByRole("button", { name: "Run ETL" })).toBeInTheDocument();
  }
});

it("shows a successful correlated result and links to existing run detail", async () => {
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  const succeeded: UploadOperation = {
    ...uploadOperation,
    status: "SUCCEEDED",
    validated_at: "2026-09-07T10:00:01Z",
    triggered_at: "2026-09-07T10:00:02Z",
    airflow_state: "SUCCESS",
    etl_run_id: "RUN_EXACT",
    preflight_result: { status: "READY", config_approved: true, source_valid: true, canonical_compatible: true, drift: { detected: false, status: "NONE", events: [] } },
    etl_run: { run_id: "RUN_EXACT", status: "SUCCEEDED", started_at: "2026-09-07T10:00:02Z", completed_at: "2026-09-07T10:00:03Z", duration_seconds: 1, rows_extracted: 4, rows_transformed: 3, rows_contract_passed: 3, rows_quarantined: 1, rows_loaded: 3, drift_status: "NONE" },
  };
  vi.spyOn(api, "upload").mockResolvedValue(succeeded);
  const user = userEvent.setup();
  renderAt(<Routes><Route path="/datasets/:dataset/upload" element={<UploadPage />} /></Routes>, "/datasets/customers/upload");
  await user.upload(await screen.findByLabelText(/choose csv file/i), new File(["x"], "batch.csv"));
  await user.click(screen.getByRole("button", { name: "Upload file" }));

  expect((await screen.findAllByLabelText("Status: SUCCEEDED")).length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "View Run Details" })).toHaveAttribute("href", "/runs/RUN_EXACT");
  expect(screen.getByText("Quarantined")).toBeInTheDocument();
});

it("renders a running upload monitor without invented progress", async () => {
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  vi.spyOn(api, "upload").mockResolvedValue({
    ...uploadOperation,
    status: "RUNNING",
    triggered_at: "2026-09-07T10:00:02Z",
    airflow_state: "RUNNING",
  });
  const user = userEvent.setup();
  renderAt(
    <Routes><Route path="/datasets/:dataset/upload" element={<UploadPage />} /></Routes>,
    "/datasets/customers/upload",
  );
  await user.upload(
    await screen.findByLabelText(/choose csv file/i),
    new File(["x"], "batch.csv"),
  );
  await user.click(screen.getByRole("button", { name: "Upload file" }));

  expect((await screen.findAllByLabelText("Status: RUNNING")).length).toBeGreaterThan(0);
  expect(screen.getByText("Checking every few seconds")).toBeInTheDocument();
  expect(screen.queryByText(/%/)).not.toBeInTheDocument();
});

it("renders a safe failed result", async () => {
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  vi.spyOn(api, "upload").mockResolvedValue({
    ...uploadOperation,
    status: "FAILED",
    triggered_at: "2026-09-07T10:00:02Z",
    airflow_state: "FAILED",
    safe_error: "The ETL run failed safely.",
  });
  const user = userEvent.setup();
  renderAt(
    <Routes><Route path="/datasets/:dataset/upload" element={<UploadPage />} /></Routes>,
    "/datasets/customers/upload",
  );
  await user.upload(
    await screen.findByLabelText(/choose csv file/i),
    new File(["x"], "batch.csv"),
  );
  await user.click(screen.getByRole("button", { name: "Upload file" }));

  expect((await screen.findAllByLabelText("Status: FAILED")).length).toBeGreaterThan(0);
  expect(screen.getByText("The ETL run failed safely.")).toBeInTheDocument();
});

it("shows a safe upload API error", async () => {
  vi.spyOn(api, "datasetUploadCapability").mockResolvedValue(capability);
  vi.spyOn(api, "upload").mockRejectedValue(new Error("Operational actions are disabled."));
  const user = userEvent.setup();
  renderAt(<Routes><Route path="/datasets/:dataset/upload" element={<UploadPage />} /></Routes>, "/datasets/customers/upload");
  await user.upload(await screen.findByLabelText(/choose csv file/i), new File(["x"], "batch.csv"));
  await user.click(screen.getByRole("button", { name: "Upload file" }));

  expect(await screen.findByRole("alert")).toHaveTextContent("Operational actions are disabled.");
});

it("renders run detail row flow and technical metadata", async () => {
  vi.spyOn(api, "run").mockResolvedValue(run);
  vi.spyOn(api, "runQuality").mockResolvedValue([]);

  renderAt(
    <Routes>
      <Route path="/runs/:runId" element={<RunDetailPage />} />
    </Routes>,
    "/runs/RUN_001",
  );

  expect(await screen.findByRole("heading", { name: "RUN_001" })).toBeInTheDocument();
  expect(screen.getByText("Contract passed")).toBeInTheDocument();
  expect(screen.getByText("raw-hash")).toBeInTheDocument();
  expect(screen.getByText("Quarantined")).toBeInTheDocument();
});
