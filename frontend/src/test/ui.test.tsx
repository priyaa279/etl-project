import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../api/client";
import { StatusBadge } from "../components/StatusBadge";
import { DatasetDetailPage } from "../pages/DatasetDetailPage";
import { DatasetsPage } from "../pages/DatasetsPage";
import { OverviewPage } from "../pages/OverviewPage";
import { RunDetailPage } from "../pages/RunDetailPage";
import type { DatasetSummary, Overview, RunDetail } from "../types/api";
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
  const user = userEvent.setup();

  renderAt(
    <Routes>
      <Route path="/datasets/:dataset" element={<DatasetDetailPage />} />
    </Routes>,
    "/datasets/customers",
  );

  expect(await screen.findByRole("heading", { name: "customers" })).toBeInTheDocument();
  await user.click(screen.getByRole("tab", { name: "Watermark" }));
  expect(screen.getByText("No watermark exists for this dataset")).toBeInTheDocument();
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
