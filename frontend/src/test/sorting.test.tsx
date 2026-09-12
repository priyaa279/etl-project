import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../api/client";
import { DatasetTable } from "../components/DatasetTable";
import { QualityTable } from "../components/QualityTable";
import { SortableHeader } from "../components/SortableHeader";
import { RunsPage } from "../pages/RunsPage";
import { SchemaDriftPage } from "../pages/SchemaDriftPage";
import { WatermarksPage } from "../pages/WatermarksPage";
import type { DatasetSummary, QualitySummary, RunDetail } from "../types/api";
import { nextSort, sortRows, type SortState } from "../utils/tableSorting";

const dataset = (name: string, loaded: number, started: string): DatasetSummary => ({
  dataset: name,
  health_status: "HEALTHY",
  latest_run_status: "SUCCEEDED",
  latest_run_id: `RUN_${name}`,
  latest_run_time: started,
  latest_duration_seconds: loaded / 10,
  latest_rows_extracted: loaded,
  latest_rows_loaded: loaded,
  latest_rows_quarantined: 0,
  latest_drift_status: "NONE",
  latest_watermark: null,
  last_successful_run: started,
  source_type: "csv",
  load_strategy: "full",
});

const run: RunDetail = {
  run_id: "RUN_001",
  dataset: "term_course",
  status: "SUCCEEDED",
  started_at: "2026-09-10T10:00:00Z",
  completed_at: "2026-09-10T10:00:02Z",
  duration_seconds: 2,
  source_type: "csv",
  load_strategy: "full",
  run_mode: "normal",
  backfill_from: null,
  backfill_to: null,
  rows_extracted: 47,
  rows_transformed: 47,
  rows_contract_passed: 47,
  rows_quarantined: 0,
  rows_loaded: 47,
  rows_inserted: 47,
  rows_updated: 0,
  rows_expired: 0,
  rows_history_inserted: 0,
  raw_schema_hash: "raw",
  canonical_schema_hash: "canonical",
  drift_status: "NONE",
  watermark_before: null,
  watermark_after: null,
  git_commit_sha: "sha",
  config_hash: "hash",
};

afterEach(() => vi.restoreAllMocks());

it("sorts text, numbers, dates, and nulls consistently without mutating input", () => {
  const rows = [
    { name: "beta", count: 10, date: "2026-09-10T00:00:00Z" },
    { name: "Alpha", count: 2, date: "2026-09-11T00:00:00Z" },
    { name: null, count: null, date: null },
  ];
  const columns = {
    name: { value: (item: (typeof rows)[number]) => item.name },
    count: { value: (item: (typeof rows)[number]) => item.count, kind: "number" as const },
    date: { value: (item: (typeof rows)[number]) => item.date, kind: "date" as const },
  };

  expect(sortRows(rows, { field: "name", direction: "asc" }, columns).map((item) => item.name)).toEqual(["Alpha", "beta", null]);
  expect(sortRows(rows, { field: "count", direction: "desc" }, columns).map((item) => item.count)).toEqual([10, 2, null]);
  expect(sortRows(rows, { field: "date", direction: "asc" }, columns).map((item) => item.date)).toEqual(["2026-09-10T00:00:00Z", "2026-09-11T00:00:00Z", null]);
  expect(rows[0].name).toBe("beta");
});

function HeaderHarness() {
  const [sort, setSort] = React.useState<SortState<"dataset">>({
    field: "dataset",
    direction: "asc",
  });
  return (
    <table><thead><tr><SortableHeader label="Dataset" field="dataset" sort={sort} onSort={(field, preferred) => setSort((current) => nextSort(current, field, preferred))} /></tr></thead></table>
  );
}

it("exposes accessible state and toggles with keyboard activation", async () => {
  const user = userEvent.setup();
  render(<HeaderHarness />);
  const button = screen.getByRole("button", { name: /sorted by dataset ascending/i });

  expect(button.closest("th")).toHaveAttribute("aria-sort", "ascending");
  expect(button).toHaveTextContent("↑");
  button.focus();
  await user.keyboard("{Enter}");
  expect(button.closest("th")).toHaveAttribute("aria-sort", "descending");
  expect(button).toHaveTextContent("↓");
  await user.keyboard(" ");
  expect(button.closest("th")).toHaveAttribute("aria-sort", "ascending");
});

it("sorts the shared Overview and Datasets table by dataset and numeric values", async () => {
  const user = userEvent.setup();
  render(
    <MemoryRouter>
      <DatasetTable datasets={[
        dataset("Zulu", 2, "2026-09-09T00:00:00Z"),
        dataset("alpha", 100, "2026-09-11T00:00:00Z"),
        dataset("Beta", 10, "2026-09-10T00:00:00Z"),
      ]} />
    </MemoryRouter>,
  );
  const table = screen.getByRole("table");
  const firstDataset = () => within(table).getAllByRole("row")[1].querySelector("td")?.textContent;

  expect(firstDataset()).toContain("alpha");
  await user.click(screen.getByRole("button", { name: /sorted by dataset ascending/i }));
  expect(firstDataset()).toContain("Zulu");
  await user.click(screen.getByRole("button", { name: /sort by loaded descending/i }));
  expect(firstDataset()).toContain("alpha");
  await user.click(screen.getByRole("button", { name: /sorted by loaded descending/i }));
  expect(firstDataset()).toContain("Zulu");
});

function LocationState() {
  return <output data-testid="location">{useLocation().search}</output>;
}

it("keeps Runs filters in URL state and sends server sort parameters", async () => {
  const runs = vi.spyOn(api, "runs").mockResolvedValue([run]);
  const user = userEvent.setup();
  render(
    <MemoryRouter initialEntries={["/runs?search=term&status=SUCCEEDED&load_strategy=full"]}>
      <Routes><Route path="/runs" element={<><RunsPage /><LocationState /></>} /></Routes>
    </MemoryRouter>,
  );

  await screen.findByRole("link", { name: "RUN_001" });
  expect(runs).toHaveBeenLastCalledWith(expect.objectContaining({ sort: "started_at", direction: "desc", search: "term", status: "SUCCEEDED", load_strategy: "full" }));
  await user.click(screen.getByRole("button", { name: /sort by duration descending/i }));
  await waitFor(() => expect(runs).toHaveBeenLastCalledWith(expect.objectContaining({ sort: "duration_seconds", direction: "desc", search: "term", status: "SUCCEEDED", load_strategy: "full" })));
  expect(screen.getByTestId("location")).toHaveTextContent("search=term");
  expect(screen.getByTestId("location")).toHaveTextContent("sort=duration_seconds");
  await user.click(screen.getByRole("button", { name: /sorted by duration descending/i }));
  await waitFor(() => expect(runs).toHaveBeenLastCalledWith(expect.objectContaining({ sort: "duration_seconds", direction: "asc" })));
});

it("sorts fully loaded quality and watermark tables client-side", async () => {
  const user = userEvent.setup();
  const quality: QualitySummary[] = [
    { run_id: "RUN_1", dataset: "alpha", rule_id: "DQ1", rule_type: "range", records_checked: 100, records_failed: 2, failure_rate: 2, status: "FAILED", timestamp: "2026-09-10T00:00:00Z" },
    { run_id: "RUN_2", dataset: "beta", rule_id: "DQ2", rule_type: "regex", records_checked: 10, records_failed: 10, failure_rate: 100, status: "FAILED", timestamp: "2026-09-11T00:00:00Z" },
  ];
  const { unmount } = render(<QualityTable results={quality} />);
  expect(screen.getAllByRole("row")[1]).toHaveTextContent("beta");
  await user.click(screen.getByRole("button", { name: /sort by failed descending/i }));
  expect(screen.getAllByRole("row")[1]).toHaveTextContent("beta");
  await user.click(screen.getByRole("button", { name: /sorted by failed descending/i }));
  expect(screen.getAllByRole("row")[1]).toHaveTextContent("alpha");
  unmount();

  vi.spyOn(api, "watermarks").mockResolvedValue([
    { dataset: "zulu", watermark_column: "updated_at", last_successful_value: "2", updated_at: "2026-09-10T00:00:00Z", run_id: "RUN_Z" },
    { dataset: "alpha", watermark_column: "updated_at", last_successful_value: "10", updated_at: "2026-09-11T00:00:00Z", run_id: "RUN_A" },
  ]);
  render(<MemoryRouter><WatermarksPage /></MemoryRouter>);
  await screen.findByRole("link", { name: "alpha" });
  expect(screen.getAllByRole("row")[1]).toHaveTextContent("alpha");
  await user.click(screen.getByRole("button", { name: /sorted by dataset ascending/i }));
  expect(screen.getAllByRole("row")[1]).toHaveTextContent("zulu");
});

it("requests schema-drift sorting from the server", async () => {
  const drift = vi.spyOn(api, "schemaDrift").mockResolvedValue([
    { dataset: "customers", run_id: "RUN_1", schema_level: "raw", drift_type: "column_added", policy: "warn", action_taken: "WARNED", detected_at: "2026-09-11T00:00:00Z", old_schema_hash: "old", new_schema_hash: "new" },
  ]);
  const user = userEvent.setup();
  render(<MemoryRouter><SchemaDriftPage /></MemoryRouter>);

  await screen.findByRole("link", { name: "customers" });
  expect(drift).toHaveBeenLastCalledWith(200, "detected_at", "desc", "", "", "");
  await user.click(screen.getByRole("button", { name: /sort by policy ascending/i }));
  await waitFor(() => expect(drift).toHaveBeenLastCalledWith(200, "policy", "asc", "", "", ""));
});
