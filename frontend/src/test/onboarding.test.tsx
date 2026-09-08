import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../api/client";
import { AppShell } from "../layouts/AppShell";
import { OnboardingNewPage, OnboardingSessionPage } from "../pages/OnboardingPage";
import { ReviewCenterPage } from "../pages/ReviewCenterPage";
import type {
  OnboardingCapability,
  OnboardingReview,
  OnboardingSession,
} from "../types/api";

const capability: OnboardingCapability = {
  enabled: true,
  source_types: ["csv", "json", "parquet"],
  supported_datatypes: ["boolean", "date", "decimal", "integer", "string", "timestamp"],
  reason: null,
};

const uploaded: OnboardingSession = {
  onboarding_id: "onboarding-123",
  proposed_dataset_name: "course_enrollments",
  source_type: "csv",
  original_filename: "students.csv",
  size_bytes: 200,
  sha256: "a".repeat(64),
  status: "UPLOADED",
  uploaded_at: "2026-09-07T10:00:00Z",
  profiled_at: null,
  updated_at: "2026-09-07T10:00:00Z",
  safe_error: null,
  profile_summary: null,
};

const profiled: OnboardingSession = {
  ...uploaded,
  status: "NEEDS_REVIEW",
  profiled_at: "2026-09-07T10:00:01Z",
  profile_summary: {
    rows_scanned: 3,
    rows_profiled: 3,
    exact_duplicate_count: 0,
    column_count: 2,
    possible_key_candidates: 1,
    required_decisions: 1,
    nested_structure_detected: false,
  },
};

const review: OnboardingReview = {
  onboarding_id: uploaded.onboarding_id,
  dataset: uploaded.proposed_dataset_name,
  source_type: "csv",
  original_filename: uploaded.original_filename,
  size_bytes: uploaded.size_bytes,
  status: "NEEDS_REVIEW",
  rows_scanned: 3,
  rows_profiled: 3,
  exact_duplicate_count: 0,
  column_count: 2,
  nested_fields: [],
  progress: { reviewed: 0, total: 2, remaining: 2 },
  unresolved: [
    { kind: "schema", field: "birth_date", reason: "ambiguous_date_format" },
    { kind: "key", field: "student_id", reason: "possible_key_candidate" },
  ],
  final_approved: false,
  fields: [
    {
      source_name: "Student ID",
      canonical_name: "student_id",
      datatype: "string",
      nullable: false,
      format: null,
      confidence: "high",
      reason: "leading_zeros",
      review_required: false,
      review_resolved: false,
      review_reasons: [],
      profile: {
        null_percentage: 0,
        distinct_count: 3,
        leading_zeros_detected: true,
        possible_key_candidate: true,
        observed_examples: ["00123", "00451"],
      },
      key_candidate: true,
      key_decision: null,
      editable: true,
    },
    {
      source_name: "Birth Date",
      canonical_name: "birth_date",
      datatype: "date",
      nullable: false,
      format: null,
      confidence: "medium",
      reason: "ambiguous_date_format",
      review_required: true,
      review_resolved: false,
      review_reasons: ["ambiguous_date_format"],
      profile: {
        null_percentage: 0,
        distinct_count: 3,
        observed_examples: ["01/02/2001", "05/07/1999"],
      },
      key_candidate: false,
      key_decision: null,
      editable: true,
    },
  ],
};

afterEach(() => vi.restoreAllMocks());

function renderRoute(element: React.ReactNode, path: string, route: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes><Route path={route} element={element} /></Routes>
    </MemoryRouter>,
  );
}

it("shows the onboarding entry point only when its gate is enabled", async () => {
  vi.spyOn(api, "onboardingCapability").mockResolvedValue(capability);
  const view = render(
    <MemoryRouter initialEntries={["/"]}>
      <Routes><Route element={<AppShell />}><Route index element={<p>Home</p>} /></Route></Routes>
    </MemoryRouter>,
  );
  expect(await screen.findByRole("link", { name: "Onboard Dataset" })).toBeInTheDocument();

  view.unmount();
  vi.mocked(api.onboardingCapability).mockResolvedValue({
    ...capability,
    enabled: false,
    reason: "New-dataset onboarding is disabled in this environment.",
  });
  render(
    <MemoryRouter initialEntries={["/"]}>
      <Routes><Route element={<AppShell />}><Route index element={<p>Home</p>} /></Route></Routes>
    </MemoryRouter>,
  );
  expect(await screen.findByText("Home")).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "Onboard Dataset" })).not.toBeInTheDocument();
});

it("uploads a completely new dataset and keeps existing-data language separate", async () => {
  const user = userEvent.setup();
  vi.spyOn(api, "onboardingCapability").mockResolvedValue(capability);
  const create = vi.spyOn(api, "createOnboarding").mockResolvedValue(uploaded);
  renderRoute(<OnboardingNewPage />, "/onboarding/new", "/onboarding/new");

  await user.type(await screen.findByLabelText(/Dataset name/), "course_enrollments");
  await user.upload(
    screen.getByLabelText(/Choose a new dataset file/),
    new File(["id\n001\n"], "students.csv", { type: "text/csv" }),
  );
  expect(screen.getByText(/For an existing approved dataset/)).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Start onboarding" }));

  expect(create).toHaveBeenCalledWith("course_enrollments", "csv", expect.any(File));
});

it("resumes an uploaded session and shows the real profile summary", async () => {
  const user = userEvent.setup();
  vi.spyOn(api, "onboardingSession").mockResolvedValue(uploaded);
  let finishProfile!: (value: OnboardingSession) => void;
  const pendingProfile = new Promise<OnboardingSession>((resolve) => {
    finishProfile = resolve;
  });
  const profile = vi.spyOn(api, "profileOnboarding").mockReturnValue(pendingProfile);
  renderRoute(
    <OnboardingSessionPage />,
    "/onboarding/onboarding-123",
    "/onboarding/:onboardingId",
  );

  await user.click(await screen.findByRole("button", { name: "Profile dataset" }));
  expect(screen.getByRole("button", { name: "Profiling…" })).toBeDisabled();
  finishProfile(profiled);
  expect(profile).toHaveBeenCalledWith("onboarding-123");
  expect(await screen.findByText("Profile complete")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: /Review configuration/ })).toBeInTheDocument();
});

it("shows a safe profiler failure", async () => {
  const user = userEvent.setup();
  vi.spyOn(api, "onboardingSession").mockResolvedValue(uploaded);
  vi.spyOn(api, "profileOnboarding").mockRejectedValue(
    new Error("The source could not be profiled."),
  );
  renderRoute(
    <OnboardingSessionPage />,
    "/onboarding/onboarding-123",
    "/onboarding/:onboardingId",
  );

  await user.click(await screen.findByRole("button", { name: "Profile dataset" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("could not be profiled");
});

it("reviews ambiguous dates, warns on leading-zero conversion, and saves key decisions", async () => {
  const user = userEvent.setup();
  vi.spyOn(api, "onboardingReview").mockResolvedValue(review);
  vi.spyOn(api, "onboardingCapability").mockResolvedValue(capability);
  const updateSchema = vi.spyOn(api, "updateOnboardingSchema").mockResolvedValue(review);
  const updateKey = vi.spyOn(api, "saveKeyDecision").mockResolvedValue(review);
  renderRoute(
    <ReviewCenterPage />,
    "/onboarding/onboarding-123/review",
    "/onboarding/:onboardingId/review",
  );

  expect(await screen.findByText("Review Required")).toBeInTheDocument();
  expect(screen.getByText("0 of 2 decisions reviewed")).toBeInTheDocument();
  const student = screen.getByRole("heading", { name: "student_id" }).closest("article")!;
  await user.selectOptions(within(student).getByLabelText("Datatype"), "integer");
  expect(within(student).getByRole("alert")).toHaveTextContent("remove leading zeros");
  await user.click(within(student).getByRole("button", { name: "Accept suggestion" }));
  expect(updateKey).toHaveBeenCalledWith("onboarding-123", "student_id", "accepted");
  await user.click(within(student).getByRole("button", { name: "Reject suggestion" }));
  expect(updateKey).toHaveBeenCalledWith("onboarding-123", "student_id", "rejected");

  const birthDate = screen.getByRole("heading", { name: "birth_date" }).closest("article")!;
  await user.selectOptions(within(birthDate).getByLabelText("Date or timestamp format"), "%d/%m/%Y");
  await user.click(within(birthDate).getByRole("button", { name: "Save schema decision" }));
  expect(updateSchema).toHaveBeenCalledWith(
    "onboarding-123",
    "birth_date",
    expect.objectContaining({ format: "%d/%m/%Y", datatype: "date" }),
  );
  expect(screen.queryByRole("button", { name: /Approve Dataset/ })).not.toBeInTheDocument();
});

it("loads the actual read-only YAML and supports direct refresh into review", async () => {
  const user = userEvent.setup();
  vi.spyOn(api, "onboardingReview").mockResolvedValue(review);
  vi.spyOn(api, "onboardingCapability").mockResolvedValue(capability);
  const yaml = vi.spyOn(api, "onboardingYAML").mockResolvedValue({
    onboarding_id: "onboarding-123",
    yaml: "review:\n  required: true\n  approved: false\n",
  });
  renderRoute(
    <ReviewCenterPage />,
    "/onboarding/onboarding-123/review",
    "/onboarding/:onboardingId/review",
  );

  expect(await screen.findByRole("heading", { name: "course_enrollments" })).toBeInTheDocument();
  await user.click(screen.getByRole("tab", { name: "YAML View" }));

  expect(await screen.findByText(/approved: false/)).toBeInTheDocument();
  expect(yaml).toHaveBeenCalledWith("onboarding-123");
  expect(screen.queryByRole("textbox", { name: /YAML/ })).not.toBeInTheDocument();
});
