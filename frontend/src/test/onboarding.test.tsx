import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../api/client";
import { AppShell } from "../layouts/AppShell";
import { OnboardingNewPage, OnboardingSessionPage } from "../pages/OnboardingPage";
import { ConfigurationBuilderPage } from "../pages/ConfigurationBuilderPage";
import { ReviewCenterPage } from "../pages/ReviewCenterPage";
import type {
  OnboardingCapability,
  OnboardingCompletion,
  OnboardingConfiguration,
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

const configuration: OnboardingConfiguration = {
  onboarding_id: "onboarding-123",
  dataset: "course_enrollments",
  source_type: "csv",
  status: "CONFIGURING",
  review_complete: true,
  normalization_complete: true,
  activation_ready: true,
  columns: [
    { name: "student_id", source: "Student ID", datatype: "string", nullable: false, format: null, classification: null, quarantine_value: "none" },
    { name: "enrolled_on", source: "Enrolled On", datatype: "date", nullable: false, format: "%Y-%m-%d", classification: null, quarantine_value: "none" },
  ],
  post_transformation_columns: [
    { name: "student_id", datatype: "string" },
    { name: "enrolled_on", datatype: "date" },
  ],
  accepted_key_candidates: ["student_id"],
  normalization: null,
  transformations: [],
  contracts: [],
  load: { strategy: "full", connection_env: "ETL_POSTGRES_DSN", schema: "public", staging_table: "course_enrollments_staging", target_table: "course_enrollments" },
  schema_drift: { added_columns: "warn", removed_columns: "fail", datatype_change: "fail", canonical_change: "fail", raw_structure_change: "warn" },
  orchestration: { enabled: true, schedule: null, retries: 0, retry_delay_minutes: 5 },
  validation: { result: "NOT_VALIDATED", draft_hash: "a".repeat(64), validated_hash: null, validated_at: null, errors: [] },
  final_approved: false,
  approval: {
    approved_at: null,
    approved_by: null,
    validation_hash: null,
    approved_config_hash: null,
    git_commit_sha: null,
    git_push_status: null,
    dag_id: null,
    activation_checked_at: null,
  },
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

it("adds, edits, removes, and reorders existing transformation operators", async () => {
  const user = userEvent.setup();
  const existing = {
    ...configuration,
    transformations: [
      { id: "T001", type: "filter", condition: "student_id IS NOT NULL" },
      { id: "T002", type: "cast", column: "student_id", datatype: "string" },
    ],
  };
  vi.spyOn(api, "onboardingConfiguration").mockResolvedValue(existing);
  const add = vi.spyOn(api, "addOnboardingTransformation").mockResolvedValue(existing);
  const edit = vi.spyOn(api, "updateOnboardingTransformation").mockResolvedValue(existing);
  const remove = vi.spyOn(api, "deleteOnboardingTransformation").mockResolvedValue(existing);
  const move = vi.spyOn(api, "moveOnboardingTransformation").mockResolvedValue(existing);
  renderRoute(<ConfigurationBuilderPage />, "/onboarding/onboarding-123/configure", "/onboarding/:onboardingId/configure");

  expect(await screen.findByRole("heading", { name: "Transformations" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Move T002 up" }));
  expect(move).toHaveBeenCalledWith("onboarding-123", "T002", "up");
  await user.click(screen.getAllByRole("button", { name: "Edit" })[0]);
  await user.clear(screen.getByLabelText("SQL condition"));
  await user.type(screen.getByLabelText("SQL condition"), "student_id <> ''");
  await user.click(screen.getByRole("button", { name: "Save transformation" }));
  expect(edit).toHaveBeenCalledWith("onboarding-123", "T001", expect.objectContaining({ type: "filter" }));
  await user.click(screen.getAllByRole("button", { name: "Remove" })[0]);
  await user.click(screen.getByRole("button", { name: "Confirm remove" }));
  expect(remove).toHaveBeenCalledWith("onboarding-123", "T001");
  await user.selectOptions(screen.getByLabelText("Operator"), "derive");
  await user.type(screen.getByLabelText("Target column"), "student_label");
  await user.type(screen.getByLabelText("SQL expression"), "student_id");
  await user.click(screen.getByRole("button", { name: "Add transformation" }));
  expect(add).toHaveBeenCalledWith("onboarding-123", expect.objectContaining({ type: "derive", target_column: "student_label" }));
});

it("shows load-specific fields without silently applying accepted key suggestions", async () => {
  const user = userEvent.setup();
  vi.spyOn(api, "onboardingConfiguration").mockResolvedValue(configuration);
  const update = vi.spyOn(api, "updateOnboardingLoad").mockResolvedValue(configuration);
  renderRoute(<ConfigurationBuilderPage />, "/onboarding/onboarding-123/configure", "/onboarding/:onboardingId/configure");

  await screen.findByRole("heading", { name: "Load Strategy" });
  expect(screen.getByText(/Accepted profiler suggestions: student_id/)).toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("Strategy"), "upsert");
  expect(screen.getByLabelText("Business keys")).toHaveValue("");
  await user.type(screen.getByLabelText("Business keys"), "student_id");
  await user.click(screen.getByRole("button", { name: "Save load strategy" }));
  expect(update).toHaveBeenCalledWith("onboarding-123", { strategy: "upsert", keys: ["student_id"] });
});

it("shows actionable validation results and blocks final approval", async () => {
  const user = userEvent.setup();
  const invalid = { ...configuration, status: "VALIDATION_FAILED", validation: { ...configuration.validation, result: "INVALID" as const, validated_hash: configuration.validation.draft_hash, errors: [{ section: "load", message: "Incremental load requires a watermark." }] } };
  vi.spyOn(api, "onboardingConfiguration").mockResolvedValue(configuration);
  vi.spyOn(api, "validateOnboardingConfiguration").mockResolvedValue(invalid);
  renderRoute(<ConfigurationBuilderPage />, "/onboarding/onboarding-123/configure", "/onboarding/:onboardingId/configure");

  await user.click(await screen.findByRole("button", { name: "Validate configuration" }));
  expect(await screen.findByText(/Incremental load requires a watermark/)).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Review final approval" })).toBeDisabled();
  expect(screen.getByText(/successful validation is required/)).toBeInTheDocument();
});

it("requires acknowledgment and a second confirmation before exact-hash approval", async () => {
  const user = userEvent.setup();
  const ready = {
    ...configuration,
    status: "READY_FOR_APPROVAL",
    validation: {
      ...configuration.validation,
      result: "VALID" as const,
      validated_hash: configuration.validation.draft_hash,
    },
  };
  const approved = {
    ...ready,
    status: "READY_FOR_FIRST_RUN",
    final_approved: true,
    approval: {
      approved_at: "2026-09-07T12:00:00Z",
      approved_by: "Priya A",
      validation_hash: ready.validation.draft_hash,
      approved_config_hash: "b".repeat(64),
      git_commit_sha: "c".repeat(40),
      git_push_status: "DISABLED",
      dag_id: "etl_course_enrollments",
      activation_checked_at: "2026-09-07T12:00:01Z",
    },
  };
  const completion = {
    onboarding_id: "onboarding-123",
    dataset: "course_enrollments",
    status: "READY_FOR_FIRST_RUN",
    safe_error: null,
    approval: approved.approval,
    first_run: {
      attempt: 0,
      correlation_id: null,
      airflow_dag_id: "etl_course_enrollments",
      airflow_run_id: null,
      airflow_state: null,
      etl_run_id: null,
      completed_at: null,
      etl_run: null,
    },
  };
  vi.spyOn(api, "onboardingConfiguration").mockResolvedValueOnce(ready).mockResolvedValue(approved);
  vi.spyOn(api, "onboardingCompletion").mockResolvedValue(completion);
  const approve = vi.spyOn(api, "approveOnboarding").mockResolvedValue(completion);
  const run = vi.spyOn(api, "runOnboardingFirst").mockResolvedValue({ ...completion, status: "QUEUED" });
  renderRoute(<ConfigurationBuilderPage />, "/onboarding/onboarding-123/configure", "/onboarding/:onboardingId/configure");

  const reviewApproval = await screen.findByRole("button", { name: "Review final approval" });
  expect(reviewApproval).toBeDisabled();
  await user.type(screen.getByLabelText("Approved by"), "Priya A");
  await user.click(screen.getByRole("checkbox"));
  await user.click(reviewApproval);
  expect(approve).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Confirm and approve" }));
  expect(approve).toHaveBeenCalledWith("onboarding-123", {
    expected_hash: ready.validation.draft_hash,
    approved_by: "Priya A",
    acknowledged: true,
  });
  expect(run).not.toHaveBeenCalled();
  await user.click(await screen.findByRole("button", { name: "Run first ETL load" }));
  expect(run).toHaveBeenCalledWith("onboarding-123", false);
});

it("reconciles the page status with the polled first-run completion", async () => {
  const approved = {
    ...configuration,
    status: "QUEUED",
    final_approved: true,
    approval: {
      approved_at: "2026-09-07T12:00:00Z",
      approved_by: "Priya A",
      validation_hash: configuration.validation.draft_hash,
      approved_config_hash: "b".repeat(64),
      git_commit_sha: "c".repeat(40),
      git_push_status: "DISABLED",
      dag_id: "etl_course_enrollments",
      activation_checked_at: "2026-09-07T12:00:01Z",
    },
  } as OnboardingConfiguration;
  const completion: OnboardingCompletion = {
    onboarding_id: "onboarding-123",
    dataset: "course_enrollments",
    status: "SUCCEEDED",
    safe_error: null,
    approval: approved.approval,
    first_run: {
      attempt: 1,
      correlation_id: "onboarding:onboarding-123:first-run:1",
      airflow_dag_id: "etl_course_enrollments",
      airflow_run_id: "onboarding__onboarding123__1",
      airflow_state: "SUCCESS",
      etl_run_id: "RUN_FIRST",
      completed_at: "2026-09-07T12:01:00Z",
      etl_run: null,
    },
  };
  vi.spyOn(api, "onboardingConfiguration").mockResolvedValue(approved);
  vi.spyOn(api, "onboardingCompletion").mockResolvedValue(completion);

  renderRoute(<ConfigurationBuilderPage />, "/onboarding/onboarding-123/configure", "/onboarding/:onboardingId/configure");

  expect((await screen.findAllByText("SUCCEEDED")).length).toBeGreaterThan(0);
  expect(screen.queryByText("QUEUED")).not.toBeInTheDocument();
});

it("requires explicit nested JSON normalization before later configuration", async () => {
  const user = userEvent.setup();
  const nested = { ...configuration, source_type: "json", review_complete: false, normalization_complete: false, columns: [{ ...configuration.columns[0], name: "order", source: "order" }] };
  const normalized = { ...nested, review_complete: true, normalization_complete: true };
  vi.spyOn(api, "onboardingConfiguration").mockResolvedValue(nested);
  const update = vi.spyOn(api, "updateOnboardingNormalization").mockResolvedValue(normalized);
  renderRoute(<ConfigurationBuilderPage />, "/onboarding/onboarding-123/configure", "/onboarding/:onboardingId/configure");

  expect(await screen.findByRole("heading", { name: "JSON Normalization" })).toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "Transformations" })).not.toBeInTheDocument();
  await user.type(screen.getByLabelText("Base field mappings"), "order_id=order.id");
  await user.clear(screen.getByLabelText("Canonical columns"));
  await user.type(screen.getByLabelText("Canonical columns"), "order_id:string:required");
  await user.click(screen.getByRole("button", { name: "Save normalization" }));
  expect(update).toHaveBeenCalledWith("onboarding-123", expect.objectContaining({ fields: { order_id: "order.id" } }));
});
