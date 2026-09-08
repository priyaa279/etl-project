import { ArrowRight, Check, FileSearch, FileUp, Sparkles } from "lucide-react";
import { useCallback, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import { DetailGrid } from "../components/DetailGrid";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { ErrorState, LoadingState } from "../components/PageState";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import type { OnboardingSession } from "../types/api";
import { formatLabel, formatNumber } from "../utils/format";

function fileSize(bytes: number) {
  if (bytes < 1024) return `${bytes} bytes`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function Workflow({ active }: { active: number }) {
  return (
    <ol className="workflow-steps" aria-label="New dataset onboarding workflow">
      {["Dataset & File", "Profile", "Review Schema", "Review Summary"].map((label, index) => (
        <li
          key={label}
          className={index + 1 <= active ? "workflow-step workflow-step-complete" : "workflow-step"}
        >
          <span>{index + 1 < active ? <Check className="h-4 w-4" aria-hidden="true" /> : index + 1}</span>
          {label}
        </li>
      ))}
    </ol>
  );
}

export function OnboardingNewPage() {
  const navigate = useNavigate();
  const loader = useCallback(() => api.onboardingCapability(), []);
  const capability = useApi(loader);
  const [datasetName, setDatasetName] = useState("");
  const [sourceType, setSourceType] = useState("csv");
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const create = async () => {
    if (!file) return;
    setBusy(true);
    setError(null);
    try {
      const result = await api.createOnboarding(datasetName, sourceType, file);
      navigate(`/onboarding/${encodeURIComponent(result.onboarding_id)}`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Onboarding could not be started.");
    } finally {
      setBusy(false);
    }
  };

  if (capability.loading) return <LoadingState label="Loading onboarding capability" />;
  if (capability.error) return <ErrorState message={capability.error} onRetry={capability.reload} />;
  if (!capability.data) return null;

  return (
    <>
      <PageHeader
        eyebrow="New dataset onboarding"
        title="Onboard New Dataset"
        description="Create a new draft configuration by profiling a source that has never been onboarded before."
      />
      <Workflow active={1} />
      {!capability.data.enabled ? (
        <div className="info-panel" role="note">{capability.data.reason}</div>
      ) : (
        <section className="panel p-6">
          <SectionHeader
            title="1. Dataset & File"
            description="For an existing approved dataset, use Upload new data from that dataset instead."
          />
          <div className="grid gap-5 lg:grid-cols-2">
            <label className="field-label">
              Dataset name
              <input
                className="field-control"
                value={datasetName}
                pattern="[a-z][a-z0-9_]{0,62}"
                placeholder="course_enrollments"
                onChange={(event) => setDatasetName(event.target.value)}
              />
              <span>Lowercase letters, digits, and underscores only.</span>
            </label>
            <label className="field-label">
              Source type
              <select
                className="field-control"
                value={sourceType}
                onChange={(event) => setSourceType(event.target.value)}
              >
                {capability.data.source_types.map((type) => (
                  <option key={type} value={type}>{formatLabel(type)}</option>
                ))}
              </select>
              <span>PostgreSQL onboarding is intentionally deferred.</span>
            </label>
          </div>
          <label className="file-picker mt-6">
            <FileUp className="h-7 w-7 text-cyan-700" aria-hidden="true" />
            <span className="font-bold text-slate-900">Choose a new dataset file</span>
            <input
              type="file"
              accept={sourceType === "json" ? ".json,.jsonl,.ndjson" : `.${sourceType}`}
              onChange={(event) => setFile(event.target.files?.[0] ?? null)}
            />
          </label>
          {file && (
            <div className="mt-5 flex flex-wrap items-center justify-between gap-4">
              <div>
                <p className="font-bold text-slate-900">{file.name}</p>
                <p className="text-sm text-slate-500">{fileSize(file.size)} · new {formatLabel(sourceType)} dataset</p>
              </div>
              <button
                className="primary-button"
                type="button"
                disabled={busy || !datasetName}
                onClick={() => void create()}
              >
                <Sparkles className="h-4 w-4" aria-hidden="true" />
                {busy ? "Creating draft…" : "Start onboarding"}
              </button>
            </div>
          )}
        </section>
      )}
      {error && <div className="blocked-panel mt-5" role="alert">{error}</div>}
    </>
  );
}

export function OnboardingSessionPage() {
  const { onboardingId = "" } = useParams();
  const loader = useCallback(() => api.onboardingSession(onboardingId), [onboardingId]);
  const state = useApi(loader);
  const [session, setSession] = useState<OnboardingSession | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const current = session ?? state.data;

  const profile = async () => {
    setBusy(true);
    setError(null);
    try {
      setSession(await api.profileOnboarding(onboardingId));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Profiling could not be completed.");
    } finally {
      setBusy(false);
    }
  };

  if (state.loading && !current) return <LoadingState label="Resuming onboarding" />;
  if (state.error) return <ErrorState message={state.error} onRetry={state.reload} />;
  if (!current) return null;
  const profiled = Boolean(current.profile_summary);

  return (
    <>
      <PageHeader
        eyebrow="New dataset onboarding"
        title={current.proposed_dataset_name}
        description="The source is isolated from runnable datasets while the profiler proposes a draft."
        action={<StatusBadge status={current.status} />}
      />
      <Workflow active={profiled ? 3 : 2} />
      <DetailGrid items={[
        { label: "Source", value: formatLabel(current.source_type) },
        { label: "File", value: current.original_filename },
        { label: "Size", value: fileSize(current.size_bytes) },
        { label: "Status", value: <StatusBadge status={current.status} /> },
      ]} />
      {!profiled && current.status !== "FAILED" && (
        <section className="panel mt-6 p-6">
          <SectionHeader title="2. Profile" description="Run the existing generic profiler and starter-config generator." />
          <button className="primary-button" type="button" disabled={busy} onClick={() => void profile()}>
            <FileSearch className="h-4 w-4" aria-hidden="true" />
            {busy ? "Profiling…" : "Profile dataset"}
          </button>
        </section>
      )}
      {profiled && current.profile_summary && (
        <section className="panel mt-6 p-6">
          <SectionHeader title="Profile complete" description="Review the structural proposals before configuration continues." />
          <DetailGrid items={[
            { label: "Columns", value: formatNumber(current.profile_summary.column_count) },
            { label: "Rows scanned", value: formatNumber(current.profile_summary.rows_scanned) },
            { label: "Rows sampled", value: formatNumber(current.profile_summary.rows_profiled) },
            { label: "Exact duplicates", value: formatNumber(current.profile_summary.exact_duplicate_count) },
            { label: "Possible keys", value: formatNumber(current.profile_summary.possible_key_candidates) },
            { label: "Profiler decisions", value: formatNumber(current.profile_summary.required_decisions) },
          ]} />
          {current.profile_summary.nested_structure_detected && (
            <p className="info-panel mt-4">Nested structure detected. Explicit normalization is required before this dataset can be approved.</p>
          )}
          <Link className="primary-button mt-5" to={`/onboarding/${encodeURIComponent(onboardingId)}/review`}>
            Review configuration <ArrowRight className="h-4 w-4" aria-hidden="true" />
          </Link>
        </section>
      )}
      {(error || current.safe_error) && <div className="blocked-panel mt-5" role="alert">{error ?? current.safe_error}</div>}
    </>
  );
}
