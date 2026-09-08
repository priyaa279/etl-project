import { AlertTriangle, ArrowLeft, Check, FileCode2, KeyRound } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { DetailGrid } from "../components/DetailGrid";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { ErrorState, LoadingState } from "../components/PageState";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import type { OnboardingField, OnboardingReview, SchemaDecision } from "../types/api";
import { formatLabel, formatNumber } from "../utils/format";

const dateFormats = [
  ["%Y-%m-%d", "YYYY-MM-DD"],
  ["%d/%m/%Y", "DD/MM/YYYY"],
  ["%m/%d/%Y", "MM/DD/YYYY"],
  ["%Y/%m/%d", "YYYY/MM/DD"],
  ["%Y-%m-%d %H:%M:%S", "YYYY-MM-DD HH:mm:ss"],
  ["%Y-%m-%dT%H:%M:%S", "YYYY-MM-DDTHH:mm:ss"],
] as const;

function reasonLabel(reason: string | null) {
  if (!reason) return "Structural proposal";
  const labels: Record<string, string> = {
    ambiguous_date_format: "Ambiguous date format",
    leading_zeros: "Leading zeros detected",
    non_empty_null_token: "Observed explicit null token",
    mixed_numeric_and_text_values: "Mixed numeric and text values",
    nested_structure_requires_explicit_normalization: "Nested structure requires explicit normalization",
    possible_key_candidate: "Unique with no nulls in the sample",
  };
  return labels[reason] ?? formatLabel(reason);
}

function SchemaFieldEditor({
  field,
  supportedTypes,
  onSave,
  onKeyDecision,
}: {
  field: OnboardingField;
  supportedTypes: string[];
  onSave: (field: string, decision: SchemaDecision) => Promise<void>;
  onKeyDecision: (field: string, decision: "accepted" | "rejected") => Promise<void>;
}) {
  const [canonicalName, setCanonicalName] = useState(field.canonical_name);
  const [datatype, setDatatype] = useState(field.datatype);
  const [nullable, setNullable] = useState(field.nullable);
  const [format, setFormat] = useState(field.format ?? "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const dateLike = datatype === "date" || datatype === "timestamp";
  const leadingZeroRisk = field.profile.leading_zeros_detected && ["integer", "decimal"].includes(datatype);

  const save = async () => {
    setBusy(true);
    setError(null);
    try {
      await onSave(field.canonical_name, {
        canonical_name: canonicalName,
        datatype,
        nullable,
        format: dateLike ? format || null : null,
      });
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "The schema decision could not be saved.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <article id={`field-${field.canonical_name}`} className="schema-card" tabIndex={-1}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs font-bold uppercase tracking-wide text-slate-500">{field.source_name}</p>
          <h3 className="mt-1 text-lg font-bold text-slate-950">{field.canonical_name}</h3>
        </div>
        <StatusBadge status={field.review_resolved ? "REVIEWED" : field.review_required ? "REVIEW REQUIRED" : "PROPOSED"} />
      </div>
      <div className="mt-4 grid gap-4 md:grid-cols-2">
        <label className="field-label">Canonical column
          <input className="field-control" value={canonicalName} disabled={!field.editable} onChange={(event) => setCanonicalName(event.target.value)} />
        </label>
        <label className="field-label">Datatype
          <select className="field-control" value={datatype} disabled={!field.editable} onChange={(event) => { setDatatype(event.target.value); if (!["date", "timestamp"].includes(event.target.value)) setFormat(""); }}>
            {supportedTypes.map((type) => <option key={type} value={type}>{formatLabel(type)}</option>)}
          </select>
        </label>
        <label className="inline-flex items-center gap-2 text-sm font-bold text-slate-700">
          <input type="checkbox" checked={nullable} disabled={!field.editable} onChange={(event) => setNullable(event.target.checked)} /> Nullable
        </label>
        {dateLike && (
          <label className="field-label">Date or timestamp format
            <select className="field-control" value={format} disabled={!field.editable} onChange={(event) => setFormat(event.target.value)}>
              <option value="">Choose format</option>
              {dateFormats.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
            </select>
          </label>
        )}
      </div>
      <div className="mt-4 grid gap-3 rounded-xl bg-slate-50 p-4 text-sm text-slate-600 md:grid-cols-3">
        <p><strong>Confidence</strong><br />{formatLabel(field.confidence)}</p>
        <p><strong>Reason</strong><br />{reasonLabel(field.reason)}</p>
        <p><strong>Null rate</strong><br />{field.profile.null_percentage ?? 0}%</p>
      </div>
      {(field.profile.observed_examples?.length ?? 0) > 0 && (
        <div className="mt-4"><p className="text-xs font-bold uppercase tracking-wide text-slate-500">Observed examples</p><div className="mt-2 flex flex-wrap gap-2">{field.profile.observed_examples?.map((example) => <code key={example} className="example-chip">{example}</code>)}</div></div>
      )}
      {leadingZeroRisk && <p className="warning-panel mt-4" role="alert">This may remove leading zeros.</p>}
      {!field.editable && <p className="info-panel mt-4">Nested structure is descriptive only in this phase. Explicit normalization is required later.</p>}
      {field.editable && (
        <button className="secondary-button mt-4" type="button" disabled={busy || (dateLike && !format)} onClick={() => void save()}>
          <Check className="h-4 w-4" aria-hidden="true" />{busy ? "Saving…" : "Save schema decision"}
        </button>
      )}
      {field.key_candidate && (
        <div className="key-candidate mt-4">
          <div><p className="font-bold text-slate-900">Potential key</p><p className="text-sm text-slate-600">No nulls and unique in the sample. This does not configure upsert or SCD2.</p></div>
          <div className="flex gap-2">
            <button className={field.key_decision === "accepted" ? "primary-button" : "secondary-button"} type="button" onClick={() => void onKeyDecision(field.canonical_name, "accepted")}>Accept suggestion</button>
            <button className={field.key_decision === "rejected" ? "primary-button" : "secondary-button"} type="button" onClick={() => void onKeyDecision(field.canonical_name, "rejected")}>Reject suggestion</button>
          </div>
        </div>
      )}
      {error && <p className="blocked-panel mt-4" role="alert">{error}</p>}
    </article>
  );
}

export function ReviewCenterPage() {
  const { onboardingId = "" } = useParams();
  const reviewLoader = useCallback(() => api.onboardingReview(onboardingId), [onboardingId]);
  const capabilityLoader = useCallback(() => api.onboardingCapability(), []);
  const reviewState = useApi(reviewLoader);
  const capability = useApi(capabilityLoader);
  const [review, setReview] = useState<OnboardingReview | null>(null);
  const [mode, setMode] = useState<"understanding" | "yaml">("understanding");
  const [yaml, setYaml] = useState<string | null>(null);
  const [yamlError, setYamlError] = useState<string | null>(null);
  const current = review ?? reviewState.data;

  useEffect(() => {
    if (mode !== "yaml") return;
    void api.onboardingYAML(onboardingId).then((result) => setYaml(result.yaml)).catch((reason: unknown) => setYamlError(reason instanceof Error ? reason.message : "YAML could not be loaded."));
  }, [mode, onboardingId, review]);

  if (reviewState.loading || capability.loading) return <LoadingState label="Loading Review Center" />;
  if (reviewState.error) return <ErrorState message={reviewState.error} onRetry={reviewState.reload} />;
  if (capability.error) return <ErrorState message={capability.error} onRetry={capability.reload} />;
  if (!current || !capability.data) return null;
  const supportedTypes = capability.data.supported_datatypes;
  const canConfigure = current.progress.remaining === 0 || (
    current.source_type === "json" &&
    current.unresolved.length > 0 &&
    current.unresolved.every((item) => item.reason === "nested_structure_requires_explicit_normalization")
  );

  const saveSchema = async (field: string, decision: SchemaDecision) => {
    setReview(await api.updateOnboardingSchema(onboardingId, field, decision));
  };
  const saveKey = async (field: string, decision: "accepted" | "rejected") => {
    setReview(await api.saveKeyDecision(onboardingId, field, decision));
  };

  return (
    <>
      <Link to={`/onboarding/${encodeURIComponent(onboardingId)}`} className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700 hover:text-cyan-900"><ArrowLeft className="h-4 w-4" aria-hidden="true" />Profile summary</Link>
      <PageHeader eyebrow="Human Review Center" title={current.dataset} description="Understand and resolve profiler proposals while persisted YAML remains authoritative." action={<StatusBadge status={current.status} />} />
      <div className="panel mb-6 flex gap-1 p-2" role="tablist" aria-label="Review modes">
        <button className={mode === "understanding" ? "review-tab review-tab-active" : "review-tab"} role="tab" aria-selected={mode === "understanding"} onClick={() => setMode("understanding")}>Understanding View</button>
        <button className={mode === "yaml" ? "review-tab review-tab-active" : "review-tab"} role="tab" aria-selected={mode === "yaml"} onClick={() => setMode("yaml")}><FileCode2 className="h-4 w-4" aria-hidden="true" />YAML View</button>
      </div>

      {mode === "yaml" ? (
        <section className="panel p-6"><SectionHeader title="Current draft YAML" description="Read-only, persisted draft—not a reconstructed preview." />{yamlError ? <p className="blocked-panel">{yamlError}</p> : yaml ? <pre className="yaml-preview">{yaml}</pre> : <LoadingState label="Loading draft YAML" />}</section>
      ) : (
        <div className="space-y-6">
          <section><SectionHeader title="Overview" /><DetailGrid items={[
            { label: "Dataset", value: current.dataset },
            { label: "Source", value: formatLabel(current.source_type) },
            { label: "Original file", value: current.original_filename },
            { label: "Columns", value: formatNumber(current.column_count) },
            { label: "Rows scanned", value: formatNumber(current.rows_scanned) },
            { label: "Exact duplicates", value: formatNumber(current.exact_duplicate_count) },
          ]} /></section>

          <section className="panel p-6">
            <div className="flex flex-wrap items-center justify-between gap-3"><SectionHeader title="Review Required" description={`${current.progress.reviewed} of ${current.progress.total} decisions reviewed`} /><span className="text-sm font-bold text-slate-600">{current.progress.remaining} remaining</span></div>
            {current.unresolved.length ? <ul className="review-list">{current.unresolved.map((item) => <li key={`${item.kind}-${item.field}`}><a href={`#field-${item.field}`}><AlertTriangle className="h-4 w-4" aria-hidden="true" /><span><strong>{item.field}</strong><br />{reasonLabel(item.reason)}</span></a></li>)}</ul> : <p className="success-panel">Schema review complete.</p>}
          </section>

          <section><SectionHeader title="Schema" description="Every explanation and statistic comes from the persisted profiler proposal." /><div className="grid gap-5">{current.fields.map((field) => <SchemaFieldEditor key={field.canonical_name} field={field} supportedTypes={supportedTypes} onSave={saveSchema} onKeyDecision={saveKey} />)}</div></section>

          <section className="panel p-6">
            <div className="flex items-start gap-3"><KeyRound className="mt-0.5 h-5 w-5 text-cyan-700" aria-hidden="true" /><div><h2 className="font-bold text-slate-950">Review Summary</h2><p className="mt-1 text-sm text-slate-600">The draft remains unapproved. Continue to configure normalization, transformations, quality rules, loading, and drift policy.</p></div></div>
            {canConfigure ? (
              <Link className="primary-button mt-4" to={`/onboarding/${encodeURIComponent(onboardingId)}/configure`}>Continue configuration</Link>
            ) : (
              <button className="secondary-button mt-4" type="button" disabled>Continue configuration</button>
            )}
          </section>
        </div>
      )}
    </>
  );
}
