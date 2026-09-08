import {
  AlertTriangle,
  ArrowDown,
  ArrowLeft,
  ArrowUp,
  CheckCircle2,
  FileCode2,
  Pencil,
  ShieldCheck,
  Trash2,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { PageHeader, SectionHeader } from "../components/PageHeader";
import { ErrorState, LoadingState } from "../components/PageState";
import { StatusBadge } from "../components/StatusBadge";
import { useApi } from "../hooks/useApi";
import type { OnboardingConfiguration } from "../types/api";
import type { OnboardingCompletion } from "../types/api";
import { formatLabel } from "../utils/format";

const datatypes = ["string", "integer", "decimal", "boolean", "date", "timestamp"];
const driftSettings = [
  "added_columns",
  "removed_columns",
  "datatype_change",
  "canonical_change",
  "raw_structure_change",
];
const transformationHelp: Record<string, string> = {
  cast: "Converts one field using the engine's approved datatype rules.",
  filter: "Rows matching the condition remain in this output; this intentionally excludes other rows.",
  derive: "Creates a new field from existing values through the SQL transformation engine.",
  map: "Replaces configured source values with approved output values.",
  deduplicate: "Chooses one record from duplicate groups according to the configured ordering.",
};
const loadHelp: Record<string, string> = {
  full: "Replace and publish the complete trusted result.",
  incremental: "Process records newer than the successful watermark, which advances only after publish.",
  upsert: "Insert new business keys and update existing keys.",
  scd2: "Preserve previous versions when tracked attributes change.",
};

function list(value: string) {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function mappings(value: string) {
  return Object.fromEntries(
    value.split("\n").map((line) => line.trim()).filter(Boolean).map((line) => {
      const separator = line.indexOf("=");
      if (separator < 1) throw new Error("Each mapping must use source=result on its own line.");
      return [line.slice(0, separator).trim(), line.slice(separator + 1).trim()];
    }),
  );
}

function mappingText(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "";
  return Object.entries(value).map(([source, target]) => `${source}=${String(target)}`).join("\n");
}

function asString(value: unknown) {
  return value === undefined || value === null ? "" : String(value);
}

function OperatorList({
  items,
  noun,
  onEdit,
  onDelete,
  onMove,
}: {
  items: Record<string, unknown>[];
  noun: string;
  onEdit: (item: Record<string, unknown>) => void;
  onDelete: (id: string) => Promise<void>;
  onMove?: (id: string, direction: "up" | "down") => Promise<void>;
}) {
  const [confirming, setConfirming] = useState<string | null>(null);
  if (!items.length) return <p className="info-panel">No {noun.toLowerCase()} rules configured.</p>;
  return (
    <div className="grid gap-3">
      {items.map((item, index) => {
        const id = asString(item.id);
        return (
          <article className="rounded-xl border border-slate-200 bg-white p-4" key={id}>
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0 flex-1"><p className="font-bold text-slate-950">{id} · {formatLabel(asString(item.type))}</p><p className="mt-1 break-all text-sm text-slate-600">{JSON.stringify(item)}</p></div>
              <div className="flex flex-wrap gap-2">
                {onMove && <><button className="icon-button" type="button" aria-label={`Move ${id} up`} disabled={index === 0} onClick={() => void onMove(id, "up")}><ArrowUp className="h-4 w-4" /></button><button className="icon-button" type="button" aria-label={`Move ${id} down`} disabled={index === items.length - 1} onClick={() => void onMove(id, "down")}><ArrowDown className="h-4 w-4" /></button></>}
                <button className="secondary-button" type="button" onClick={() => onEdit(item)}><Pencil className="h-4 w-4" />Edit</button>
                {confirming === id ? <button className="danger-button" type="button" onClick={() => void onDelete(id)}>Confirm remove</button> : <button className="secondary-button" type="button" onClick={() => setConfirming(id)}><Trash2 className="h-4 w-4" />Remove</button>}
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}

function TransformationSection({ value, save }: { value: OnboardingConfiguration; save: (next: Promise<OnboardingConfiguration>) => Promise<void> }) {
  const [editing, setEditing] = useState<string | null>(null);
  const [id, setId] = useState("T001");
  const [type, setType] = useState("cast");
  const [column, setColumn] = useState(value.post_transformation_columns[0]?.name ?? "");
  const [datatype, setDatatype] = useState("string");
  const [format, setFormat] = useState("");
  const [condition, setCondition] = useState("");
  const [target, setTarget] = useState("");
  const [expression, setExpression] = useState("");
  const [mapRows, setMapRows] = useState([{ source: "", result: "" }]);
  const [defaultValue, setDefaultValue] = useState("");
  const [keys, setKeys] = useState("");
  const [orderColumn, setOrderColumn] = useState("");
  const [direction, setDirection] = useState("desc");
  const [error, setError] = useState<string | null>(null);
  const columns = value.post_transformation_columns;

  const reset = () => {
    setEditing(null); setId(`T${String(value.transformations.length + 1).padStart(3, "0")}`);
    setType("cast"); setColumn(columns[0]?.name ?? ""); setDatatype("string"); setFormat("");
    setCondition(""); setTarget(""); setExpression(""); setMapRows([{ source: "", result: "" }]); setDefaultValue("");
    setKeys(""); setOrderColumn(""); setDirection("desc"); setError(null);
  };
  const edit = (item: Record<string, unknown>) => {
    setEditing(asString(item.id)); setId(asString(item.id)); setType(asString(item.type));
    setColumn(asString(item.column)); setDatatype(asString(item.datatype) || "string");
    setFormat(asString(item.format)); setCondition(asString(item.condition));
    setTarget(asString(item.target_column)); setExpression(asString(item.expression));
    const existingMappings = item.mappings && typeof item.mappings === "object" && !Array.isArray(item.mappings) ? Object.entries(item.mappings).map(([source, result]) => ({ source, result: asString(result) })) : [{ source: "", result: "" }];
    setMapRows(existingMappings); setDefaultValue(asString(item.default));
    setKeys(Array.isArray(item.keys) ? item.keys.join(", ") : "");
    const order = item.order_by && typeof item.order_by === "object" ? Object.entries(item.order_by)[0] : undefined;
    setOrderColumn(order?.[0] ?? ""); setDirection(asString(order?.[1]) || "desc"); setError(null);
  };
  const submit = async () => {
    try {
      let payload: Record<string, unknown> = { id, type };
      if (type === "cast") payload = { ...payload, column, datatype, ...(format ? { format } : {}) };
      if (type === "filter") payload = { ...payload, condition };
      if (type === "derive") payload = { ...payload, target_column: target, datatype, expression, ...(format ? { format } : {}) };
      if (type === "map") payload = { ...payload, column, datatype, mappings: mapRows, ...(defaultValue ? { default: defaultValue } : {}) };
      if (type === "deduplicate") payload = { ...payload, keys: list(keys), order_by: { [orderColumn]: direction } };
      await save(editing ? api.updateOnboardingTransformation(value.onboarding_id, editing, payload) : api.addOnboardingTransformation(value.onboarding_id, payload));
      reset();
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Transformation could not be saved."); }
  };
  return (
    <section className="panel p-6" id="transformations">
      <SectionHeader title="Transformations" description="Add only the reusable operators already supported by the ETL engine. Order is execution order." />
      <OperatorList items={value.transformations} noun="Transformation" onEdit={edit} onDelete={(itemId) => save(api.deleteOnboardingTransformation(value.onboarding_id, itemId))} onMove={(itemId, move) => save(api.moveOnboardingTransformation(value.onboarding_id, itemId, move))} />
      <div className="mt-6 rounded-xl bg-slate-50 p-4">
        <h3 className="font-bold text-slate-950">{editing ? `Edit ${editing}` : "Add transformation"}</h3>
        <p className="mt-1 text-sm text-slate-600">{transformationHelp[type]}</p>
        <div className="mt-4 grid gap-4 md:grid-cols-3">
          <label className="field-label">Rule ID<input className="field-control" value={id} disabled={Boolean(editing)} onChange={(event) => setId(event.target.value)} /></label>
          <label className="field-label">Operator<select className="field-control" value={type} onChange={(event) => setType(event.target.value)}>{["cast", "filter", "derive", "map", "deduplicate"].map((item) => <option key={item}>{item}</option>)}</select></label>
          {["cast", "map"].includes(type) && <label className="field-label">Column<select className="field-control" value={column} onChange={(event) => setColumn(event.target.value)}>{columns.map((item) => <option key={item.name}>{item.name}</option>)}</select></label>}
          {["cast", "derive", "map"].includes(type) && <label className="field-label">Output datatype<select className="field-control" value={datatype} onChange={(event) => setDatatype(event.target.value)}>{datatypes.map((item) => <option key={item}>{item}</option>)}</select></label>}
          {type === "cast" && ["date", "timestamp"].includes(datatype) && <label className="field-label">Input format<input className="field-control" placeholder="%Y-%m-%d" value={format} onChange={(event) => setFormat(event.target.value)} /></label>}
          {type === "filter" && <label className="field-label md:col-span-3">SQL condition<input className="field-control" placeholder="status = 'ACTIVE'" value={condition} onChange={(event) => setCondition(event.target.value)} /></label>}
          {type === "derive" && <><label className="field-label">Target column<input className="field-control" value={target} onChange={(event) => setTarget(event.target.value)} /></label><label className="field-label md:col-span-2">SQL expression<input className="field-control" value={expression} onChange={(event) => setExpression(event.target.value)} /></label></>}
          {type === "map" && <><div className="md:col-span-2"><p className="field-label">Mapping table</p><div className="mt-2 grid gap-2">{mapRows.map((row, index) => <div className="grid grid-cols-[1fr_1fr_auto] gap-2" key={index}><label className="sr-only" htmlFor={`map-source-${index}`}>Source value {index + 1}</label><input id={`map-source-${index}`} className="field-control" placeholder="Source value" value={row.source} onChange={(event) => setMapRows(mapRows.map((item, rowIndex) => rowIndex === index ? { ...item, source: event.target.value } : item))} /><label className="sr-only" htmlFor={`map-result-${index}`}>Result value {index + 1}</label><input id={`map-result-${index}`} className="field-control" placeholder="Result value" value={row.result} onChange={(event) => setMapRows(mapRows.map((item, rowIndex) => rowIndex === index ? { ...item, result: event.target.value } : item))} /><button className="icon-button" type="button" aria-label={`Remove mapping ${index + 1}`} disabled={mapRows.length === 1} onClick={() => setMapRows(mapRows.filter((_, rowIndex) => rowIndex !== index))}><Trash2 className="h-4 w-4" /></button></div>)}</div><button className="secondary-button mt-2" type="button" onClick={() => setMapRows([...mapRows, { source: "", result: "" }])}>Add mapping row</button></div><label className="field-label">Default (optional)<input className="field-control" value={defaultValue} onChange={(event) => setDefaultValue(event.target.value)} /></label></>}
          {type === "deduplicate" && <><label className="field-label">Key columns<input className="field-control" placeholder="id, term" value={keys} onChange={(event) => setKeys(event.target.value)} /></label><label className="field-label">Order column<select className="field-control" value={orderColumn} onChange={(event) => setOrderColumn(event.target.value)}><option value="">Choose column</option>{columns.map((item) => <option key={item.name}>{item.name}</option>)}</select></label><label className="field-label">Direction<select className="field-control" value={direction} onChange={(event) => setDirection(event.target.value)}><option value="desc">Newest first</option><option value="asc">Oldest first</option></select></label></>}
        </div>
        <div className="mt-4 flex gap-2"><button className="primary-button" type="button" onClick={() => void submit()}>{editing ? "Save transformation" : "Add transformation"}</button>{editing && <button className="secondary-button" type="button" onClick={reset}>Cancel</button>}</div>
        {error && <p className="blocked-panel mt-4" role="alert">{error}</p>}
      </div>
    </section>
  );
}

function QualitySection({ value, save }: { value: OnboardingConfiguration; save: (next: Promise<OnboardingConfiguration>) => Promise<void> }) {
  const [editing, setEditing] = useState<string | null>(null);
  const [id, setId] = useState("DQ001");
  const [type, setType] = useState("not_null");
  const [column, setColumn] = useState(value.post_transformation_columns[0]?.name ?? "");
  const [columns, setColumns] = useState("");
  const [minimum, setMinimum] = useState("");
  const [maximum, setMaximum] = useState("");
  const [pattern, setPattern] = useState("");
  const [error, setError] = useState<string | null>(null);
  const names = value.post_transformation_columns.map((item) => item.name);
  const reset = () => { setEditing(null); setId(`DQ${String(value.contracts.length + 1).padStart(3, "0")}`); setType("not_null"); setColumn(names[0] ?? ""); setColumns(""); setMinimum(""); setMaximum(""); setPattern(""); setError(null); };
  const edit = (item: Record<string, unknown>) => { setEditing(asString(item.id)); setId(asString(item.id)); setType(asString(item.type)); setColumn(asString(item.column)); setColumns(Array.isArray(item.columns) ? item.columns.join(", ") : ""); setMinimum(asString(item.min)); setMaximum(asString(item.max)); setPattern(asString(item.pattern)); setError(null); };
  const submit = async () => {
    try {
      const payload: Record<string, unknown> = { id, type };
      if (["not_null", "range", "regex"].includes(type)) payload.column = column;
      if (type === "unique") payload.columns = list(columns);
      if (type === "range") { if (minimum) payload.min = Number(minimum); if (maximum) payload.max = Number(maximum); }
      if (type === "regex") payload.pattern = pattern;
      await save(editing ? api.updateOnboardingContract(value.onboarding_id, editing, payload) : api.addOnboardingContract(value.onboarding_id, payload)); reset();
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Quality rule could not be saved."); }
  };
  return (
    <section className="panel p-6" id="quality">
      <SectionHeader title="Data Quality" description="Configure row-level contracts and explicitly control what failed values may enter quarantine." />
      <p className="info-panel mb-4">Transformations intentionally change data. Quality contracts judge the transformed result; failed rows are quarantined.</p>
      <OperatorList items={value.contracts} noun="Quality" onEdit={edit} onDelete={(itemId) => save(api.deleteOnboardingContract(value.onboarding_id, itemId))} />
      <div className="mt-6 rounded-xl bg-slate-50 p-4">
        <h3 className="font-bold text-slate-950">{editing ? `Edit ${editing}` : "Add quality rule"}</h3>
        <div className="mt-4 grid gap-4 md:grid-cols-3">
          <label className="field-label">Rule ID<input className="field-control" value={id} disabled={Boolean(editing)} onChange={(event) => setId(event.target.value)} /></label>
          <label className="field-label">Contract<select className="field-control" value={type} onChange={(event) => setType(event.target.value)}>{["not_null", "unique", "range", "regex"].map((item) => <option key={item}>{item}</option>)}</select></label>
          {["not_null", "range", "regex"].includes(type) && <label className="field-label">Column<select className="field-control" value={column} onChange={(event) => setColumn(event.target.value)}>{names.map((item) => <option key={item}>{item}</option>)}</select></label>}
          {type === "unique" && <label className="field-label">Columns<input className="field-control" placeholder="id, term" value={columns} onChange={(event) => setColumns(event.target.value)} /></label>}
          {type === "range" && <><label className="field-label">Minimum<input className="field-control" type="number" value={minimum} onChange={(event) => setMinimum(event.target.value)} /></label><label className="field-label">Maximum<input className="field-control" type="number" value={maximum} onChange={(event) => setMaximum(event.target.value)} /></label></>}
          {type === "regex" && <label className="field-label md:col-span-2">Pattern<input className="field-control" placeholder="^[A-Z0-9]+$" value={pattern} onChange={(event) => setPattern(event.target.value)} /></label>}
        </div>
        <div className="mt-4 flex gap-2"><button className="primary-button" type="button" onClick={() => void submit()}>{editing ? "Save quality rule" : "Add quality rule"}</button>{editing && <button className="secondary-button" type="button" onClick={reset}>Cancel</button>}</div>
        {error && <p className="blocked-panel mt-4" role="alert">{error}</p>}
      </div>
      <div className="mt-6"><h3 className="font-bold text-slate-950">Quarantine privacy</h3><p className="mt-1 text-sm text-slate-600">Full retains the failed scalar; masked stores a generic mask; hashed stores stable SHA-256; none stores no failed value.</p><div className="mt-3 grid gap-3">{value.columns.map((item) => <PrivacyRow key={item.name} item={item} save={(classification, quarantineValue) => save(api.updateOnboardingPrivacy(value.onboarding_id, item.name, { classification, quarantine_value: quarantineValue }))} />)}</div></div>
    </section>
  );
}

function PrivacyRow({ item, save }: { item: OnboardingConfiguration["columns"][number]; save: (classification: string | null, value: "full" | "masked" | "hashed" | "none") => Promise<void> }) {
  const [classification, setClassification] = useState(item.classification ?? "");
  const [policy, setPolicy] = useState(item.quarantine_value);
  return <div className="grid items-end gap-3 rounded-xl border border-slate-200 p-3 md:grid-cols-[1fr_1fr_1fr_auto]"><p className="pb-2 font-bold text-slate-800">{item.name}</p><label className="field-label">Classification<select className="field-control" value={classification} onChange={(event) => setClassification(event.target.value)}><option value="">Unclassified</option><option value="internal">Internal</option><option value="pii">PII</option><option value="confidential">Confidential</option><option value="restricted">Restricted</option></select></label><label className="field-label">Failed value policy<select className="field-control" value={policy} onChange={(event) => setPolicy(event.target.value as typeof policy)}><option value="none">Do not store</option><option value="masked">Masked</option><option value="hashed">Hashed</option><option value="full">Full value</option></select></label><button className="secondary-button" type="button" onClick={() => void save(classification || null, policy)}>Save</button></div>;
}

function LoadSection({ value, save }: { value: OnboardingConfiguration; save: (next: Promise<OnboardingConfiguration>) => Promise<void> }) {
  const initial = asString(value.load.strategy) || "full";
  const [strategy, setStrategy] = useState(initial);
  const [keys, setKeys] = useState(Array.isArray(value.load.keys) ? value.load.keys.join(", ") : "");
  const watermark = value.load.watermark as Record<string, unknown> | undefined;
  const effective = value.load.effective_timestamp as Record<string, unknown> | undefined;
  const history = value.load.history_columns as Record<string, unknown> | undefined;
  const [watermarkColumn, setWatermarkColumn] = useState(asString(watermark?.column));
  const [initialValue, setInitialValue] = useState(asString(watermark?.initial_value));
  const [tracked, setTracked] = useState(Array.isArray(value.load.tracked_columns) ? value.load.tracked_columns.join(", ") : "");
  const [effectiveColumn, setEffectiveColumn] = useState(asString(effective?.column));
  const [validFrom, setValidFrom] = useState(asString(history?.valid_from) || "valid_from");
  const [validTo, setValidTo] = useState(asString(history?.valid_to) || "valid_to");
  const [isCurrent, setIsCurrent] = useState(asString(history?.is_current) || "is_current");
  const [error, setError] = useState<string | null>(null);
  const sourceColumns = value.columns;
  const saveLoad = async () => {
    try {
      let payload: Record<string, unknown> = { strategy };
      if (strategy === "incremental") { const selected = sourceColumns.find((item) => item.name === watermarkColumn); payload.watermark = { column: watermarkColumn, type: selected?.datatype, ...(initialValue ? { initial_value: initialValue } : {}) }; }
      if (strategy === "upsert") payload.keys = list(keys);
      if (strategy === "scd2") payload = { ...payload, keys: list(keys), tracked_columns: list(tracked), effective_timestamp: { column: effectiveColumn }, history_columns: { valid_from: validFrom, valid_to: validTo, is_current: isCurrent } };
      await save(api.updateOnboardingLoad(value.onboarding_id, payload)); setError(null);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Load strategy could not be saved."); }
  };
  return <section className="panel p-6" id="load"><SectionHeader title="Load Strategy" description="Potential keys are suggestions only. Select production semantics explicitly." />
    {value.accepted_key_candidates.length > 0 && <p className="info-panel mb-4">Accepted profiler suggestions: {value.accepted_key_candidates.join(", ")}. They are not selected automatically.</p>}
    <p className="mb-4 text-sm text-slate-600">{loadHelp[strategy]}</p>
    <div className="grid gap-4 md:grid-cols-3"><label className="field-label">Strategy<select className="field-control" value={strategy} onChange={(event) => setStrategy(event.target.value)}>{["full", "incremental", "upsert", "scd2"].map((item) => <option key={item}>{item}</option>)}</select></label>
      {["upsert", "scd2"].includes(strategy) && <label className="field-label md:col-span-2">Business keys<input className="field-control" value={keys} onChange={(event) => setKeys(event.target.value)} /></label>}
      {strategy === "incremental" && <><label className="field-label">Watermark column<select className="field-control" value={watermarkColumn} onChange={(event) => setWatermarkColumn(event.target.value)}><option value="">Choose column</option>{sourceColumns.map((item) => <option key={item.name}>{item.name}</option>)}</select></label><label className="field-label">Initial value (optional)<input className="field-control" value={initialValue} onChange={(event) => setInitialValue(event.target.value)} /></label></>}
      {strategy === "scd2" && <><label className="field-label">Tracked columns<input className="field-control" value={tracked} onChange={(event) => setTracked(event.target.value)} /></label><label className="field-label">Effective date column<select className="field-control" value={effectiveColumn} onChange={(event) => setEffectiveColumn(event.target.value)}><option value="">Choose column</option>{value.post_transformation_columns.map((item) => <option key={item.name}>{item.name}</option>)}</select></label><label className="field-label">Valid from<input className="field-control" value={validFrom} onChange={(event) => setValidFrom(event.target.value)} /></label><label className="field-label">Valid to<input className="field-control" value={validTo} onChange={(event) => setValidTo(event.target.value)} /></label><label className="field-label">Current flag<input className="field-control" value={isCurrent} onChange={(event) => setIsCurrent(event.target.value)} /></label></>}
    </div><button className="primary-button mt-4" type="button" onClick={() => void saveLoad()}>Save load strategy</button>{error && <p className="blocked-panel mt-4" role="alert">{error}</p>}</section>;
}

function DriftSection({ value, save }: { value: OnboardingConfiguration; save: (next: Promise<OnboardingConfiguration>) => Promise<void> }) {
  const [settings, setSettings] = useState(value.schema_drift);
  return <section className="panel p-6" id="drift"><SectionHeader title="Schema Drift" description="Choose allow, warn, or fail for every supported source and canonical change." /><p className="info-panel mb-4"><strong>Raw schema:</strong> did the physical source change? <strong>Canonical schema:</strong> does the normalized structure still match what the pipeline expects?</p><div className="grid gap-4 md:grid-cols-3">{driftSettings.map((setting) => <label className="field-label" key={setting}>{formatLabel(setting)}<select className="field-control" value={settings[setting] ?? "warn"} onChange={(event) => setSettings({ ...settings, [setting]: event.target.value })}><option value="allow">Allow</option><option value="warn">Warn</option><option value="fail">Fail</option></select></label>)}</div><button className="primary-button mt-4" type="button" onClick={() => void save(api.updateOnboardingDrift(value.onboarding_id, settings))}>Save drift policy</button></section>;
}

function JsonNormalizationSection({ value, save }: { value: OnboardingConfiguration; save: (next: Promise<OnboardingConfiguration>) => Promise<void> }) {
  const normalization = value.normalization ?? {};
  const explode = normalization.explode as Record<string, unknown> | undefined;
  const [root, setRoot] = useState(asString(normalization.root_path));
  const [fieldText, setFieldText] = useState(mappingText(normalization.fields));
  const [explodePath, setExplodePath] = useState(asString(explode?.path));
  const [explodeAs, setExplodeAs] = useState(asString(explode?.as));
  const [explodeFields, setExplodeFields] = useState(mappingText(explode?.fields));
  const [columnText, setColumnText] = useState(value.columns.map((column) => `${column.name}:${column.datatype}:${column.nullable ? "nullable" : "required"}`).join("\n"));
  const [error, setError] = useState<string | null>(null);
  const submit = async () => {
    try {
      const columns = Object.fromEntries(columnText.split("\n").map((line) => line.trim()).filter(Boolean).map((line) => { const [name, type, nullable = "nullable"] = line.split(":").map((item) => item.trim()); if (!name || !type) throw new Error("Columns must use name:type:nullable or name:type:required."); return [name, { type, nullable: nullable !== "required", quarantine_value: "none" }]; }));
      const payload: Record<string, unknown> = { fields: mappings(fieldText), columns, ...(root ? { root_path: root } : {}) };
      if (explodePath) payload.explode = { path: explodePath, as: explodeAs, fields: mappings(explodeFields) };
      await save(api.updateOnboardingNormalization(value.onboarding_id, payload)); setError(null);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Normalization could not be saved."); }
  };
  return <section className="panel p-6" id="normalization"><SectionHeader title="JSON Normalization" description="Nested paths and array explosion are explicit. The framework never guesses how to flatten JSON." /><div className="grid gap-4 md:grid-cols-2"><label className="field-label">Root path (optional)<input className="field-control" value={root} onChange={(event) => setRoot(event.target.value)} /></label><label className="field-label">Base field mappings<textarea className="field-control min-h-28" placeholder={"order_id=order.id\ncreated_at=order.created_at"} value={fieldText} onChange={(event) => setFieldText(event.target.value)} /></label><label className="field-label">Array path to explode (optional)<input className="field-control" placeholder="order.items" value={explodePath} onChange={(event) => setExplodePath(event.target.value)} /></label><label className="field-label">Array alias<input className="field-control" placeholder="item" value={explodeAs} onChange={(event) => setExplodeAs(event.target.value)} /></label><label className="field-label">Exploded field mappings<textarea className="field-control min-h-28" placeholder={"sku=item.sku\nquantity=item.quantity"} value={explodeFields} onChange={(event) => setExplodeFields(event.target.value)} /></label><label className="field-label">Canonical columns<textarea className="field-control min-h-28" placeholder={"order_id:string:required\nsku:string:required"} value={columnText} onChange={(event) => setColumnText(event.target.value)} /></label></div><button className="primary-button mt-4" type="button" onClick={() => void submit()}>Save normalization</button>{error && <p className="blocked-panel mt-4" role="alert">{error}</p>}</section>;
}

function ApprovalSection({
  value,
  completion,
  busy,
  act,
}: {
  value: OnboardingConfiguration;
  completion: OnboardingCompletion | null;
  busy: boolean;
  act: (action: "approve" | "activate" | "run" | "retry", approvedBy?: string) => Promise<void>;
}) {
  const [approvedBy, setApprovedBy] = useState("");
  const [acknowledged, setAcknowledged] = useState(false);
  const [confirming, setConfirming] = useState(false);
  if (!value.final_approved) {
    const ready = value.validation.result === "VALID" && Boolean(value.validation.validated_hash) && value.activation_ready;
    return <section className="panel p-6" id="approve">
      <SectionHeader title="Final human approval" description="Approval is bound to the exact validated SHA-256 version shown below. Approval does not start ETL." />
      <div className="grid gap-3 md:grid-cols-2"><p><strong>Validated draft hash</strong><br /><code className="break-all text-sm">{value.validation.validated_hash ?? "Not validated"}</code></p><p><strong>Current draft hash</strong><br /><code className="break-all text-sm">{value.validation.draft_hash}</code></p></div>
      <label className="field-label mt-5">Approved by<input className="field-control" value={approvedBy} maxLength={120} onChange={(event) => setApprovedBy(event.target.value)} placeholder="Your name or operator ID" /></label>
      <label className="mt-4 flex items-start gap-3 text-sm text-slate-700"><input className="mt-1" type="checkbox" checked={acknowledged} onChange={(event) => setAcknowledged(event.target.checked)} /><span>I reviewed this exact configuration and understand that approval promotes it into the runnable config directory and creates a Git commit.</span></label>
      {!confirming ? <button className="primary-button mt-5" type="button" disabled={busy || !ready || !acknowledged || !approvedBy.trim()} onClick={() => setConfirming(true)}>Review final approval</button> : <div className="warning-panel mt-5"><p><strong>Confirm approval</strong></p><p className="mt-1 text-sm">Promote this exact validated version for <strong>{value.dataset}</strong>? The first ETL run will still require a separate action.</p><div className="mt-4 flex gap-3"><button className="primary-button" type="button" disabled={busy} onClick={() => void act("approve", approvedBy)}>Confirm and approve</button><button className="secondary-button" type="button" disabled={busy} onClick={() => setConfirming(false)}>Cancel</button></div></div>}
      {value.validation.result !== "VALID" && <p className="warning-panel mt-4">A current successful validation is required before approval.</p>}
      {!value.activation_ready && <p className="warning-panel mt-4">This draft must be validated again to bind its stable source path and on-demand Airflow activation settings.</p>}
    </section>;
  }
  const result = completion;
  const run = result?.first_run;
  return <section className="panel p-6" id="activation">
    <div className="flex flex-wrap items-start justify-between gap-4"><SectionHeader title="Approved dataset activation" description="The configuration is immutable. Airflow discovery and the first run are separate operational steps." /><StatusBadge status={result?.status ?? value.status} /></div>
    <div className="grid gap-3 rounded-xl bg-slate-50 p-4 md:grid-cols-2 lg:grid-cols-3"><p><strong>Approved by</strong><br />{value.approval.approved_by}</p><p><strong>Approved at</strong><br />{value.approval.approved_at}</p><p><strong>Git commit</strong><br /><code>{value.approval.git_commit_sha}</code></p><p><strong>Git push</strong><br />{formatLabel(value.approval.git_push_status ?? "not requested")}</p><p><strong>DAG</strong><br /><code>{value.approval.dag_id}</code></p><p><strong>Validated hash</strong><br /><code className="break-all text-xs">{value.approval.validation_hash}</code></p><p><strong>Approved hash</strong><br /><code className="break-all text-xs">{value.approval.approved_config_hash}</code></p></div>
    {result?.safe_error && <p className="warning-panel mt-4">{result.safe_error}</p>}
    {result?.status === "ACTIVATION_FAILED" && <button className="primary-button mt-4" type="button" disabled={busy} onClick={() => void act("activate")}>Retry DAG discovery</button>}
    {result?.status === "READY_FOR_FIRST_RUN" && <button className="primary-button mt-4" type="button" disabled={busy} onClick={() => void act("run")}>Run first ETL load</button>}
    {result?.status === "FIRST_RUN_FAILED" && <button className="primary-button mt-4" type="button" disabled={busy} onClick={() => void act("retry")}>Retry first ETL load</button>}
    {run?.correlation_id && <div className="mt-5 grid gap-3 md:grid-cols-2 lg:grid-cols-4"><p><strong>Correlation ID</strong><br /><code className="break-all text-sm">{run.correlation_id}</code></p><p><strong>Airflow run</strong><br /><code className="break-all text-sm">{run.airflow_run_id}</code></p><p><strong>Airflow state</strong><br />{formatLabel(run.airflow_state ?? "waiting")}</p><p><strong>ETL run</strong><br /><code>{run.etl_run_id ?? "Waiting"}</code></p></div>}
    {run?.etl_run && <div className="success-panel mt-5"><p><strong>First run {formatLabel(run.etl_run.status)}</strong></p><p className="mt-1 text-sm">Extracted {run.etl_run.rows_extracted ?? 0} · passed {run.etl_run.rows_contract_passed ?? 0} · quarantined {run.etl_run.rows_quarantined ?? 0} · loaded {run.etl_run.rows_loaded ?? 0}</p><div className="mt-3 flex gap-4"><Link className="font-bold text-cyan-800" to={`/runs/${encodeURIComponent(run.etl_run.run_id)}`}>Run detail</Link><Link className="font-bold text-cyan-800" to={`/datasets/${encodeURIComponent(value.dataset)}`}>Dataset detail</Link></div></div>}
  </section>;
}

export function ConfigurationBuilderPage() {
  const { onboardingId = "" } = useParams();
  const loader = useCallback(() => api.onboardingConfiguration(onboardingId), [onboardingId]);
  const state = useApi(loader);
  const [configuration, setConfiguration] = useState<OnboardingConfiguration | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mode, setMode] = useState<"builder" | "yaml">("builder");
  const [yaml, setYaml] = useState<string | null>(null);
  const [completion, setCompletion] = useState<OnboardingCompletion | null>(null);
  const current = configuration ?? state.data;
  const save = useCallback(async (next: Promise<OnboardingConfiguration>) => { setBusy(true); setError(null); try { setConfiguration(await next); } catch (reason) { setError(reason instanceof Error ? reason.message : "Configuration could not be saved."); throw reason; } finally { setBusy(false); } }, []);
  useEffect(() => { if (mode === "yaml") void api.onboardingYAML(onboardingId).then((result) => setYaml(result.yaml)).catch((reason: unknown) => setError(reason instanceof Error ? reason.message : "YAML could not be loaded.")); }, [mode, onboardingId, configuration]);
  useEffect(() => {
    if (!current?.final_approved) return;
    let active = true;
    const refresh = () => void api.onboardingCompletion(onboardingId).then((result) => { if (active) setCompletion(result); }).catch(() => undefined);
    refresh();
    const timer = window.setInterval(refresh, 2500);
    return () => { active = false; window.clearInterval(timer); };
  }, [current?.final_approved, onboardingId]);
  const progress = useMemo(() => current ? [
    ["Source", true], ["Profile", true], ["Schema", current.review_complete],
    ["Transformations", current.review_complete], ["Data Quality", current.review_complete],
    ["Load Strategy", Boolean(current.load.strategy)], ["Schema Drift", Object.keys(current.schema_drift).length > 0],
    ["Review & Validate", current.validation.result === "VALID"], ["Approval", current.final_approved],
    ["First Run", completion?.status === "SUCCEEDED"],
  ] as [string, boolean][] : [], [current, completion?.status]);

  if (state.loading) return <LoadingState label="Loading configuration builder" />;
  if (state.error) return <ErrorState message={state.error} onRetry={state.reload} />;
  if (!current) return null;
  if (!current.review_complete && current.source_type !== "json") return <><Link to={`/onboarding/${encodeURIComponent(onboardingId)}/review`} className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700"><ArrowLeft className="h-4 w-4" />Review Center</Link><PageHeader eyebrow="Configuration Builder" title={current.dataset} description="Schema and profiler decisions must be complete first." /><p className="blocked-panel">Complete every required schema and key-candidate decision before configuring this dataset.</p></>;

  const validate = async () => { try { await save(api.validateOnboardingConfiguration(onboardingId)); } catch { /* shared error panel already contains the safe response */ } };
  const operationalAction = async (action: "approve" | "activate" | "run" | "retry", approvedBy?: string) => {
    setBusy(true); setError(null);
    try {
      const result = action === "approve"
        ? await api.approveOnboarding(onboardingId, { expected_hash: current.validation.validated_hash ?? "", approved_by: approvedBy ?? "", acknowledged: true })
        : action === "activate" ? await api.activateOnboarding(onboardingId)
          : await api.runOnboardingFirst(onboardingId, action === "retry");
      setCompletion(result);
      setConfiguration(await api.onboardingConfiguration(onboardingId));
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Onboarding operation could not be completed."); }
    finally { setBusy(false); }
  };
  return <>
    <Link to={`/onboarding/${encodeURIComponent(onboardingId)}/review`} className="mb-5 inline-flex items-center gap-2 text-sm font-bold text-cyan-700 hover:text-cyan-900"><ArrowLeft className="h-4 w-4" />Review Center</Link>
    <PageHeader eyebrow="Configuration Builder" title={current.dataset} description="Build the dataset contract using the same validators and operators as the ETL runtime." action={<StatusBadge status={current.status} />} />
    <div className="panel mb-6 overflow-x-auto p-4"><ol className="flex min-w-max gap-2" aria-label="Onboarding progress">{progress.map(([label, complete]) => <li key={label} className={complete ? "example-chip text-emerald-700" : "example-chip"}>{complete && <CheckCircle2 className="mr-1 inline h-4 w-4" />}{label}</li>)}</ol></div>
    <div className="panel mb-6 flex gap-1 p-2" role="tablist" aria-label="Configuration modes"><button className={mode === "builder" ? "review-tab review-tab-active" : "review-tab"} role="tab" aria-selected={mode === "builder"} onClick={() => setMode("builder")}>Configuration Builder</button><button className={mode === "yaml" ? "review-tab review-tab-active" : "review-tab"} role="tab" aria-selected={mode === "yaml"} onClick={() => setMode("yaml")}><FileCode2 className="h-4 w-4" />YAML View</button></div>
    {busy && <p className="info-panel mb-4">Saving authoritative draft…</p>}{error && <p className="blocked-panel mb-4" role="alert">{error}</p>}
    {mode === "yaml" ? <section className="panel p-6"><SectionHeader title={current.final_approved ? "Approved YAML" : "Current draft YAML"} description="Read-only persisted YAML. Refreshing this page reloads the authoritative server-side configuration." />{yaml ? <pre className="yaml-preview">{yaml}</pre> : <LoadingState label="Loading configuration YAML" />}</section> : <div className="space-y-6">
      {!current.final_approved && current.source_type === "json" && <JsonNormalizationSection value={current} save={save} />}
      {!current.review_complete && <p className="blocked-panel">Save explicit JSON normalization to resolve the nested schema before configuring later stages.</p>}
      {!current.final_approved && current.review_complete && <TransformationSection value={current} save={save} />}
      {!current.final_approved && current.review_complete && <QualitySection value={current} save={save} />}
      {!current.final_approved && current.review_complete && <LoadSection value={current} save={save} />}
      {!current.final_approved && current.review_complete && <DriftSection value={current} save={save} />}
      {!current.final_approved && current.review_complete &&
      <section className="panel p-6" id="validate"><SectionHeader title="Review & Validate" description="Validate the exact persisted draft and uploaded source before it can be handed to a later approval milestone." />
        <div className="mb-5 grid gap-3 rounded-xl bg-slate-50 p-4 sm:grid-cols-2 lg:grid-cols-4"><p><strong>Source</strong><br />{formatLabel(current.source_type)}</p><p><strong>Columns</strong><br />{current.columns.length}</p><p><strong>Transformations</strong><br />{current.transformations.length}</p><p><strong>Quality rules</strong><br />{current.contracts.length}</p><p><strong>Load strategy</strong><br />{formatLabel(asString(current.load.strategy))}</p><p><strong>Watermark</strong><br />{asString((current.load.watermark as Record<string, unknown> | undefined)?.column) || "Not applicable"}</p><p><strong>Orchestration</strong><br />{current.orchestration.enabled ? "Enabled · on demand" : "Disabled"}</p><p><strong>Approval</strong><br />Not approved</p></div>
        <div className="grid gap-3 md:grid-cols-2"><p><strong>Draft hash</strong><br /><code className="break-all text-sm">{current.validation.draft_hash}</code></p><p><strong>Result</strong><br /><StatusBadge status={current.validation.result} /></p></div>
        {current.validation.result === "VALID" && <p className="success-panel mt-4"><ShieldCheck className="mr-2 inline h-5 w-5" />Configuration is ready for approval. It remains explicitly unapproved.</p>}
        {current.validation.result === "INVALID" && <ul className="blocked-panel mt-4">{current.validation.errors.map((item, index) => <li key={`${item.section}-${index}`}><AlertTriangle className="mr-2 inline h-4 w-4" />{item.section}{item.field ? ` · ${item.field}` : ""}: {item.message}</li>)}</ul>}
        <button className="primary-button mt-4" type="button" disabled={busy || !current.normalization_complete} onClick={() => void validate()}>Validate configuration</button>
        {!current.normalization_complete && <p className="warning-panel mt-4">Explicit JSON normalization is required before validation.</p>}
      </section>}
      {current.review_complete && <ApprovalSection value={current} completion={completion} busy={busy} act={operationalAction} />}
    </div>}
  </>;
}
