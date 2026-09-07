import {
  CheckCircle2,
  CircleHelp,
  LoaderCircle,
  TriangleAlert,
  XCircle,
} from "lucide-react";

interface StatusBadgeProps {
  status: string | null | undefined;
}

const styleByStatus: Record<string, string> = {
  HEALTHY: "border-emerald-200 bg-emerald-50 text-emerald-800",
  SUCCEEDED: "border-emerald-200 bg-emerald-50 text-emerald-800",
  WARNING: "border-amber-200 bg-amber-50 text-amber-800",
  WARN: "border-amber-200 bg-amber-50 text-amber-800",
  WARNED: "border-amber-200 bg-amber-50 text-amber-800",
  FAILED: "border-rose-200 bg-rose-50 text-rose-800",
  RUNNING: "border-sky-200 bg-sky-50 text-sky-800",
  UNKNOWN: "border-slate-200 bg-slate-50 text-slate-700",
  NONE: "border-slate-200 bg-slate-50 text-slate-700",
  PASSED: "border-emerald-200 bg-emerald-50 text-emerald-800",
  READY: "border-emerald-200 bg-emerald-50 text-emerald-800",
  BLOCKED: "border-rose-200 bg-rose-50 text-rose-800",
  QUEUED: "border-sky-200 bg-sky-50 text-sky-800",
  TRIGGERING: "border-sky-200 bg-sky-50 text-sky-800",
  VALIDATING: "border-sky-200 bg-sky-50 text-sky-800",
  UPLOADED: "border-slate-200 bg-slate-50 text-slate-700",
};

function StatusIcon({ status }: { status: string }) {
  const className = "h-3.5 w-3.5";
  if (
    status === "HEALTHY" ||
    status === "SUCCEEDED" ||
    status === "PASSED" ||
    status === "READY"
  ) {
    return <CheckCircle2 className={className} aria-hidden="true" />;
  }
  if (status === "WARNING" || status === "WARN" || status === "WARNED") {
    return <TriangleAlert className={className} aria-hidden="true" />;
  }
  if (status === "FAILED" || status === "BLOCKED") {
    return <XCircle className={className} aria-hidden="true" />;
  }
  if (status === "RUNNING" || status === "QUEUED" || status === "TRIGGERING") {
    return <LoaderCircle className={className} aria-hidden="true" />;
  }
  return <CircleHelp className={className} aria-hidden="true" />;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const normalized = status?.toUpperCase() || "UNKNOWN";
  const style = styleByStatus[normalized] ?? styleByStatus.UNKNOWN;
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-bold tracking-wide ${style}`}
      aria-label={`Status: ${normalized}`}
    >
      <StatusIcon status={normalized} />
      {normalized}
    </span>
  );
}
