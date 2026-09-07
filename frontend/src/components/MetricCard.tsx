import type { LucideIcon } from "lucide-react";

interface MetricCardProps {
  label: string;
  value: string;
  detail?: string;
  icon: LucideIcon;
  tone?: "default" | "good" | "warning" | "danger";
}

const toneStyles = {
  default: "bg-slate-100 text-slate-700",
  good: "bg-emerald-50 text-emerald-700",
  warning: "bg-amber-50 text-amber-700",
  danger: "bg-rose-50 text-rose-700",
};

export function MetricCard({ label, value, detail, icon: Icon, tone = "default" }: MetricCardProps) {
  return (
    <article className="panel flex min-h-28 flex-col justify-between p-4">
      <div className="flex items-start justify-between gap-3">
        <p className="text-sm font-semibold text-slate-500">{label}</p>
        <span className={`rounded-lg p-1.5 ${toneStyles[tone]}`}>
          <Icon className="h-4 w-4" aria-hidden="true" />
        </span>
      </div>
      <div>
        <p className="mt-3 text-2xl font-bold tracking-tight text-slate-950">{value}</p>
        {detail && <p className="mt-1 text-xs text-slate-500">{detail}</p>}
      </div>
    </article>
  );
}
