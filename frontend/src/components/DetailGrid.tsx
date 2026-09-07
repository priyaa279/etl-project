import type { ReactNode } from "react";

export interface DetailItem {
  label: string;
  value: ReactNode;
  detail?: string;
}

export function DetailGrid({ items }: { items: DetailItem[] }) {
  return (
    <dl className="grid gap-px overflow-hidden rounded-xl border border-slate-200 bg-slate-200 sm:grid-cols-2 xl:grid-cols-4">
      {items.map((item) => (
        <div key={item.label} className="min-h-28 bg-white p-4">
          <dt className="text-xs font-bold uppercase tracking-wide text-slate-500">{item.label}</dt>
          <dd className="mt-3 text-base font-bold text-slate-900">{item.value}</dd>
          {item.detail && <p className="mt-1 text-xs text-slate-500">{item.detail}</p>}
        </div>
      ))}
    </dl>
  );
}
