import {
  Activity,
  Database,
  Droplets,
  Gauge,
  GitCompareArrows,
  ListChecks,
} from "lucide-react";
import { NavLink, Outlet } from "react-router-dom";

const navigation = [
  { to: "/", label: "Overview", icon: Gauge, end: true },
  { to: "/datasets", label: "Datasets", icon: Database },
  { to: "/runs", label: "Runs", icon: Activity },
  { to: "/quality", label: "Data Quality", icon: ListChecks },
  { to: "/schema-drift", label: "Schema Drift", icon: GitCompareArrows },
  { to: "/watermarks", label: "Watermarks", icon: Droplets },
];

export function AppShell() {
  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <a href="#main-content" className="skip-link">
        Skip to content
      </a>
      <aside className="sidebar">
        <div className="flex items-center gap-3 px-4 py-3 lg:px-5">
          <span className="relative grid h-10 w-10 place-items-center rounded-xl bg-cyan-400 text-slate-950 shadow-[0_0_28px_rgba(34,211,238,0.2)]">
            <Database className="h-5 w-5" aria-hidden="true" />
            <span className="absolute -right-1 -top-1 h-2.5 w-2.5 rounded-full border-2 border-slate-950 bg-emerald-400" />
          </span>
          <div className="min-w-0">
            <p className="truncate font-bold tracking-tight text-white">ETL Control Center</p>
            <p className="text-xs font-medium text-slate-400">Read-only operations</p>
          </div>
        </div>

        <nav aria-label="Primary navigation" className="nav-list">
          {navigation.map(({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) => `nav-link ${isActive ? "nav-link-active" : ""}`}
            >
              <Icon className="h-4 w-4 shrink-0" aria-hidden="true" />
              <span>{label}</span>
            </NavLink>
          ))}
        </nav>

        <div className="mt-auto hidden px-5 pb-6 lg:block">
          <div className="border-t border-slate-800 pt-5">
            <div className="flex items-center gap-2 text-xs font-semibold text-slate-400">
              <span className="h-2 w-2 rounded-full bg-emerald-400" />
              LOCAL ENVIRONMENT
            </div>
            <p className="mt-2 text-xs leading-5 text-slate-500">Operational metadata only</p>
          </div>
        </div>
      </aside>
      <main id="main-content" className="main-content" tabIndex={-1}>
        <Outlet />
      </main>
    </div>
  );
}
