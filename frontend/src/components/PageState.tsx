import { AlertCircle, Inbox, RefreshCw } from "lucide-react";

export function LoadingState({ label = "Loading operational data" }: { label?: string }) {
  return (
    <div className="state-panel" role="status" aria-live="polite">
      <span className="h-5 w-5 animate-spin rounded-full border-2 border-slate-300 border-t-cyan-600" />
      <span>{label}…</span>
    </div>
  );
}

export function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="state-panel border-rose-200 bg-rose-50 text-rose-900" role="alert">
      <AlertCircle className="h-5 w-5" aria-hidden="true" />
      <span className="flex-1">{message}</span>
      <button type="button" className="secondary-button" onClick={onRetry}>
        <RefreshCw className="h-4 w-4" aria-hidden="true" />
        Retry
      </button>
    </div>
  );
}

export function EmptyState({ title, message }: { title: string; message: string }) {
  return (
    <div className="state-panel flex-col py-12 text-center">
      <Inbox className="h-7 w-7 text-slate-400" aria-hidden="true" />
      <div>
        <p className="font-semibold text-slate-800">{title}</p>
        <p className="mt-1 text-sm text-slate-500">{message}</p>
      </div>
    </div>
  );
}
