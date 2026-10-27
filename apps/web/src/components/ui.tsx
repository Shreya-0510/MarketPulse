import type { ReactNode } from "react";
import { AlertTriangle, CheckCircle2, CircleHelp, Info, RefreshCw, XCircle } from "lucide-react";
import { fmtInt, fmtPct } from "../lib/format";

export function Card({ title, subtitle, right, children, className = "" }: { title?: string; subtitle?: ReactNode; right?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`rounded-lg border border-slate-200 bg-white ${className}`}>
      {(title || right) && (
        <header className="flex items-start justify-between gap-3 border-b border-slate-100 px-4 py-2.5">
          <div><h2 className="text-sm font-semibold text-ink-900">{title}</h2>{subtitle && <p className="mt-0.5 text-xs text-ink-500">{subtitle}</p>}</div>
          {right}
        </header>
      )}
      <div className="p-4">{children}</div>
    </section>
  );
}

export function Skeleton({ h = 120 }: { h?: number }) { return <div className="animate-pulse rounded bg-slate-100" style={{ height: h }} role="status" aria-label="Loading" />; }

export function Empty({ title, hint }: { title: string; hint?: string }) {
  return <div className="flex flex-col items-center justify-center gap-1 rounded border border-dashed border-slate-200 px-4 py-8 text-center">
    <Info className="h-5 w-5 text-ink-300" /><p className="text-sm font-medium text-ink-700">{title}</p>{hint && <p className="max-w-md text-xs text-ink-500">{hint}</p>}</div>;
}

export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return <div role="alert" className="flex items-center justify-between gap-3 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
    <span className="flex items-center gap-2"><XCircle className="h-4 w-4 shrink-0" />{message}</span>
    {onRetry && <button onClick={onRetry} className="flex items-center gap-1 rounded border border-red-300 bg-white px-2 py-1 text-xs font-medium hover:bg-red-100"><RefreshCw className="h-3 w-3" />Retry</button>}</div>;
}

export function Banner({ tone, children }: { tone: "warn" | "error" | "info"; children: ReactNode }) {
  const c = { warn: "border-amber-300 bg-amber-50 text-amber-900", error: "border-red-300 bg-red-50 text-red-900", info: "border-blue-200 bg-blue-50 text-blue-900" }[tone];
  const I = tone === "info" ? Info : AlertTriangle;
  return <div role="status" className={`flex items-start gap-2 rounded border px-3 py-2 text-sm ${c}`}><I className="mt-0.5 h-4 w-4 shrink-0" /><div>{children}</div></div>;
}

/** Standard async wrapper: skeleton while first load, error with retry (keeping last data visible), empty state. */
export function Async<T>({ data, error, loading, reload, isEmpty, empty, h, children }: {
  data: T | null; error: string | null; loading: boolean; reload: () => void; isEmpty?: (d: T) => boolean;
  empty?: { title: string; hint?: string }; h?: number; children: (d: T) => ReactNode;
}) {
  if (data === null && loading) return <Skeleton h={h} />;
  if (data === null && error) return <ErrorState message={error} onRetry={reload} />;
  if (data === null) return null;
  return <>
    {error && <div className="mb-2"><ErrorState message={`Refresh failed — showing last loaded data. ${error}`} onRetry={reload} /></div>}
    {isEmpty?.(data) ? <Empty title={empty?.title ?? "No data"} hint={empty?.hint} /> : children(data)}
  </>;
}

const PILL: Record<string, string> = {
  OK: "bg-emerald-50 text-emerald-800 border-emerald-200", FRESH: "bg-emerald-50 text-emerald-800 border-emerald-200", open: "bg-emerald-50 text-emerald-800 border-emerald-200",
  STALE: "bg-amber-50 text-amber-900 border-amber-300", DEGRADED: "bg-amber-50 text-amber-900 border-amber-300", reconnecting: "bg-amber-50 text-amber-900 border-amber-300", connecting: "bg-amber-50 text-amber-900 border-amber-300",
  FAILED: "bg-red-50 text-red-800 border-red-200", ERROR: "bg-red-50 text-red-800 border-red-200", closed: "bg-red-50 text-red-800 border-red-200",
};
export function Pill({ label, tone }: { label: string; tone?: string }) {
  const cls = PILL[tone ?? label] ?? "bg-slate-100 text-ink-700 border-slate-200";
  const Icon = cls.includes("emerald") ? CheckCircle2 : cls.includes("red") ? XCircle : cls.includes("amber") ? AlertTriangle : CircleHelp;
  return <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium ${cls}`}><Icon className="h-3 w-3" />{label}</span>;
}

export function ModePill({ mode }: { mode: string }) {
  const cls = mode === "live" ? "bg-blue-50 text-blue-800 border-blue-200" : mode === "test" ? "bg-fuchsia-50 text-fuchsia-800 border-fuchsia-200" : "bg-slate-100 text-ink-700 border-slate-200";
  return <span className={`rounded border px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide ${cls}`} title="data_mode: how the record was obtained">{mode}</span>;
}

export const sentimentColor = { positive: "#059669", neutral: "#94a3b8", negative: "#dc2626" } as const;

export function SentimentBar({ positive, neutral, negative, scored, total }: { positive: number; neutral: number; negative: number; scored: number; total?: number }) {
  if (!scored) return <p className="text-xs text-ink-500">No scored items{total ? ` (${fmtInt(total)} items awaiting sentiment)` : ""}.</p>;
  const pc = (n: number) => (n / scored) * 100;
  return <div>
    <div className="flex h-3 overflow-hidden rounded bg-slate-100" role="img" aria-label={`${positive} positive, ${neutral} neutral, ${negative} negative`}>
      <div style={{ width: `${pc(positive)}%`, background: sentimentColor.positive }} /><div style={{ width: `${pc(neutral)}%`, background: sentimentColor.neutral }} /><div style={{ width: `${pc(negative)}%`, background: sentimentColor.negative }} /></div>
    <p className="mt-1 text-xs text-ink-500">{fmtInt(positive)} pos · {fmtInt(neutral)} neu · {fmtInt(negative)} neg — n={fmtInt(scored)} scored{total !== undefined && total > scored ? ` of ${fmtInt(total)}` : ""}</p></div>;
}

export function SentimentTag({ label }: { label: string | null }) {
  if (!label) return <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-ink-500" title="Sentiment not yet computed">unscored</span>;
  const c = label === "positive" ? "bg-emerald-50 text-emerald-800" : label === "negative" ? "bg-red-50 text-red-800" : "bg-slate-100 text-ink-700";
  return <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${c}`}>{label}</span>;
}

export function Change({ pct }: { pct: number | null }) {
  if (pct === null) return <span className="text-ink-500">—</span>;
  return <span className={pct > 0 ? "text-emerald-700" : pct < 0 ? "text-red-700" : "text-ink-700"}>{fmtPct(pct)}</span>;
}

export function Hint({ text }: { text: string }) { return <span title={text} className="inline-flex cursor-help align-middle text-ink-300"><CircleHelp className="h-3.5 w-3.5" aria-label={text} /></span>; }

export function ExtLink({ href, children }: { href: string | null; children: ReactNode }) {
  if (!href) return <>{children}</>;
  return <a href={href} target="_blank" rel="noopener noreferrer" className="text-brand-600 hover:underline">{children}</a>;
}

export const chartTooltip = { contentStyle: { fontSize: 12, borderRadius: 6, border: "1px solid #e2e8f0" } };
