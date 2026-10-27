export const fmtNum = (v: number | null | undefined, d = 2): string =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : v.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
export const fmtInt = (v: number | null | undefined): string => (v === null || v === undefined ? "—" : v.toLocaleString());
export const fmtPct = (v: number | null | undefined, d = 2): string =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(d)}%`;
export const fmtCompact = (v: number | null | undefined): string =>
  v === null || v === undefined ? "—" : Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(v);

/** Absolute timestamp in the viewer's locale, with timezone abbreviation. Null/invalid -> em dash (never "now"). */
export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZoneName: "short" });
}
export const fmtDay = (iso: string): string => new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric" });
export const fmtHour = (iso: string): string => new Date(iso).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });

/** Seconds since iso (>=0), or null when missing/invalid. */
export function ageSeconds(iso: string | null | undefined, now = Date.now()): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  return Number.isNaN(t) ? null : Math.max(0, (now - t) / 1000);
}
export function fmtAge(sec: number | null): string {
  if (sec === null) return "unknown";
  if (sec < 90) return `${Math.round(sec)}s ago`;
  if (sec < 5400) return `${Math.round(sec / 60)}m ago`;
  if (sec < 172800) return `${Math.round(sec / 3600)}h ago`;
  return `${Math.round(sec / 86400)}d ago`;
}

export type Freshness = "FRESH" | "STALE" | "UNKNOWN";
/** UNKNOWN when timestamp absent — never reported as fresh or zero-aged. */
export function freshness(iso: string | null | undefined, staleAfterSec: number, now = Date.now()): Freshness {
  const a = ageSeconds(iso, now);
  return a === null ? "UNKNOWN" : a > staleAfterSec ? "STALE" : "FRESH";
}
