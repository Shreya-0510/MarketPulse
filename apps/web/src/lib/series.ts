import type { Bar, EntityMetrics, Sentiment } from "../types/api";

export interface ActivityPoint { t: string; social: number; news: number }
/** Sum social/news match counts per window_start across entities (a post matching 2 entities counts once per entity). */
export function sumActivity(all: EntityMetrics[][], cutoffMs: number): ActivityPoint[] {
  const m = new Map<string, ActivityPoint>();
  for (const ws of all) for (const w of ws) {
    if (new Date(w.window_start).getTime() < cutoffMs) continue;
    const p = m.get(w.window_start) ?? { t: w.window_start, social: 0, news: 0 };
    p.social += w.social.count; p.news += w.news.count; m.set(w.window_start, p);
  }
  return [...m.values()].sort((a, b) => a.t.localeCompare(b.t));
}

export interface SentSum { positive: number; neutral: number; negative: number; scored: number; total: number }
export function sumSentiment(all: EntityMetrics[][], kind: "social" | "news", cutoffMs: number): SentSum {
  const s: SentSum = { positive: 0, neutral: 0, negative: 0, scored: 0, total: 0 };
  for (const ws of all) for (const w of ws) {
    if (new Date(w.window_start).getTime() < cutoffMs) continue;
    const x = w[kind]; s.positive += x.positive; s.neutral += x.neutral; s.negative += x.negative; s.scored += x.scored; s.total += x.count;
  }
  return s;
}

export type Row = { date: string } & Record<string, number | string | null>;
/** Rebase each instrument to 100 at its first bar in range (display normalisation only; allows cross-currency comparison). Missing day -> null (gap). */
export function indexSeries(histories: Record<string, Bar[]>): Row[] {
  const dates = new Set<string>();
  const base: Record<string, number> = {};
  const byDay: Record<string, Record<string, number>> = {};
  for (const [sym, bars] of Object.entries(histories)) {
    const sorted = [...bars].sort((a, b) => a.event_time.localeCompare(b.event_time));
    byDay[sym] = {};
    for (const b of sorted) { const d = b.event_time.slice(0, 10); dates.add(d); byDay[sym][d] = b.close; if (!(sym in base) && b.close > 0) base[sym] = b.close; }
  }
  return [...dates].sort().map((d) => {
    const r: Row = { date: d };
    for (const sym of Object.keys(histories)) { const v = byDay[sym][d]; r[sym] = v !== undefined && base[sym] ? (v / base[sym]) * 100 : null; }
    return r;
  });
}

/** Simple return over the last `days` calendar days using the latest bar and the last bar at/before (latest - days). null if no such bar. */
export function periodReturn(bars: Bar[], days: number): { pct: number | null; from: string | null; n: number } {
  if (bars.length < 2) return { pct: null, from: null, n: bars.length };
  const s = [...bars].sort((a, b) => a.event_time.localeCompare(b.event_time));
  const last = s[s.length - 1];
  const target = new Date(last.event_time).getTime() - days * 86400000;
  let ref: Bar | null = null;
  for (const b of s) if (new Date(b.event_time).getTime() <= target) ref = b;
  if (!ref || !(ref.close > 0)) return { pct: null, from: null, n: s.length };
  return { pct: ((last.close - ref.close) / ref.close) * 100, from: ref.event_time, n: s.length };
}

export const sentimentOf = (s: Sentiment | null) => s ?? "unscored";
export const isProd = <T extends { data_mode: string }>(x: T) => x.data_mode !== "test";
