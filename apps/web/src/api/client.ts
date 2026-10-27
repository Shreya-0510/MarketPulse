import type * as T from "../types/api";

const BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "").replace(/\/$/, "");

export class ApiError extends Error {
  constructor(public status: number, message: string) { super(message); }
}

export function buildQuery(params: Record<string, string | number | undefined | null>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== "") q.set(k, String(v));
  const s = q.toString();
  return s ? `?${s}` : "";
}

async function get<R>(path: string, signal?: AbortSignal): Promise<R> {
  let res: Response;
  try { res = await fetch(`${BASE}${path}`, { signal, headers: { Accept: "application/json" } }); }
  catch (e) { if ((e as Error).name === "AbortError") throw e; throw new ApiError(0, "API unreachable (network error or backend not running)"); }
  if (!res.ok) {
    let detail = res.statusText;
    try { const b = await res.json(); if (b?.detail) detail = typeof b.detail === "string" ? b.detail : JSON.stringify(b.detail); } catch { /* ignore */ }
    throw new ApiError(res.status, `HTTP ${res.status}: ${detail}`);
  }
  return res.json() as Promise<R>;
}

export function wsUrl(params: Record<string, string | undefined> = {}): string {
  const explicit = import.meta.env.VITE_WS_BASE_URL as string | undefined;
  let base: string;
  if (explicit) base = explicit.replace(/\/$/, "");
  else if (BASE) base = BASE.replace(/^http/, "ws");
  else base = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}`;
  return `${base}/api/v1/ws${buildQuery(params)}`;
}

const V = "/api/v1";
export const api = {
  health: (s?: AbortSignal) => get<Record<string, string>>("/health", s),
  watchlist: (s?: AbortSignal) => get<T.WatchlistResponse>(`${V}/market/watchlist`, s),
  history: (symbol: string, p: { start?: string; end?: string; limit?: number }, s?: AbortSignal) =>
    get<T.HistoryResponse>(`${V}/market/${encodeURIComponent(symbol)}/history${buildQuery({ interval: "1d", limit: 500, ...p })}`, s),
  news: (p: { entity?: string; source?: string; publisher?: string; sentiment?: string; start?: string; end?: string; limit?: number; offset?: number }, s?: AbortSignal) =>
    get<T.NewsResponse>(`${V}/news${buildQuery(p)}`, s),
  social: (p: { entity?: string; source?: string; sentiment?: string; start?: string; end?: string; limit?: number; offset?: number }, s?: AbortSignal) =>
    get<T.SocialResponse>(`${V}/social${buildQuery(p)}`, s),
  entities: (s?: AbortSignal) => get<T.EntitiesResponse>(`${V}/entities`, s),
  entityMetrics: (id: string, limit: number, s?: AbortSignal) => get<T.EntityMetricsResponse>(`${V}/entities/${encodeURIComponent(id)}/metrics${buildQuery({ limit })}`, s),
  attention: (p: { entity?: string; source_type?: string; start?: string; end?: string; limit?: number }, s?: AbortSignal) =>
    get<T.AttentionResponse>(`${V}/attention${buildQuery(p)}`, s),
  anomalies: (p: { entity?: string; source_type?: string; start?: string; end?: string; limit?: number }, s?: AbortSignal) =>
    get<T.AnomalyResponse>(`${V}/anomalies${buildQuery(p)}`, s),
  crossSignals: (p: { entity?: string; window_minutes?: number; lookback_windows?: number }, s?: AbortSignal) =>
    get<T.CrossSignalResponse>(`${V}/cross-signals${buildQuery(p)}`, s),
  pipelineStatus: (s?: AbortSignal) => get<T.PipelineStatus>(`${V}/pipeline/status`, s),
  sourcesStatus: (s?: AbortSignal) => get<T.SourceStatus[]>(`${V}/sources/status`, s),
};
