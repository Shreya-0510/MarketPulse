import { api } from "../api/client";
import type { EntityMetrics, EntitySummary } from "../types/api";
import { useApi } from "./useApi";

export interface AllMetrics { windows: EntityMetrics[][]; failed: string[]; withData: number }
/** One /entities/{id}/metrics call per entity (the API has no cross-entity endpoint). Failures are reported, not hidden. */
export function useAllMetrics(entities: EntitySummary[] | undefined, limit: number, pollMs = 60000) {
  const key = (entities ?? []).map((e) => e.entity_id).join(",");
  return useApi<AllMetrics>(async (signal) => {
    const res = await Promise.allSettled((entities ?? []).map((e) => api.entityMetrics(e.entity_id, limit, signal)));
    if (signal.aborted) throw new DOMException("aborted", "AbortError");
    const windows: EntityMetrics[][] = []; const failed: string[] = [];
    res.forEach((r, i) => { if (r.status === "fulfilled") windows.push(r.value.windows); else failed.push(entities![i].entity_id); });
    return { windows, failed, withData: windows.filter((w) => w.length).length };
  }, [key, limit], key ? pollMs : 0);
}
