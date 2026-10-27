import { useCallback, useEffect, useRef, useState } from "react";

export interface ApiState<T> { data: T | null; error: string | null; loading: boolean; updatedAt: number | null; reload: () => void }

/** Fetch with AbortController cleanup, optional polling (paused while the tab is hidden), and last-good data retention on error. */
export function useApi<T>(fetcher: (signal: AbortSignal) => Promise<T>, deps: unknown[], pollMs = 0): ApiState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const fetchRef = useRef(fetcher);
  fetchRef.current = fetcher;

  useEffect(() => {
    const ctl = new AbortController();
    let first = true;
    const run = async () => {
      if (!first && document.hidden) return;
      setLoading(true);
      try {
        const d = await fetchRef.current(ctl.signal);
        if (ctl.signal.aborted) return;
        setData(d); setError(null); setUpdatedAt(Date.now());
      } catch (e) {
        if ((e as Error).name === "AbortError") return;
        setError((e as Error).message);
      } finally { if (!ctl.signal.aborted) setLoading(false); first = false; }
    };
    setData(null); setError(null);       // dependency changed: do not show another selection's data
    run();
    const id = pollMs > 0 ? window.setInterval(run, pollMs) : undefined;
    return () => { ctl.abort(); if (id) window.clearInterval(id); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, pollMs, tick]);

  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, updatedAt, reload };
}
