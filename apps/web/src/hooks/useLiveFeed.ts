import { useCallback, useEffect, useRef, useState } from "react";
import { wsUrl } from "../api/client";
import { dedupeAppend, toFeedItem, type FeedItem } from "../lib/feed";
import type { WSMessage } from "../types/api";

export type WSState = "connecting" | "open" | "reconnecting" | "closed";
export const MAX_FEED = 300;

export interface LiveFeed {
  state: WSState; items: FeedItem[]; lastMessageAt: number | null; lastLiveEventAt: number | null; lastPingAt: number | null;
  reconnectAttempt: number; paused: boolean; setPaused: (p: boolean) => void; pendingWhilePaused: number; clear: () => void; error: string | null;
}

/**
 * Bounded WebSocket consumer with exponential-backoff reconnect (1s..30s) and full cleanup on unmount.
 * Pause only freezes DISPLAY: messages keep being received into a buffer (bounded) and are merged on resume.
 */
export function useLiveFeed(entity?: string): LiveFeed {
  const [state, setState] = useState<WSState>("connecting");
  const [items, setItems] = useState<FeedItem[]>([]);
  const [lastMessageAt, setLast] = useState<number | null>(null);
  const [lastLiveEventAt, setLastLive] = useState<number | null>(null);
  const [lastPingAt, setPing] = useState<number | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [paused, setPausedState] = useState(false);
  const [pending, setPending] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const pausedRef = useRef(false);
  const buffer = useRef<FeedItem[]>([]);

  const setPaused = useCallback((p: boolean) => {
    pausedRef.current = p; setPausedState(p);
    if (!p && buffer.current.length) { const b = buffer.current; buffer.current = []; setPending(0); setItems((cur) => dedupeAppend(cur, b, MAX_FEED)); }
  }, []);
  const clear = useCallback(() => { setItems([]); buffer.current = []; setPending(0); }, []);

  useEffect(() => {
    let ws: WebSocket | null = null; let timer: number | undefined; let stopped = false; let tries = 0;
    let sessionStart: string | null = null;
    setItems([]); buffer.current = []; setPending(0);

    const connect = () => {
      if (stopped) return;
      setState(tries === 0 ? "connecting" : "reconnecting");
      sessionStart = null;
      try { ws = new WebSocket(wsUrl({ entity })); } catch (e) { setError((e as Error).message); schedule(); return; }
      ws.onopen = () => { tries = 0; setAttempt(0); setState("open"); setError(null); };
      ws.onmessage = (ev) => {
        let m: WSMessage;
        try { m = JSON.parse(ev.data as string); } catch { return; }
        const now = Date.now();
        setLast(now);
        if (!sessionStart) sessionStart = m.sent_at;
        if (m.type === "ping") { setPing(now); return; }
        if (m.type === "error") { setError(String(m.payload?.message ?? "server error")); return; }
        const item = toFeedItem(m, sessionStart, now);
        if (!item) return;
        if (item.origin === "live") setLastLive(now);
        if (pausedRef.current) { buffer.current = dedupeAppend(buffer.current, [item], MAX_FEED); setPending(buffer.current.length); }
        else setItems((cur) => dedupeAppend(cur, [item], MAX_FEED));
      };
      ws.onerror = () => setError("WebSocket error (is the API running?)");
      ws.onclose = () => { ws = null; if (!stopped) schedule(); };
    };
    const schedule = () => {
      tries += 1; setAttempt(tries); setState("reconnecting");
      timer = window.setTimeout(connect, Math.min(30000, 1000 * 2 ** Math.min(tries - 1, 5)));
    };
    connect();
    return () => { stopped = true; if (timer) window.clearTimeout(timer); if (ws) { ws.onclose = null; ws.close(); } setState("closed"); };
  }, [entity]);

  return { state, items, lastMessageAt, lastLiveEventAt, lastPingAt, reconnectAttempt: attempt, paused, setPaused, pendingWhilePaused: pending, clear, error };
}
