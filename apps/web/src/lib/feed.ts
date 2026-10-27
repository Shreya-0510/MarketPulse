import type { AnomalyEvent, NewsArticle, SocialPost, WSMessage } from "../types/api";

export type Origin = "live" | "snapshot";   // snapshot = sent by the server on connect (history), NOT newly ingested
export interface FeedItem {
  key: string; kind: "social" | "news" | "anomaly"; origin: Origin; id: string;
  time: string | null; ingestedAt: string; source: string; entityIds: string[];
  sentiment: string | null; title: string; body: string; url: string | null; receivedAt: number;
}

/** Newest-first merge; drops duplicate keys (existing wins) and truncates to `max`. */
export function dedupeAppend(existing: FeedItem[], incoming: FeedItem[], max: number): FeedItem[] {
  const seen = new Set(existing.map((e) => e.key));
  const fresh: FeedItem[] = [];
  for (const i of incoming) if (!seen.has(i.key)) { seen.add(i.key); fresh.push(i); }
  return [...fresh.reverse(), ...existing].slice(0, max);
}

/**
 * Convert a WS message to a FeedItem. `sessionStartIso` is the server `sent_at` of the first message on this
 * connection: items whose ingested_at is earlier were already persisted when we connected -> "snapshot".
 * Uses server clocks only (no browser/server skew).
 */
export function toFeedItem(m: WSMessage, sessionStartIso: string, receivedAt: number): FeedItem | null {
  const ts = (iso: string) => new Date(iso).getTime();
  if (m.type === "social_post") {
    const p = m.payload as SocialPost;
    return { key: `s:${p.event_id}`, kind: "social", id: p.event_id, time: p.created_at ?? p.event_time, ingestedAt: p.ingested_at,
      origin: ts(p.ingested_at) >= ts(sessionStartIso) ? "live" : "snapshot", source: p.source, entityIds: p.entity_ids ?? [],
      sentiment: p.sentiment_label, title: "", body: p.text, url: p.post_url, receivedAt };
  }
  if (m.type === "news_article") {
    const a = m.payload as NewsArticle;
    return { key: `n:${a.event_id}`, kind: "news", id: a.event_id, time: a.published_at, ingestedAt: a.ingested_at,
      origin: ts(a.ingested_at) >= ts(sessionStartIso) ? "live" : "snapshot", source: a.publisher || a.source, entityIds: a.entity_ids ?? [],
      sentiment: a.sentiment_label, title: a.title, body: a.summary ?? "", url: a.url, receivedAt };
  }
  if (m.type === "anomaly") {
    const a = m.payload as AnomalyEvent;
    return { key: `a:${a.anomaly_id}`, kind: "anomaly", id: a.anomaly_id, time: a.detected_at, ingestedAt: a.detected_at,
      origin: ts(a.detected_at) >= ts(sessionStartIso) ? "live" : "snapshot", source: a.source_type, entityIds: [a.entity_id], sentiment: null,
      title: `${a.source_type} ${a.direction}`, body: `observed ${a.observed_count} vs baseline ${a.baseline_mean.toFixed(1)} ± ${a.baseline_std.toFixed(1)} (z=${a.z_score.toFixed(2)})`,
      url: null, receivedAt };
  }
  return null;
}
