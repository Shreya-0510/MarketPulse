import { describe, expect, it } from "vitest";
import { ageSeconds, fmtNum, fmtPct, fmtTime, freshness } from "./format";
import { buildQuery } from "../api/client";
import { dedupeAppend, toFeedItem, type FeedItem } from "./feed";

describe("format", () => {
  it("never renders missing values as zero", () => { expect(fmtNum(null)).toBe("—"); expect(fmtPct(undefined)).toBe("—"); expect(fmtTime(null)).toBe("—"); });
  it("formats signed percentages", () => { expect(fmtPct(1.234)).toBe("+1.23%"); expect(fmtPct(-0.5)).toBe("-0.50%"); expect(fmtPct(0)).toBe("0.00%"); });
  it("freshness is UNKNOWN without timestamp", () => { expect(freshness(null, 60)).toBe("UNKNOWN"); expect(ageSeconds("garbage")).toBeNull(); });
  it("freshness thresholds", () => {
    const now = Date.parse("2026-01-01T01:00:00Z");
    expect(freshness("2026-01-01T00:59:30Z", 60, now)).toBe("FRESH");
    expect(freshness("2026-01-01T00:00:00Z", 60, now)).toBe("STALE");
  });
  it("buildQuery drops empty params", () => { expect(buildQuery({ a: "x", b: undefined, c: "", d: 0 })).toBe("?a=x&d=0"); });
});

const mk = (id: string): FeedItem => ({ key: id, kind: "social", origin: "live", id, time: null, ingestedAt: "", source: "s", entityIds: [], sentiment: null, title: "", body: "", url: null, receivedAt: 0 });

describe("feed", () => {
  it("dedupes by key, keeps newest first, bounds size", () => {
    let l = dedupeAppend([], [mk("a"), mk("b")], 3);
    expect(l.map((x) => x.key)).toEqual(["b", "a"]);
    l = dedupeAppend(l, [mk("b"), mk("c"), mk("d")], 3);
    expect(l.map((x) => x.key)).toEqual(["d", "c", "b"]);
  });
  it("labels connect-time history as snapshot and later events as live", () => {
    const post = (ing: string) => ({ type: "social_post" as const, sent_at: "x", payload: { event_id: ing, source: "bsky", ingested_at: ing, created_at: ing, entity_ids: ["nvda"], text: "t", sentiment_label: null, post_url: null } });
    const start = "2026-01-01T10:00:00Z";
    expect(toFeedItem(post("2026-01-01T09:00:00Z"), start, 0)?.origin).toBe("snapshot");
    expect(toFeedItem(post("2026-01-01T10:00:05Z"), start, 0)?.origin).toBe("live");
    expect(toFeedItem({ type: "ping", sent_at: start, payload: {} }, start, 0)).toBeNull();
  });
});

import { indexSeries, periodReturn, sumActivity, sumSentiment } from "./series";
import type { Bar, EntityMetrics } from "../types/api";

const bar = (d: string, c: number): Bar => ({ event_id: d, symbol: "X", entity_id: "x", interval: "1d", event_time: `${d}T00:00:00Z`, open: null, high: null, low: null, close: c, adjusted_close: null, volume: null, currency: "USD", data_mode: "historical", source: "yf", ingested_at: "", processed_at: null });
const st = (count: number, p = 0, n = 0, u = 0) => ({ count, scored: p + n + u, positive: p, neutral: u, negative: n, avg_score: null });
const win = (start: string, soc: number, news: number): EntityMetrics => ({ entity_id: "e", metric: "combined", window_start: start, window_end: start, window_minutes: 60, social: st(soc, soc), news: st(news, 0, news), total_mentions: soc + news, computed_at: start });

describe("series", () => {
  it("rebases to 100 and leaves gaps as null", () => {
    const rows = indexSeries({ A: [bar("2026-01-01", 50), bar("2026-01-02", 55)], B: [bar("2026-01-02", 10)] });
    expect(rows[0].A).toBe(100); expect(rows[1].A).toBeCloseTo(110); expect(rows[0].B).toBeNull(); expect(rows[1].B).toBe(100);
  });
  it("period return needs a reference bar at/before the target", () => {
    const bars = [bar("2026-01-01", 100), bar("2026-01-08", 110)];
    expect(periodReturn(bars, 7).pct).toBeCloseTo(10);
    expect(periodReturn(bars, 30).pct).toBeNull();
    expect(periodReturn([bar("2026-01-01", 1)], 7).pct).toBeNull();
  });
  it("sums activity and sentiment across entities and respects cutoff", () => {
    const a = [win("2026-01-01T00:00:00Z", 2, 1), win("2025-12-01T00:00:00Z", 9, 9)];
    const b = [win("2026-01-01T00:00:00Z", 3, 0)];
    const cut = Date.parse("2026-01-01T00:00:00Z");
    expect(sumActivity([a, b], cut)).toEqual([{ t: "2026-01-01T00:00:00Z", social: 5, news: 1 }]);
    const s = sumSentiment([a, b], "social", cut);
    expect(s.positive).toBe(5); expect(s.total).toBe(5); expect(sumSentiment([a, b], "news", cut).negative).toBe(1);
  });
});
