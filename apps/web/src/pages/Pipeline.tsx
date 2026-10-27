import type { ReactNode } from "react";
import { api } from "../api/client";
import { Async, Banner, Card, Pill } from "../components/ui";
import { useApi } from "../hooks/useApi";
import { ageSeconds, fmtAge, fmtInt, fmtTime } from "../lib/format";

const STALE_SOURCE_SEC = 3600;

export default function Pipeline({ nowTick }: { nowTick: number }) {
  const src = useApi((s) => api.sourcesStatus(s), [], 15000);
  const pipe = useApi((s) => api.pipelineStatus(s), [], 15000);
  const health = useApi((s) => api.health(s), [], 15000);
  const spark = (pipe.data?.components ?? []).filter((c) => c.component === "spark");
  const other = (pipe.data?.components ?? []).filter((c) => c.component !== "spark" && c.component !== "ingestion");

  return (
    <div className="space-y-4">
      <Banner tone="info">This page shows <b>recorded</b> status documents written by the adapters and Spark jobs. “Process running” is not “healthy”: judge the <i>last success</i> and <i>updated</i> times. Fields the platform does not instrument are shown as UNKNOWN, never as zero.</Banner>

      <Card title="Layer status">
        <div className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
          <Cell label="API process" value={health.data?.process ?? (health.error ? "unreachable" : "…")} bad={!!health.error} />
          <Cell label="MongoDB (live ping by API)" value={pipe.data?.mongodb ?? health.data?.mongodb ?? "…"} bad={!!pipe.error || (pipe.data ? pipe.data.mongodb !== "ok" : false)} />
          <Cell label="Kafka lag / health" value={pipe.data?.kafka ?? "UNKNOWN"} note="not instrumented" />
          <Cell label="Status snapshot taken" value={pipe.data ? fmtTime(pipe.data.checked_at) : "…"} />
        </div>
        {pipe.error && <p className="mt-2 text-sm text-red-700">{pipe.error}</p>}
      </Card>

      <Card title="Ingestion sources" subtitle="From /sources/status (config/sources.yaml + recorded adapter status).">
        <Async {...src} h={160} isEmpty={(d) => d.length === 0} empty={{ title: "No sources configured" }}>
          {(d) => <div className="overflow-x-auto"><table className="w-full text-xs">
            <thead className="text-left text-ink-500"><tr><th className="py-1">Source</th><th>Type</th><th>Enabled</th><th>State</th><th>Verification</th><th>Last attempt</th><th>Last success</th><th>Status updated</th><th>Totals</th><th>Last error</th></tr></thead>
            <tbody className="divide-y divide-slate-100 align-top">{d.map((s) => { const stale = s.enabled && s.updated_at && (ageSeconds(s.updated_at, nowTick) ?? 0) > STALE_SOURCE_SEC; return <tr key={s.name}>
              <td className="py-1.5 font-medium text-ink-900">{s.name}<div className="max-w-[16rem] truncate font-normal text-ink-500" title={s.url}>{s.url}</div></td><td>{s.type}</td><td>{s.enabled ? "yes" : "no"}</td>
              <td><Pill label={s.state} />{stale && <div className="mt-1"><Pill label="status stale" tone="STALE" /></div>}</td>
              <td title="OBSERVED_SUCCESS only after the adapter recorded a successful fetch">{s.verification}</td>
              <td>{fmtTime(s.last_attempt_at)}</td><td>{fmtTime(s.last_success_at)}<div className="text-ink-500">{s.last_success_at ? fmtAge(ageSeconds(s.last_success_at, nowTick)) : ""}</div></td>
              <td>{fmtTime(s.updated_at)}</td><td>{Object.keys(s.totals).length ? Object.entries(s.totals).map(([k, v]) => <div key={k}>{k}: {fmtInt(v)}</div>) : "—"}</td>
              <td className="max-w-[14rem] break-words text-red-700">{s.last_error ?? "—"}</td></tr>; })}</tbody></table></div>}</Async>
      </Card>

      <Card title="Spark Structured Streaming queries" subtitle="Written by each foreachBatch after a micro-batch. No batch ⇒ no update (idle ≠ failed).">
        <Async {...pipe} h={120} isEmpty={() => spark.length === 0} empty={{ title: "No Spark status recorded yet", hint: "Spark writes status after its first non-empty micro-batch." }}>
          {() => <div className="grid gap-3 lg:grid-cols-3">{spark.map((c) => <div key={c.name} className="rounded border border-slate-200 p-3 text-xs">
            <div className="mb-1 flex items-center justify-between"><b className="text-sm">{c.name}</b><Pill label={String(c.state ?? "UNKNOWN")} tone={c.state === "RUNNING" ? "OK" : c.state} /></div>
            <Kv k="Topic" v={c.topic} /><Kv k="Last batch" v={c.last_batch_id !== undefined ? `#${c.last_batch_id} at ${fmtTime(c.last_batch_at)} (${fmtAge(ageSeconds(c.last_batch_at, nowTick))})` : "—"} />
            <Kv k="Last batch stats" v={c.last_batch ? Object.entries(c.last_batch).map(([k, v]) => `${k}=${v}`).join(", ") : "—"} />
            <Kv k="Totals" v={c.totals ? Object.entries(c.totals).map(([k, v]) => `${k}=${v}`).join(", ") : "—"} /><Kv k="Checkpoint" v={c.checkpoint} /><Kv k="Last error" v={c.last_error ?? "—"} /></div>)}</div>}</Async>
      </Card>

      {other.length > 0 && <Card title="Other recorded components"><pre className="overflow-x-auto text-xs">{JSON.stringify(other, null, 2)}</pre></Card>}

      <Card title="Architecture & data flow">
        <div className="overflow-x-auto"><div className="flex min-w-[820px] items-center gap-2 text-center text-xs">
          {[["yfinance · Bluesky Jetstream · RSS/Atom", "Sources"], ["ingestion-market / -social / -news", "Adapters"], ["market.raw · social.raw · news.raw", "Kafka (KRaft)"], ["market_stream · social_stream · news_stream", "Spark Structured Streaming"], ["market_prices · social_posts · news_articles", "MongoDB"], ["analytics_job (Python, polls Mongo)", "entity/attention/anomaly metrics"], ["REST + WebSocket", "FastAPI"], ["this dashboard", "React"]].map(([a, b], i, arr) => <div key={b} className="flex items-center gap-2">
            <div className="rounded border border-slate-300 bg-white px-2 py-2"><div className="font-semibold text-ink-900">{b}</div><div className="text-ink-500">{a}</div></div>{i < arr.length - 1 && <span className="text-ink-300">→</span>}</div>)}</div></div>
        <p className="mt-2 text-xs text-ink-500">Adapters publish only to Kafka. Spark persists idempotently (upsert on stable event_id). analytics_job reads persisted data, so metrics lag ingestion by up to one cycle.</p>
      </Card>

      <Card title="Methodology & limitations">
        <div className="grid gap-4 text-sm text-ink-700 lg:grid-cols-2">
          <Sec t="Market data">Daily OHLCV bars from Yahoo Finance via yfinance (unofficial, not an exchange feed; no guaranteed real-time quotes). Records are labelled <code>historical</code>; the current session's bar may be provisional. Change % = (latest close − previous available daily close) / previous close × 100. “Indexed to 100” charts rebase for display only. Relative-performance cards on the Entity page are simple returns computed in the browser from persisted daily bars.</Sec>
          <Sec t="Social data">Public Bluesky posts via Jetstream, matched to entities with configurable aliases and ambiguity rules. Bluesky is one platform and not representative of all investors; counts are not market-wide attention.</Sec>
          <Sec t="News data">Headlines and permitted summaries from configured RSS/Atom feeds. Publication time is source-reported and never replaced by ingestion time; items without one are flagged. Publisher is marked “inferred” when only derived from an aggregator.</Sec>
          <Sec t="Sentiment">VADER compound score in [−1, 1]. Label: positive ≥ +0.05, negative ≤ −0.05, otherwise neutral (defaults in <code>config/settings.yaml</code>; check the file for current values). News is scored on headlines only. Unscored items are shown as “unscored”, never as neutral. Social and news sentiment are shown separately.</Sec>
          <Sec t="Windows & attention">analytics_job aggregates matches per fixed UTC-aligned window (default 60 min) from persisted production records (<code>test</code> data excluded). Entity-matches may count one item under several entities.</Sec>
          <Sec t="Anomalies">z = (observed − baseline mean) / baseline std over the previous N windows (default 24). Emitted only with enough history, a minimum count (default 5), and a stable baseline (std ≥ 0.5). Hence none appear for roughly the first day.</Sec>
          <Sec t="Cross-signals">Descriptive co-occurrence rules (e.g. price up while average social sentiment is down) with supporting counts. No causation, no prediction.</Sec>
          <Sec t="Retention & delivery">Social/news TTL on ingested_at (defaults 14 / 30 days); market bars kept. Delivery is at-least-once with idempotent upserts. Kafka lag is not instrumented. This application provides no investment advice.</Sec>
        </div>
      </Card>
    </div>
  );
}

const Cell = ({ label, value, bad, note }: { label: string; value: string; bad?: boolean; note?: string }) => <div className="rounded border border-slate-100 p-2"><div className="text-xs text-ink-500">{label}</div><div className={`font-medium ${bad ? "text-red-700" : value === "UNKNOWN" ? "text-ink-500" : "text-ink-900"}`}>{value}</div>{note && <div className="text-[11px] text-ink-500">{note}</div>}</div>;
const Kv = ({ k, v }: { k: string; v: unknown }) => <div className="flex gap-2 py-0.5"><span className="w-24 shrink-0 text-ink-500">{k}</span><span className="break-all">{v === undefined || v === null ? "—" : String(v)}</span></div>;
const Sec = ({ t, children }: { t: string; children: ReactNode }) => <div><h3 className="mb-1 text-sm font-semibold text-ink-900">{t}</h3><p className="text-xs leading-relaxed">{children}</p></div>;
