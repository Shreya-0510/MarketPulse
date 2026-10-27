import { useMemo, useState } from "react";
import { ExternalLink, Pause, Play, Trash2 } from "lucide-react";
import { Banner, Card, Empty, ExtLink, Pill, SentimentTag } from "../components/ui";
import { MAX_FEED, type LiveFeed } from "../hooks/useLiveFeed";
import { ageSeconds, fmtAge, fmtTime } from "../lib/format";
import type { EntitySummary } from "../types/api";

const STREAM_STALE_SEC = 120;   // no message (incl. 20s server pings) for this long => stale

export default function Firehose({ entities, feed, nowTick, entityFilter, setEntityFilter }: { entities: EntitySummary[]; feed: LiveFeed; nowTick: number; entityFilter: string; setEntityFilter: (e: string) => void }) {
  const [kind, setKind] = useState("all"); const [source, setSource] = useState("all"); const [sent, setSent] = useState("all");
  const [liveOnly, setLiveOnly] = useState(false); const [q, setQ] = useState("");
  const names = useMemo(() => Object.fromEntries(entities.map((e) => [e.entity_id, e.display_name])), [entities]);
  const sources = useMemo(() => [...new Set(feed.items.map((i) => i.source))].sort(), [feed.items]);
  const rows = feed.items.filter((i) => (kind === "all" || i.kind === kind) && (source === "all" || i.source === source) &&
    (sent === "all" || (sent === "unscored" ? !i.sentiment : i.sentiment === sent)) && (!liveOnly || i.origin === "live") &&
    (!q || `${i.title} ${i.body}`.toLowerCase().includes(q.toLowerCase())));
  const lastAge = ageSeconds(feed.lastMessageAt ? new Date(feed.lastMessageAt).toISOString() : null, nowTick);
  const streamStale = feed.state === "open" && (lastAge === null || lastAge > STREAM_STALE_SEC);
  const liveCount = feed.items.filter((i) => i.origin === "live").length;

  return (
    <div className="space-y-4">
      <Card>
        <div className="flex flex-wrap items-center gap-3 text-sm">
          <Pill label={`WebSocket: ${feed.state}${feed.state === "reconnecting" ? ` (attempt ${feed.reconnectAttempt})` : ""}`} tone={feed.state} />
          {streamStale && <Pill label="STREAM STALE" tone="STALE" />}
          <span className="text-xs text-ink-500">last message (any type): {feed.lastMessageAt ? fmtAge(lastAge) : "none yet"} · last server ping: {feed.lastPingAt ? fmtAge(ageSeconds(new Date(feed.lastPingAt).toISOString(), nowTick)) : "none yet"}</span>
          <span className="text-xs text-ink-500">last newly-ingested event: {feed.lastLiveEventAt ? fmtAge(ageSeconds(new Date(feed.lastLiveEventAt).toISOString(), nowTick)) : "none this session"}</span>
          <div className="ml-auto flex gap-2">
            <button onClick={() => feed.setPaused(!feed.paused)} className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50">{feed.paused ? <Play className="h-3 w-3" /> : <Pause className="h-3 w-3" />}{feed.paused ? `Resume (${feed.pendingWhilePaused} buffered)` : "Pause display"}</button>
            <button onClick={feed.clear} className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50"><Trash2 className="h-3 w-3" />Clear</button></div>
        </div>
        {feed.error && <div className="mt-2"><Banner tone="warn">{feed.error}</Banner></div>}
        {feed.paused && <div className="mt-2"><Banner tone="info">Display paused. Ingestion and the WebSocket keep running; new events are buffered (bounded) and merged on resume.</Banner></div>}
        {feed.state === "open" && !feed.lastMessageAt && <p className="mt-2 text-xs text-ink-500">Connected. Waiting for the server snapshot — an empty database sends nothing until new matched events are persisted.</p>}
      </Card>

      <Card>
        <div className="grid gap-3 text-xs text-ink-500 sm:grid-cols-3 lg:grid-cols-6">
          <Sel label="Entity (server-side)" value={entityFilter} onChange={setEntityFilter} options={[["", "All entities"], ...entities.map((e): [string, string] => [e.entity_id, e.display_name])]} />
          <Sel label="Type" value={kind} onChange={setKind} options={[["all", "All"], ["social", "Social"], ["news", "News"], ["anomaly", "Anomalies"]]} />
          <Sel label="Source" value={source} onChange={setSource} options={[["all", "All"], ...sources.map((s): [string, string] => [s, s])]} />
          <Sel label="Sentiment" value={sent} onChange={setSent} options={[["all", "All"], ["positive", "Positive"], ["neutral", "Neutral"], ["negative", "Negative"], ["unscored", "Unscored"]]} />
          <label>Search text<input value={q} onChange={(e) => setQ(e.target.value)} className="mt-1 block w-full rounded border border-slate-300 px-2 py-1.5 text-sm text-ink-900" /></label>
          <label className="flex items-end gap-2 pb-1.5 text-sm text-ink-700"><input type="checkbox" checked={liveOnly} onChange={(e) => setLiveOnly(e.target.checked)} />Newly ingested only</label>
        </div>
        <p className="mt-2 text-xs text-ink-500">{rows.length} shown · {feed.items.length} held (max {MAX_FEED}, oldest dropped) · {liveCount} newly ingested this session. On connect the server replays recent persisted items; those are labelled <b>snapshot</b> and are not new arrivals.</p>
      </Card>

      <Card title="Events">
        {rows.length === 0 ? <Empty title={feed.items.length ? "No events match the filters" : "No events received"} hint={feed.items.length ? undefined : "Nothing persisted matches yet. Check the Pipeline tab for source and Spark status."} /> :
          <ul className="divide-y divide-slate-100">{rows.map((i) => <li key={i.key} className="py-2.5">
            <div className="flex flex-wrap items-center gap-2 text-xs text-ink-500">
              <span className={`rounded px-1.5 py-0.5 font-medium ${i.kind === "news" ? "bg-teal-50 text-teal-800" : i.kind === "social" ? "bg-blue-50 text-blue-800" : "bg-amber-50 text-amber-900"}`}>{i.kind}</span>
              <span className={`rounded border px-1.5 py-0.5 ${i.origin === "live" ? "border-emerald-300 text-emerald-800" : "border-slate-200 text-ink-500"}`}>{i.origin === "live" ? "newly ingested" : "snapshot"}</span>
              <span>{i.source}</span><span title="Source-reported time">event {fmtTime(i.time)}</span><span title="Time the pipeline ingested it">ingested {fmtTime(i.ingestedAt)}</span>
              {i.kind !== "anomaly" && <SentimentTag label={i.sentiment} />}
              {i.entityIds.map((e) => <span key={e} className="rounded bg-brand-50 px-1.5 text-brand-700">{names[e] ?? e}</span>)}</div>
            {i.title && <div className="mt-1 text-sm font-medium"><ExtLink href={i.kind === "news" ? i.url : null}>{i.title}</ExtLink>{i.kind === "news" && i.url && <ExternalLink className="ml-1 inline h-3 w-3 text-ink-300" />}</div>}
            {i.body && <p className="mt-0.5 line-clamp-3 break-words text-sm text-ink-700">{i.body}</p>}
            {i.kind === "social" && i.url && <ExtLink href={i.url}><span className="text-xs">original post</span></ExtLink>}
          </li>)}</ul>}
      </Card>
    </div>
  );
}

function Sel({ label, value, onChange, options }: { label: string; value: string; onChange: (v: string) => void; options: [string, string][] }) {
  return <label>{label}<select value={value} onChange={(e) => onChange(e.target.value)} className="mt-1 block w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-sm text-ink-900">{options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}</select></label>;
}
