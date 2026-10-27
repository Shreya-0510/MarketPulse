import { useMemo, useState } from "react";
import { Area, AreaChart, Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "../api/client";
import { HeadlineRow, PostRow } from "../components/Items";
import { Async, Banner, Card, Change, Empty, ErrorState, Hint, ModePill, Pill, SentimentBar, Skeleton, chartTooltip } from "../components/ui";
import { useAllMetrics } from "../hooks/useAllMetrics";
import { useApi } from "../hooks/useApi";
import { fmtAge, fmtHour, fmtDay, fmtInt, fmtNum, fmtTime, ageSeconds } from "../lib/format";
import { indexSeries, isProd, sumActivity, sumSentiment } from "../lib/series";
import type { Bar as BarT, EntitySummary, WatchlistItem } from "../types/api";

const RANGES = { "1M": 30, "3M": 90, "6M": 180, "1Y": 365 } as const;
const ACT = { "24h": 24, "72h": 72, "7d": 168 } as const;
const COLORS = ["#1d4ed8", "#0f766e", "#b45309", "#7c3aed", "#be123c", "#475569"];

export default function Overview({ entities, onOpenEntity, nowTick }: { entities: EntitySummary[]; onOpenEntity: (id: string) => void; nowTick: number }) {
  const names = useMemo(() => Object.fromEntries(entities.map((e) => [e.entity_id, e.display_name])), [entities]);
  const wl = useApi((s) => api.watchlist(s), [], 60000);
  const src = useApi((s) => api.sourcesStatus(s), [], 30000);
  const [range, setRange] = useState<keyof typeof RANGES>("3M");
  const [act, setAct] = useState<keyof typeof ACT>("72h");
  const [picked, setPicked] = useState<string[] | null>(null);

  const withData = (wl.data?.items ?? []).filter((i) => i.status !== "NO_DATA");
  const selected = picked ?? withData.slice(0, 3).map((i) => i.symbol);

  const hist = useApi(async (signal) => {
    const start = new Date(Date.now() - RANGES[range] * 86400000).toISOString();
    const res = await Promise.allSettled(selected.map((s) => api.history(s, { start }, signal)));
    const out: Record<string, BarT[]> = {}; const failed: string[] = [];
    res.forEach((r, i) => (r.status === "fulfilled" ? (out[selected[i]] = r.value.bars) : failed.push(selected[i])));
    return { out, failed };
  }, [selected.join(","), range]);

  const metrics = useAllMetrics(entities, ACT[act]);
  const cutoff = Date.now() - ACT[act] * 3600000;
  const att = useApi(async (s) => {
    const start = new Date(Date.now() - 24 * 3600000).toISOString();
    const [soc, news] = await Promise.all([api.attention({ source_type: "social", start, limit: 1000 }, s), api.attention({ source_type: "news", start, limit: 1000 }, s)]);
    return { soc, news };
  }, [], 60000);
  const anomalies = useApi((s) => api.anomalies({ limit: 8 }, s), [], 60000);
  const news = useApi((s) => api.news({ limit: 30 }, s), [], 60000);
  const social = useApi((s) => api.social({ limit: 30 }, s), [], 30000);

  const top = useMemo(() => {
    if (!att.data) return [];
    const m = new Map<string, { social: number; news: number }>();
    for (const [k, r] of [["social", att.data.soc], ["news", att.data.news]] as const)
      for (const i of r.items) { const x = m.get(i.entity_id) ?? { social: 0, news: 0 }; x[k] += i.count; m.set(i.entity_id, x); }
    return [...m.entries()].map(([id, v]) => ({ id, name: names[id] ?? id, ...v, total: v.social + v.news })).sort((a, b) => b.total - a.total).slice(0, 8);
  }, [att.data, names]);

  const activity = useMemo(() => (metrics.data ? sumActivity(metrics.data.windows, cutoff) : []), [metrics.data, cutoff]);
  const socSent = metrics.data ? sumSentiment(metrics.data.windows, "social", cutoff) : null;
  const newsSent = metrics.data ? sumSentiment(metrics.data.windows, "news", cutoff) : null;
  const indexRows = useMemo(() => (hist.data ? indexSeries(hist.data.out) : []), [hist.data]);

  const staleItems = (wl.data?.items ?? []).filter((i) => i.status === "STALE");
  const failedSources = (src.data ?? []).filter((s) => s.state === "FAILED" || s.state === "DEGRADED");
  const attTrunc = att.data && (att.data.soc.count >= 1000 || att.data.news.count >= 1000);

  return (
    <div className="space-y-4">
      {wl.error && !wl.data && <ErrorState message={wl.error} onRetry={wl.reload} />}
      {wl.data && withData.length === 0 && <Banner tone="warn"><b>No persisted market data.</b> The market pipeline has not delivered bars yet (see Pipeline tab). Nothing is shown rather than placeholder values.</Banner>}
      {staleItems.length > 0 && <Banner tone="warn"><b>Stale market data:</b> {staleItems.map((i) => i.symbol).join(", ")} — latest bar is older than {wl.data?.stale_after_days} days. Weekends/holidays alone can explain a short gap.</Banner>}
      {failedSources.length > 0 && <Banner tone="error"><b>Source problems:</b> {failedSources.map((s) => `${s.name} (${s.state})`).join(", ")}. Previously collected records remain visible.</Banner>}

      <Card title="Watchlist" subtitle="Latest persisted daily bar vs previous available daily bar. Daily closes, not live ticks." right={wl.updatedAt && <span className="text-xs text-ink-500">updated {fmtAge(ageSeconds(new Date(wl.updatedAt).toISOString(), nowTick))}</span>}>
        <Async {...wl} h={200} isEmpty={(d) => d.items.length === 0} empty={{ title: "Watchlist is empty", hint: "Add instruments in config/watchlist.yaml." }}>
          {(d) => <WatchTable items={d.items} selected={selected} onToggle={(sym) => setPicked((cur) => { const c = cur ?? selected; return c.includes(sym) ? c.filter((x) => x !== sym) : [...c, sym].slice(-6); })} onOpen={onOpenEntity} />}
        </Async>
      </Card>

      <Card title="Market performance (indexed to 100)" subtitle="Each line starts at 100 on its first bar in range — a display normalisation so different currencies are comparable. Click watchlist rows to add/remove (max 6)."
        right={<Seg value={range} options={Object.keys(RANGES)} onChange={(v) => setRange(v as keyof typeof RANGES)} />}>
        {selected.length === 0 ? <Empty title="No instruments with data selected" /> : hist.error && !hist.data ? <ErrorState message={hist.error} onRetry={hist.reload} /> : !hist.data ? <Skeleton h={280} /> :
          indexRows.length < 2 ? <Empty title="Not enough bars in this range" hint="At least two persisted daily bars are needed to draw a line." /> : <>
            {hist.data.failed.length > 0 && <div className="mb-2"><Banner tone="warn">History failed for {hist.data.failed.join(", ")}.</Banner></div>}
            <div className="h-72"><ResponsiveContainer><LineChart data={indexRows} margin={{ left: 0, right: 12, top: 5 }}>
              <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" /><XAxis dataKey="date" tickFormatter={fmtDay} fontSize={11} minTickGap={30} />
              <YAxis fontSize={11} domain={["auto", "auto"]} width={44} label={{ value: "Index (start = 100)", angle: -90, fontSize: 11, position: "insideLeft", dx: 10 }} />
              <Tooltip {...chartTooltip} formatter={(v: number) => fmtNum(v, 1)} /><Legend wrapperStyle={{ fontSize: 12 }} />
              {Object.keys(hist.data.out).map((s, i) => <Line key={s} dataKey={s} stroke={COLORS[i % COLORS.length]} dot={false} strokeWidth={2} connectNulls={false} isAnimationActive={false} />)}
            </LineChart></ResponsiveContainer></div></>}
      </Card>

      <div className="flex items-center justify-between"><h2 className="text-sm font-semibold text-ink-700">Public discussion &amp; news activity</h2><Seg value={act} options={Object.keys(ACT)} onChange={(v) => setAct(v as keyof typeof ACT)} /></div>
      {metrics.data && metrics.data.failed.length > 0 && <Banner tone="warn">Metrics unavailable for: {metrics.data.failed.join(", ")}.</Banner>}
      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Social matches per window" subtitle={<>Bluesky only — not representative of all investors. Entity-matches: a post matching 2 entities counts twice. <Hint text="Source: /entities/{id}/metrics (analytics_job, production data modes only), summed across entities per window." /></>}>
          <Async {...metrics} h={200} isEmpty={() => activity.length === 0} empty={{ title: "No computed windows yet", hint: "analytics_job writes the first windows after its first cycle (default every 5 min) once matching posts exist." }}>
            {() => <Vol data={activity} k="social" color="#1d4ed8" />}</Async></Card>
        <Card title="News matches per window" subtitle="Articles matched to configured entities (publication-time windows are not used; ingestion windows per analytics_job).">
          <Async {...metrics} h={200} isEmpty={() => activity.length === 0} empty={{ title: "No computed windows yet" }}>{() => <Vol data={activity} k="news" color="#0f766e" />}</Async></Card>
        <Card title="Social sentiment distribution" subtitle="VADER compound labels over the selected period. Shown separately from news; the two are not comparable composites.">
          <Async {...metrics} h={60}>{() => socSent && <SentimentBar {...socSent} total={socSent.total} />}</Async></Card>
        <Card title="News sentiment distribution" subtitle="VADER on headlines only (titles, not article bodies).">
          <Async {...metrics} h={60}>{() => newsSent && <SentimentBar {...newsSent} total={newsSent.total} />}</Async></Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Highest recent attention (24h)" subtitle="Sum of social + news window counts per entity from /attention.">
          {attTrunc && <div className="mb-2"><Banner tone="warn">Attention response hit the 1000-row cap; totals may be understated.</Banner></div>}
          <Async {...att} h={200} isEmpty={() => top.length === 0} empty={{ title: "No attention windows in the last 24h" }}>
            {() => <div className="h-64"><ResponsiveContainer><BarChart data={top} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid stroke="#e2e8f0" horizontal={false} /><XAxis type="number" fontSize={11} allowDecimals={false} /><YAxis type="category" dataKey="name" width={110} fontSize={11} />
              <Tooltip {...chartTooltip} /><Legend wrapperStyle={{ fontSize: 12 }} />
              <Bar dataKey="social" stackId="a" fill="#1d4ed8" name="social" isAnimationActive={false} /><Bar dataKey="news" stackId="a" fill="#0f766e" name="news" isAnimationActive={false} /></BarChart></ResponsiveContainer></div>}</Async></Card>
        <Card title="Recent attention anomalies" subtitle="z-score vs rolling baseline; requires enough history and minimum volume.">
          <Async {...anomalies} h={200} isEmpty={(d) => d.items.length === 0} empty={{ title: "No anomalies detected", hint: "Anomalies need ≥ baseline_windows (24) of history and a minimum count. Absence is not an error." }}>
            {(d) => <ul className="divide-y divide-slate-100 text-sm">{d.items.map((a) => <li key={a.anomaly_id} className="py-2">
              <div className="flex items-center justify-between"><button className="font-medium text-brand-600 hover:underline" onClick={() => onOpenEntity(a.entity_id)}>{names[a.entity_id] ?? a.entity_id}</button>
                <span className={`text-xs font-medium ${a.direction === "spike" ? "text-amber-700" : "text-ink-500"}`}>{a.source_type} {a.direction} · z={fmtNum(a.z_score, 2)}</span></div>
              <p className="text-xs text-ink-500">observed {fmtInt(a.observed_count)} vs mean {fmtNum(a.baseline_mean, 1)} ± {fmtNum(a.baseline_std, 1)} over {a.baseline_windows_used} windows · {a.window_minutes}-min window {fmtTime(a.window_start)}</p></li>)}</ul>}</Async></Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card title="Latest headlines" subtitle="Publication time as reported by the source; link opens the publisher.">
          <Async {...news} h={200} isEmpty={(d) => d.items.filter(isProd).length === 0} empty={{ title: "No persisted articles", hint: "News feeds may still be fetching, or none matched." }}>
            {(d) => <ul className="divide-y divide-slate-100">{d.items.filter(isProd).slice(0, 8).map((a) => <HeadlineRow key={a.event_id} a={a} names={names} />)}</ul>}</Async></Card>
        <Card title="Recent matched posts" subtitle="Public Bluesky posts matched to an entity. Newest by author-claimed time.">
          <Async {...social} h={200} isEmpty={(d) => d.items.filter(isProd).length === 0} empty={{ title: "No persisted posts" }}>
            {(d) => <ul className="divide-y divide-slate-100">{d.items.filter(isProd).slice(0, 8).map((p) => <PostRow key={p.event_id} p={p} names={names} />)}</ul>}</Async></Card>
      </div>

      <Card title="Source health (compact)" subtitle="From recorded ingestion status — not from container health.">
        <Async {...src} h={60}>{(d) => <div className="flex flex-wrap gap-2">{d.map((s) => <span key={s.name} className="flex items-center gap-1.5 text-xs"><Pill label={s.state} /><span className="text-ink-700">{s.name}</span></span>)}</div>}</Async>
      </Card>
    </div>
  );
}

function WatchTable({ items, selected, onToggle, onOpen }: { items: WatchlistItem[]; selected: string[]; onToggle: (s: string) => void; onOpen: (id: string) => void }) {
  return <div className="overflow-x-auto"><table className="w-full text-sm">
    <thead className="text-left text-xs text-ink-500"><tr><th className="py-1 pr-2">Chart</th><th>Instrument</th><th>Mkt</th><th className="text-right">Last close</th><th className="text-right">Change</th><th>Bar date</th><th>Mode</th><th>Status</th></tr></thead>
    <tbody className="divide-y divide-slate-100">{items.map((i) => <tr key={i.symbol} className="align-middle">
      <td className="py-1.5 pr-2"><input type="checkbox" aria-label={`Chart ${i.symbol}`} disabled={i.status === "NO_DATA"} checked={selected.includes(i.symbol)} onChange={() => onToggle(i.symbol)} /></td>
      <td><button className="text-left font-medium text-brand-600 hover:underline" onClick={() => onOpen(i.entity_id)}>{i.display_name}</button><div className="text-xs text-ink-500">{i.symbol}</div></td>
      <td className="text-xs text-ink-500">{i.market}<br />{i.timezone}</td>
      <td className="text-right tabular-nums">{i.latest ? <>{fmtNum(i.latest.close)} <span className="text-xs text-ink-500">{i.currency}</span></> : "—"}</td>
      <td className="text-right tabular-nums" title={i.change_basis}><Change pct={i.change_pct} /></td>
      <td className="text-xs text-ink-500">{i.latest ? <>{fmtTime(i.latest.bar_time)}{i.latest.is_provisional && <span className="ml-1 rounded bg-amber-50 px-1 text-amber-800" title="Daily bar for the current session; close may still change">provisional</span>}</> : "—"}</td>
      <td>{i.latest ? <ModePill mode={i.latest.data_mode} /> : "—"}</td><td><Pill label={i.status} /></td></tr>)}</tbody></table></div>;
}

export function Seg({ value, options, onChange }: { value: string; options: string[]; onChange: (v: string) => void }) {
  return <div role="group" className="inline-flex overflow-hidden rounded border border-slate-300 text-xs">{options.map((o) =>
    <button key={o} aria-pressed={o === value} onClick={() => onChange(o)} className={`px-2.5 py-1 ${o === value ? "bg-brand-600 text-white" : "bg-white text-ink-700 hover:bg-slate-50"}`}>{o}</button>)}</div>;
}

export function Vol({ data, k, color }: { data: { t: string; social: number; news: number }[]; k: "social" | "news"; color: string }) {
  return <div className="h-52"><ResponsiveContainer><AreaChart data={data} margin={{ left: 0, right: 8 }}>
    <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" /><XAxis dataKey="t" tickFormatter={fmtHour} fontSize={11} minTickGap={40} /><YAxis fontSize={11} allowDecimals={false} width={36} />
    <Tooltip {...chartTooltip} labelFormatter={(l) => fmtTime(String(l))} /><Area dataKey={k} stroke={color} fill={color} fillOpacity={0.15} name={`${k} matches`} isAnimationActive={false} /></AreaChart></ResponsiveContainer></div>;
}
