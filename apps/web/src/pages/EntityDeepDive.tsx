import { useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, ComposedChart, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api } from "../api/client";
import { HeadlineRow, PostRow } from "../components/Items";
import { Async, Banner, Card, Change, Empty, Hint, ModePill, Pill, SentimentBar, Skeleton, chartTooltip } from "../components/ui";
import { useApi } from "../hooks/useApi";
import { fmtCompact, fmtDay, fmtHour, fmtInt, fmtNum, fmtPct, fmtTime, fmtAge, ageSeconds } from "../lib/format";
import { isProd, periodReturn, sumSentiment } from "../lib/series";
import type { EntitySummary } from "../types/api";
import { Seg } from "./Overview";

const RANGES = { "1M": 30, "3M": 90, "6M": 180, "1Y": 365 } as const;
const ACT = { "24h": 24, "72h": 72, "7d": 168 } as const;

export default function EntityDeepDive({ entities, entityId, onSelect, nowTick }: { entities: EntitySummary[]; entityId: string | null; onSelect: (id: string) => void; nowTick: number }) {
  const [q, setQ] = useState("");
  const [range, setRange] = useState<keyof typeof RANGES>("3M");
  const [act, setAct] = useState<keyof typeof ACT>("72h");
  const ent = entities.find((e) => e.entity_id === entityId) ?? null;
  const names = useMemo(() => Object.fromEntries(entities.map((e) => [e.entity_id, e.display_name])), [entities]);
  const matches = entities.filter((e) => `${e.display_name} ${e.symbol} ${e.entity_id}`.toLowerCase().includes(q.toLowerCase()));
  const id = ent?.entity_id, sym = ent?.symbol;

  const wl = useApi((s) => api.watchlist(s), [], 60000);
  const item = wl.data?.items.find((i) => i.entity_id === id) ?? null;
  const hist = useApi((s) => (sym ? api.history(sym, { start: new Date(Date.now() - RANGES[range] * 86400000).toISOString() }, s) : Promise.resolve(null)), [sym, range]);
  const histYear = useApi((s) => (sym ? api.history(sym, { start: new Date(Date.now() - 400 * 86400000).toISOString() }, s) : Promise.resolve(null)), [sym]);
  const metrics = useApi((s) => (id ? api.entityMetrics(id, ACT[act], s) : Promise.resolve(null)), [id, act], 60000);
  const anomalies = useApi((s) => (id ? api.anomalies({ entity: id, limit: 20 }, s) : Promise.resolve(null)), [id], 60000);
  const cross = useApi((s) => (id ? api.crossSignals({ entity: id, window_minutes: 60, lookback_windows: ACT[act] }, s) : Promise.resolve(null)), [id, act], 60000);
  const news = useApi((s) => (id ? api.news({ entity: id, limit: 30 }, s) : Promise.resolve(null)), [id], 60000);
  const social = useApi((s) => (id ? api.social({ entity: id, limit: 30 }, s) : Promise.resolve(null)), [id], 30000);

  const cutoff = Date.now() - ACT[act] * 3600000;
  const windows = useMemo(() => (metrics.data?.windows ?? []).filter((w) => new Date(w.window_start).getTime() >= cutoff).sort((a, b) => a.window_start.localeCompare(b.window_start)), [metrics.data, cutoff]);
  const socSent = sumSentiment([windows], "social", 0), newsSent = sumSentiment([windows], "news", 0);
  const bars = useMemo(() => [...(hist.data?.bars ?? [])].sort((a, b) => a.event_time.localeCompare(b.event_time)), [hist.data]);

  return (
    <div className="space-y-4">
      <Card>
        <div className="flex flex-wrap items-end gap-3">
          <label className="text-xs text-ink-500">Search entity<input value={q} onChange={(e) => setQ(e.target.value)} placeholder="name or symbol" className="mt-1 block w-48 rounded border border-slate-300 px-2 py-1.5 text-sm text-ink-900" /></label>
          <label className="text-xs text-ink-500">Entity<select value={entityId ?? ""} onChange={(e) => onSelect(e.target.value)} className="mt-1 block w-64 rounded border border-slate-300 bg-white px-2 py-1.5 text-sm text-ink-900">
            <option value="" disabled>Select…</option>{matches.map((e) => <option key={e.entity_id} value={e.entity_id}>{e.display_name} ({e.symbol})</option>)}</select></label>
          <div><div className="mb-1 text-xs text-ink-500">Price range</div><Seg value={range} options={Object.keys(RANGES)} onChange={(v) => setRange(v as keyof typeof RANGES)} /></div>
          <div><div className="mb-1 text-xs text-ink-500">Activity window</div><Seg value={act} options={Object.keys(ACT)} onChange={(v) => setAct(v as keyof typeof ACT)} /></div>
        </div>
      </Card>
      {!ent ? <Empty title="Select an entity" hint="Choose a company or instrument above, or click one on the Overview watchlist." /> : <>
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div><h2 className="text-lg font-semibold">{ent.display_name}</h2>
              <p className="text-xs text-ink-500">{ent.symbol} · {ent.market} · {ent.instrument_type} · {ent.currency} · id <code>{ent.entity_id}</code></p></div>
            {item?.latest ? <div className="text-right"><div className="text-2xl font-semibold tabular-nums">{fmtNum(item.latest.close)} <span className="text-sm font-normal text-ink-500">{item.currency}</span></div>
              <div className="text-sm"><Change pct={item.change_pct} /> <span className="text-xs text-ink-500" title={item.change_basis}>({item.change_abs !== null ? fmtNum(item.change_abs) : "—"} vs prev. close {fmtNum(item.previous_close)})</span></div>
              <div className="mt-1 flex items-center justify-end gap-2 text-xs text-ink-500">bar {fmtTime(item.latest.bar_time)} <ModePill mode={item.latest.data_mode} /><Pill label={item.status} />{item.latest.is_provisional && <span className="rounded bg-amber-50 px-1 text-amber-800">provisional</span>}</div></div>
              : <Pill label={wl.error ? "ERROR" : "NO_DATA"} />}
          </div>
          {item?.status === "STALE" && <div className="mt-3"><Banner tone="warn">Market data is stale ({item.data_age_hours !== null ? `${fmtNum(item.data_age_hours, 0)}h old` : "age unknown"}).</Banner></div>}
        </Card>

        <div className="grid gap-4 lg:grid-cols-2">
          <Card title="Price history (daily close)" subtitle={`${ent.currency}; ${hist.data ? `${hist.data.count} of ${hist.data.total_in_range} persisted bars` : ""}`}>
            <Async {...hist} h={240} isEmpty={(d) => !d || d.bars.length === 0} empty={{ title: "No persisted bars in this range", hint: hist.data?.note ?? undefined }}>
              {() => <div className="h-60"><ResponsiveContainer><LineChart data={bars} margin={{ left: 0, right: 8 }}><CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                <XAxis dataKey="event_time" tickFormatter={fmtDay} fontSize={11} minTickGap={30} /><YAxis fontSize={11} domain={["auto", "auto"]} width={52} label={{ value: ent.currency, angle: -90, fontSize: 11, position: "insideLeft" }} />
                <Tooltip {...chartTooltip} labelFormatter={(l) => fmtTime(String(l))} formatter={(v: number) => fmtNum(v)} /><Line dataKey="close" name="close" stroke="#1d4ed8" dot={false} strokeWidth={2} isAnimationActive={false} /></LineChart></ResponsiveContainer></div>}</Async></Card>
          <Card title="Volume" subtitle="Shares traded per daily bar (missing volume is left blank, e.g. indices).">
            <Async {...hist} h={240} isEmpty={() => !bars.some((b) => b.volume !== null)} empty={{ title: "No volume data for this instrument/range" }}>
              {() => <div className="h-60"><ResponsiveContainer><BarChart data={bars}><CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" /><XAxis dataKey="event_time" tickFormatter={fmtDay} fontSize={11} minTickGap={30} />
                <YAxis fontSize={11} tickFormatter={fmtCompact} width={48} /><Tooltip {...chartTooltip} labelFormatter={(l) => fmtTime(String(l))} formatter={(v: number) => fmtInt(v)} /><Bar dataKey="volume" fill="#64748b" isAnimationActive={false} /></BarChart></ResponsiveContainer></div>}</Async></Card>
        </div>

        <Card title="Relative performance" subtitle={<>Simple return: (latest close − close at/before N days earlier) / that close. Computed in the browser from persisted daily bars <Hint text="No backend endpoint provides period returns yet; this is a presentation calculation on /market/{symbol}/history data, with the reference bar shown." /></>}>
          <Async {...histYear} h={60}>{(d) => d && <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            {[7, 30, 90, 365].map((days) => { const r = periodReturn(d.bars, days); return <div key={days} className="rounded border border-slate-100 p-2"><div className="text-xs text-ink-500">{days === 365 ? "1Y" : days === 7 ? "1W" : days === 30 ? "1M" : "3M"}</div>
              <div className="text-lg font-semibold tabular-nums">{r.pct === null ? <span className="text-ink-300">n/a</span> : <Change pct={r.pct} />}</div>
              <div className="text-[11px] text-ink-500">{r.pct === null ? `insufficient history (${r.n} bars)` : `from ${fmtDay(r.from!)} · ${r.n} bars`}</div></div>; })}</div>}</Async></Card>

        <div className="grid gap-4 lg:grid-cols-2">
          <Card title="News & social activity" subtitle={`Matches per ${windows[0]?.window_minutes ?? 60}-min window (analytics_job). Windows with no matches may not exist and are not drawn as zero.`}>
            <Async {...metrics} h={220} isEmpty={() => windows.length === 0} empty={{ title: "No computed windows in range", hint: metrics.data?.note ?? "analytics_job has not produced windows for this entity yet." }}>
              {() => <div className="h-56"><ResponsiveContainer><ComposedChart data={windows}><CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" /><XAxis dataKey="window_start" tickFormatter={fmtHour} fontSize={11} minTickGap={40} />
                <YAxis fontSize={11} allowDecimals={false} width={32} /><Tooltip {...chartTooltip} labelFormatter={(l) => fmtTime(String(l))} /><Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="social.count" name="social" fill="#1d4ed8" isAnimationActive={false} /><Bar dataKey="news.count" name="news" fill="#0f766e" isAnimationActive={false} /></ComposedChart></ResponsiveContainer></div>}</Async></Card>
          <Card title="Average sentiment trend" subtitle="Mean VADER compound per window (−1…+1). Gaps = no scored items. Social and news are separate series.">
            <Async {...metrics} h={220} isEmpty={() => !windows.some((w) => w.social.avg_score !== null || w.news.avg_score !== null)} empty={{ title: "No scored items in range" }}>
              {() => <div className="h-56"><ResponsiveContainer><LineChart data={windows}><CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" /><XAxis dataKey="window_start" tickFormatter={fmtHour} fontSize={11} minTickGap={40} />
                <YAxis domain={[-1, 1]} fontSize={11} width={32} /><Tooltip {...chartTooltip} labelFormatter={(l) => fmtTime(String(l))} formatter={(v: number) => fmtNum(v, 3)} /><Legend wrapperStyle={{ fontSize: 12 }} />
                <Line dataKey="social.avg_score" name="social" stroke="#1d4ed8" connectNulls={false} dot={{ r: 2 }} isAnimationActive={false} /><Line dataKey="news.avg_score" name="news" stroke="#0f766e" connectNulls={false} dot={{ r: 2 }} isAnimationActive={false} /></LineChart></ResponsiveContainer></div>}</Async></Card>
          <Card title="Social sentiment distribution"><Async {...metrics} h={50}>{() => <SentimentBar {...socSent} total={socSent.total} />}</Async></Card>
          <Card title="News sentiment distribution"><Async {...metrics} h={50}>{() => <SentimentBar {...newsSent} total={newsSent.total} />}</Async></Card>
        </div>

        <div className="grid gap-4 lg:grid-cols-2">
          <Card title="Attention anomalies" subtitle="Explained: observed count vs rolling-baseline mean ± std; z = (observed − mean) / std.">
            <Async {...anomalies} h={120} isEmpty={(d) => !d || d.items.length === 0} empty={{ title: "No anomalies for this entity", hint: "Requires ≥24 historical windows and a minimum count; absence ≠ calm." }}>
              {(d) => <ul className="divide-y divide-slate-100 text-sm">{d!.items.map((a) => <li key={a.anomaly_id} className="py-2"><b className={a.direction === "spike" ? "text-amber-700" : "text-ink-700"}>{a.source_type} {a.direction}</b> · z={fmtNum(a.z_score, 2)} (threshold {a.z_threshold})
                <p className="text-xs text-ink-500">{fmtTime(a.window_start)} · {a.window_minutes}-min window · observed {a.observed_count} vs {fmtNum(a.baseline_mean, 2)} ± {fmtNum(a.baseline_std, 2)} across {a.baseline_windows_used} windows</p></li>)}</ul>}</Async></Card>
          <Card title="Cross-signal observations" subtitle="Descriptive only — no causation, no prediction. Counts show how much data supports each row.">
            <Async {...cross} h={120} isEmpty={(d) => !d || d.items.length === 0} empty={{ title: "No cross-signal windows", hint: cross.data?.note ?? undefined }}>
              {(d) => { const rows = d!.items.filter((i) => i.observations.length > 0); return rows.length === 0 ? <Empty title="No divergences observed" hint={`${d!.items.length} windows evaluated; none met an observation rule.`} /> :
                <ul className="divide-y divide-slate-100 text-sm">{rows.slice(0, 12).map((c) => <li key={c.window_start} className="py-2">
                  <div className="text-xs text-ink-500">{fmtTime(c.window_start)} · {c.window_minutes} min</div>
                  <div>{c.observations.map((o) => <span key={o} className="mr-1 inline-block rounded bg-slate-100 px-1.5 py-0.5 text-xs">{o.replace(/_/g, " ")}</span>)}</div>
                  <div className="text-xs text-ink-500">price Δ {fmtPct(c.price_change_pct)} ({c.price_bar_count} bars) · social n={c.social_count} (scored {c.social_scored_count}, avg {fmtNum(c.avg_social_sentiment, 2)}) · news n={c.news_count} (scored {c.news_scored_count}, avg {fmtNum(c.avg_news_sentiment, 2)})</div></li>)}</ul>; }}</Async></Card>
        </div>

        <div className="grid gap-4 lg:grid-cols-2">
          <Card title="Recent headlines"><Async {...news} h={160} isEmpty={(d) => !d || d.items.filter(isProd).length === 0} empty={{ title: "No matched articles" }}>
            {(d) => <ul className="divide-y divide-slate-100">{d!.items.filter(isProd).slice(0, 10).map((a) => <HeadlineRow key={a.event_id} a={a} names={names} />)}</ul>}</Async></Card>
          <Card title="Recent posts"><Async {...social} h={160} isEmpty={(d) => !d || d.items.filter(isProd).length === 0} empty={{ title: "No matched posts" }}>
            {(d) => <ul className="divide-y divide-slate-100">{d!.items.filter(isProd).slice(0, 10).map((p) => <PostRow key={p.event_id} p={p} names={names} />)}</ul>}</Async></Card>
        </div>

        <Card title="Source breakdown & freshness" subtitle="Based on the most recent ≤30 persisted items per type loaded above (a sample, not the full history).">
          {!news.data && !social.data ? <Skeleton h={60} /> : <SourceBreakdown news={(news.data?.items ?? []).filter(isProd)} social={(social.data?.items ?? []).filter(isProd)} nowTick={nowTick} total={{ news: news.data?.total, social: social.data?.total }} />}
        </Card>
        <p className="text-xs text-ink-500">Observations here are descriptive and do not imply causation or predict returns. Not investment advice.</p>
      </>}
    </div>
  );
}

function SourceBreakdown({ news, social, nowTick, total }: { news: { publisher: string; source: string; ingested_at: string }[]; social: { source: string; ingested_at: string }[]; nowTick: number; total: { news?: number; social?: number } }) {
  const group = (xs: { k: string; t: string }[]) => { const m = new Map<string, { n: number; last: string }>(); for (const x of xs) { const g = m.get(x.k) ?? { n: 0, last: x.t }; g.n++; if (x.t > g.last) g.last = x.t; m.set(x.k, g); } return [...m.entries()].sort((a, b) => b[1].n - a[1].n); };
  const nRows = group(news.map((n) => ({ k: n.publisher, t: n.ingested_at }))), sRows = group(social.map((s) => ({ k: s.source, t: s.ingested_at })));
  const T = ({ title, rows, tot }: { title: string; rows: [string, { n: number; last: string }][]; tot?: number }) => <div><h3 className="mb-1 text-xs font-semibold text-ink-700">{title} <span className="font-normal text-ink-500">(total persisted matches: {fmtInt(tot)})</span></h3>
    {rows.length === 0 ? <p className="text-xs text-ink-500">none</p> : <table className="w-full text-xs"><tbody>{rows.map(([k, v]) => <tr key={k}><td className="py-0.5">{k}</td><td className="text-right tabular-nums">{v.n}</td><td className="pl-3 text-right text-ink-500">last ingested {fmtAge(ageSeconds(v.last, nowTick))}</td></tr>)}</tbody></table>}</div>;
  return <div className="grid gap-4 sm:grid-cols-2"><T title="News by publisher" rows={nRows} tot={total.news} /><T title="Social by source" rows={sRows} tot={total.social} /></div>;
}

