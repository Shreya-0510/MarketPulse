import { useCallback, useEffect, useState } from "react";
import { Activity, BarChart3, Radio, Search, Workflow } from "lucide-react";
import { api } from "./api/client";
import { Banner, Pill } from "./components/ui";
import { useApi } from "./hooks/useApi";
import { useLiveFeed } from "./hooks/useLiveFeed";
import { useNow } from "./hooks/useNow";
import { ageSeconds, fmtAge, freshness } from "./lib/format";
import EntityDeepDive from "./pages/EntityDeepDive";
import Firehose from "./pages/Firehose";
import Overview from "./pages/Overview";
import Pipeline from "./pages/Pipeline";

const TABS = [
  { id: "overview", label: "Overview", icon: BarChart3 }, { id: "entity", label: "Entity Deep Dive", icon: Search },
  { id: "firehose", label: "Live Firehose", icon: Radio }, { id: "pipeline", label: "Pipeline & Methodology", icon: Workflow },
] as const;
type TabId = (typeof TABS)[number]["id"];

const parseHash = (): { tab: TabId; entity: string | null } => {
  const [, t, e] = location.hash.split("/");
  return { tab: (TABS.find((x) => x.id === t)?.id ?? "overview") as TabId, entity: e ? decodeURIComponent(e) : null };
};

export default function App() {
  const [route, setRoute] = useState(parseHash());
  const [fhEntity, setFhEntity] = useState("");
  const now = useNow(1000);
  const entities = useApi((s) => api.entities(s), []);
  const health = useApi((s) => api.health(s), [], 30000);
  const src = useApi((s) => api.sourcesStatus(s), [], 30000);
  // One WebSocket for the app lifetime; it is only mounted visually on the Firehose tab but keeps the "last event" state honest.
  const feed = useLiveFeed(fhEntity || undefined);

  useEffect(() => { const f = () => setRoute(parseHash()); window.addEventListener("hashchange", f); return () => window.removeEventListener("hashchange", f); }, []);
  const go = useCallback((tab: TabId, entity?: string | null) => { location.hash = `/${tab}${entity ? `/${encodeURIComponent(entity)}` : ""}`; }, []);
  const openEntity = useCallback((id: string) => go("entity", id), [go]);

  const lastOk = (src.data ?? []).map((s) => s.last_success_at).filter(Boolean).sort().pop() ?? null;
  const fresh = freshness(lastOk, 3600, now);
  const list = entities.data?.items ?? [];
  const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-3 px-4 py-3">
          <div className="flex items-center gap-2"><Activity className="h-5 w-5 text-brand-600" /><div><h1 className="text-base font-semibold leading-tight">MarketPulse</h1><p className="text-[11px] text-ink-500">Market intelligence &amp; sentiment analytics</p></div></div>
          <div className="flex flex-wrap items-center gap-3 text-xs text-ink-500">
            <span title="Browser clock">{new Date(now).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "medium" })} <span className="text-ink-300">({tz})</span></span>
            <Pill label={health.error ? "API unreachable" : health.data ? `API ok · Mongo ${health.data.mongodb === "ok" ? "ok" : "down"}` : "API …"} tone={health.error || (health.data && health.data.mongodb !== "ok") ? "FAILED" : health.data ? "OK" : undefined} />
            <span title="Most recent successful source fetch recorded by any adapter"><Pill label={`Data ${fresh}${lastOk ? ` · last success ${fmtAge(ageSeconds(lastOk, now))}` : ""}`} tone={fresh} /></span>
          </div>
        </div>
        <nav className="mx-auto flex max-w-7xl gap-1 overflow-x-auto px-4" aria-label="Workspaces">
          {TABS.map((t) => <button key={t.id} onClick={() => go(t.id, t.id === "entity" ? route.entity : null)} aria-current={route.tab === t.id ? "page" : undefined}
            className={`flex items-center gap-1.5 whitespace-nowrap border-b-2 px-3 py-2 text-sm ${route.tab === t.id ? "border-brand-600 font-medium text-brand-700" : "border-transparent text-ink-500 hover:text-ink-900"}`}><t.icon className="h-4 w-4" />{t.label}</button>)}
        </nav>
      </header>
      <main className="mx-auto max-w-7xl space-y-4 px-4 py-4">
        {health.error && <Banner tone="error"><b>Backend API unreachable.</b> {health.error}. Start the API (see README) or check VITE_API_BASE_URL / the dev proxy target. No data is shown until it responds.</Banner>}
        {entities.error && !entities.data ? <Banner tone="error">Could not load the entity list: {entities.error} <button className="underline" onClick={entities.reload}>Retry</button></Banner> : null}
        {route.tab === "overview" && <Overview entities={list} onOpenEntity={openEntity} nowTick={now} />}
        {route.tab === "entity" && <EntityDeepDive entities={list} entityId={route.entity} onSelect={openEntity} nowTick={now} />}
        {route.tab === "firehose" && <Firehose entities={list} feed={feed} nowTick={now} entityFilter={fhEntity} setEntityFilter={setFhEntity} />}
        {route.tab === "pipeline" && <Pipeline nowTick={now} />}
        <footer className="pb-6 pt-2 text-center text-[11px] text-ink-500">Descriptive analytics from public data. Not investment advice. Bluesky is one platform and not representative of all investors.</footer>
      </main>
    </div>
  );
}
