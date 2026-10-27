# BUILD.md — MarketPulse cross-account engineering memory
Status words: NOT STARTED / IN PROGRESS / IMPLEMENTED / LOCALLY VERIFIED / BLOCKED.
**IMPLEMENTED = code written and py_compile'd (plus YAML parse). LOCALLY VERIFIED = pytest actually executed in sandbox and passed.**
Slices 1–4: IMPLEMENTED (not verified by live stack). Slice 5: IMPLEMENTED; 87 pytest tests pass (executed in sandbox in the Slice 5 session). Slice 6: IMPLEMENTED. **Slice 7: IMPLEMENTED (integration fixes + verification tooling). The full stack has NEVER been started, and the frontend never installed/built, in any session so far — no network/Docker in the authoring sandbox.** See section E and H for what the user must run.

## A. Project snapshot
- Purpose: BDSMA college project. yfinance / Bluesky / RSS → Kafka → Spark Structured Streaming → MongoDB → FastAPI → React. Local Docker, free/open-source, no fabricated data.
- Phase: **All 7 slices IMPLEMENTED. Project is code-complete but not yet verified end-to-end on a real machine.** The remaining work is the user's local run (section H) and fixing whatever it reveals.
- What Slice 5 adds: VADER sentiment filled in Spark foreachBatch (social title/text); `analytics_job.py` background process computes entity_metrics + attention_metrics + anomaly detection + cross-signal descriptors every `ANALYTICS_INTERVAL_SECONDS`; 6 new FastAPI endpoints + WebSocket.
- data_mode="test" records excluded from all production analytics at the query level (PRODUCTION_MODES constant). **S7:** REST `/social` and `/news` list endpoints now also exclude them by default (`include_test=true` opts in; used only by `scripts/smoke_text.py`).
- Missing sentiment = null (never replaced with "neutral").
- Slice 6 adds the React dashboard (`apps/web`) with 4 workspaces. Not done: full integration/polish (Slice 7). MASTER_SPEC.md was not in the supplied zip (spec was provided in the prompt only); it is not recreated here.
- Blockers: none known. Same first-run risks as Slices 1–4 (jar compatibility, yfinance throttling, feed availability).

## B. Architecture
```
yfinance ─► ingestion-market ─► Kafka market.raw ─► spark-streaming ─┐
Bluesky Jetstream ─► ingestion-social ─► social.raw ─► spark-text ────┼─► MongoDB ─► API :8000
RSS/Atom feeds ─► ingestion-news ─► news.raw ─► spark-text ──────────┘         ▲
                                                                                 │
analytics-job (no-Spark; polls MongoDB) ─────────────────────────────────────────┘
  writes: entity_metrics, attention_metrics, anomaly_events
  reads: market_prices, social_posts, news_articles
```
| Service (compose name) | Image / build | Status |
|---|---|---|
| `kafka` | apache/kafka:3.9.0 (KRaft) | IMPLEMENTED |
| `kafka-init` | same image; creates 7 topics | IMPLEMENTED |
| `mongodb` | mongo:7.0, standalone | IMPLEMENTED |
| `mongo-init` | marketpulse-api:dev; runs mongo_schema | IMPLEMENTED |
| `api` | infra/docker/api.Dockerfile | IMPLEMENTED |
| `ingestion-market` | ingestion.Dockerfile | IMPLEMENTED |
| `spark-streaming` | streaming.Dockerfile (market job) | IMPLEMENTED |
| `ingestion-social` | ingestion.Dockerfile | IMPLEMENTED |
| `ingestion-news` | ingestion.Dockerfile | IMPLEMENTED |
| `spark-text` | streaming.Dockerfile; `mp_streaming.text_jobs` (now with VADER sentiment) | IMPLEMENTED |
| `analytics-job` | streaming.Dockerfile; `mp_streaming.analytics_job` (NEW S5) | IMPLEMENTED |
| `web` | infra/docker/web.Dockerfile (node build → nginx:1.27-alpine; serves SPA on 8080, proxies /api incl. WebSocket to `api:8000`) — NEW S6 | IMPLEMENTED (image never built) |
Host ports (127.0.0.1 only): Kafka external 29092, Mongo 27017, API 8000, Web 3000 (`WEB_PORT`; maps to container 8080). Vite dev server: 5173.
Spark runs local[2]. analytics-job is pure Python (no Spark, no JVM).

## C. Repository manifest
| Path | Responsibility | Status |
|---|---|---|
| `BUILD.md README.md .env.example .gitignore pyproject.toml` | docs/config (pyproject now includes `analytics` extra + vaderSentiment) | IMPLEMENTED |
| `config/{watchlist,entities,sources,settings}.yaml` | instruments, matching, sources, topics/thresholds | IMPLEMENTED |
| `services/common/marketpulse_common/{ids,events,config,mongo_schema,matching}.py` | shared contracts | IMPLEMENTED |
| `services/common/tests/*` | unit tests | IMPLEMENTED, not run in live stack |
| `services/ingestion/mp_ingestion/{market,social,news,kafka_out,status,run_*,inject_bad}.py` | ingestion adapters | IMPLEMENTED |
| `services/ingestion/tests/*` | unit tests | IMPLEMENTED, not run in live stack |
| `services/streaming/mp_streaming/{transforms,sinks,market_job}.py` | Spark market job | IMPLEMENTED |
| `services/streaming/mp_streaming/{transforms_text,sinks_text,job_runner}.py` | Spark text job utilities | IMPLEMENTED |
| `services/streaming/mp_streaming/text_jobs.py` | MODIFIED S5: VADER sentiment now applied inside foreachBatch | IMPLEMENTED |
| `services/streaming/mp_streaming/sentiment.py` | NEW S5: VADER wrapper; (None,None) for empty/missing text | IMPLEMENTED; LOCALLY VERIFIED |
| `services/streaming/mp_streaming/analytics_job.py` | NEW S5: entity_metrics, attention_metrics, anomaly detection, cross-signals, main loop | IMPLEMENTED; LOCALLY VERIFIED |
| `services/streaming/tests/test_analytics.py` | NEW S5: 23 unit tests for sentiment/anomaly/cross-signal/exclusion | IMPLEMENTED; **LOCALLY VERIFIED: 23/23 passed** |
| `services/streaming/tests/{test_sinks,test_transforms_spark,test_sinks_text,test_transforms_text_spark}.py` | Spark transform tests (need Java) | IMPLEMENTED, not run |
| `services/api/app/schemas.py` | MODIFIED S5: added EntityMetrics, AttentionMetric, AnomalyEvent, CrossSignal, WSMessage etc. | IMPLEMENTED |
| `services/api/app/main.py` | MODIFIED S5: added analytics.router; version 0.5.0 | IMPLEMENTED |
| `services/api/app/deps.py` | MODIFIED S5: added get_analytics_repo() | IMPLEMENTED |
| `services/api/app/routes/analytics.py` | NEW S5: /entities, /entities/{id}/metrics, /attention, /anomalies, /cross-signals, WS /ws | IMPLEMENTED; LOCALLY VERIFIED |
| `services/api/app/repositories/analytics.py` | NEW S5: MongoAnalyticsRepo (entity_metrics, attention_metrics, anomaly_events reads) | IMPLEMENTED |
| `services/api/app/routes/{market,ops,text}.py` | unchanged S1-S4 | IMPLEMENTED |
| `services/api/tests/test_analytics_routes.py` | NEW S5: 20 API route tests (fake repo + fake config) | IMPLEMENTED; **LOCALLY VERIFIED: 20/20 passed** |
| `services/api/tests/test_text_routes.py` | FIXED S4 bug: duplicate `publisher` kwarg in `new()` helper | FIXED; LOCALLY VERIFIED |
| `services/api/tests/{test_health,test_market_routes}.py` | API tests | IMPLEMENTED; LOCALLY VERIFIED: still pass |
| `infra/compose.yaml` | MODIFIED S5: added `analytics-job` service | IMPLEMENTED |
| `infra/docker/streaming.Dockerfile` | MODIFIED S5: added `vaderSentiment==3.3.2` to pip install | IMPLEMENTED |
| `apps/web/package.json, tsconfig.json, vite.config.ts, tailwind.config.js, postcss.config.js, index.html, .env.example, .gitignore, nginx.conf` | NEW S6 frontend config (Vite 5, React 18.3, TS 5.6, Tailwind 3.4, Recharts 2.15, lucide-react, vitest). Dev proxy `/api`,`/health`,WS → `VITE_DEV_PROXY_TARGET` | IMPLEMENTED; deps never installed |
| `apps/web/src/main.tsx, App.tsx, index.css, vite-env.d.ts` | NEW S6 shell: hash routing (`#/overview`, `#/entity/<id>`, `#/firehose`, `#/pipeline`), header clock + API/data-freshness pills, single app-lifetime WebSocket | IMPLEMENTED |
| `apps/web/src/types/api.ts` | NEW S6 TS mirror of `services/api/app/schemas.py` | IMPLEMENTED |
| `apps/web/src/api/client.ts` | NEW S6 only module that calls fetch; `api.*` per endpoint, `ApiError`, `buildQuery`, `wsUrl` | IMPLEMENTED |
| `apps/web/src/hooks/{useApi,useLiveFeed,useNow,useAllMetrics}.ts` | NEW S6 fetch+poll+abort; bounded WS w/ backoff reconnect; ticking clock; per-entity metrics fan-out | IMPLEMENTED |
| `apps/web/src/lib/{format,feed,series}.ts` | NEW S6 formatting/freshness (missing → "—"/UNKNOWN), feed dedupe/bound/origin labelling, chart-series helpers | IMPLEMENTED; logic tests executed via stand-in harness |
| `apps/web/src/lib/format.test.ts` | NEW S6 10 vitest-style tests | IMPLEMENTED; 10/10 passed under a stand-in harness (NOT real vitest) |
| `apps/web/src/components/{ui,Items}.tsx` | NEW S6 Card/Async/Empty/Error/Banner/Pill/SentimentBar etc.; headline & post rows | IMPLEMENTED |
| `apps/web/src/pages/{Overview,EntityDeepDive,Firehose,Pipeline}.tsx` | NEW S6 the four workspaces | IMPLEMENTED |
| `infra/docker/web.Dockerfile`, `.dockerignore` | NEW S6 web image; root dockerignore (node_modules, dist, .venv, .git) | IMPLEMENTED |
| `services/api/app/main.py` | MODIFIED S6: optional `CORS_ORIGINS` env → CORSMiddleware (GET only); default disabled | IMPLEMENTED; py_compile OK |
| `infra/compose.yaml` | MODIFIED S6: `web` service; `api` gets `CORS_ORIGINS` | IMPLEMENTED; YAML parses |
| `services/api/app/serialization.py` | NEW S7 stdlib-only `json_default`: datetimes → ISO-8601 UTC `...Z` for WebSocket JSON | IMPLEMENTED; LOCALLY VERIFIED (3 tests run directly) |
| `services/api/app/routes/analytics.py` | MODIFIED S7: WS `json.dumps(default=json_default)` (was `str`, producing `2026-01-01 10:00:00+00:00`, non-ISO) | IMPLEMENTED; py_compile only |
| `services/api/app/routes/text.py`, `repositories/text.py` | MODIFIED S7: `include_test` query param (default false) → Mongo filter `data_mode != test` | IMPLEMENTED; py_compile only |
| `services/api/tests/test_serialization.py` | NEW S7 | LOCALLY VERIFIED (functions executed directly; pytest absent) |
| `services/api/tests/test_text_routes.py` | MODIFIED S7: FakeRepo mirrors include_test; +1 test | IMPLEMENTED; **NOT RUN** (FastAPI unavailable offline) |
| `scripts/verify_stack.py` | NEW S7 read-only checker for a running stack (API + web proxy + WS handshake), stdlib only | IMPLEMENTED; script logic exercised against a fake stdlib server only |
| `scripts/smoke_text.py` | MODIFIED S7: list queries pass `include_test=true` | IMPLEMENTED; py_compile only |
| `apps/web/vite.config.ts`, `src/pages/Pipeline.tsx` | MODIFIED S7: `defineConfig` from `vitest/config` (types the `test` key); `ReactNode` import | IMPLEMENTED; never type-checked |
| `infra/compose.yaml` | S7: `web` healthcheck (wget `/health` through nginx → api) | IMPLEMENTED; YAML parses |
| `scripts/{up.ps1,validate_config.py,smoke_market.py,smoke_text.py}` | helpers | IMPLEMENTED |

## D. Integration contracts (as coded — S5 additions)

**Sentiment (S5)**:
- Model: VADER (`vaderSentiment==3.3.2`), compound score.
- `positive_threshold` = 0.05, `negative_threshold` = -0.05 (from `settings.yaml`).
- `sentiment_label`: "positive" | "neutral" | "negative" — set after scoring; null when text is empty/missing or VADER unavailable.
- `sentiment_score`: float ∈ [-1.0, 1.0], rounded to 6dp; null when label is null.
- Scored in two places: (a) Spark `text_jobs.py` foreachBatch (for newly arriving social/news); (b) `analytics_job._score_batch` (backfill pass for older un-scored docs in production collections, up to 500 per cycle).
- Social scoring field: `text` (post body); news scoring field: `title`.
- data_mode="test" records are NOT excluded from scoring (they're stored), but are excluded from analytics aggregation via `PRODUCTION_MODES` filter.

**PRODUCTION_MODES** (used in all analytics queries): `{"historical", "polled", "live", "replay"}` — never "test".

**analytics_job** (`mp_streaming.analytics_job`, `python -m mp_streaming.analytics_job`):
- No Spark dependency (pure Python + pymongo + vaderSentiment).
- Runs in an infinite loop every `ANALYTICS_INTERVAL_SECONDS` (default 300).
- One cycle: score_batch → entity_metrics → attention_metrics → anomaly_detection → cross_signals (via entity_metrics).
- `_window_bounds(now, window_minutes)`: returns the most recent fully-elapsed window `[start, end)` aligned to epoch multiples of `window_minutes`.
- Entity metrics: social + news counts, scored counts, pos/neu/neg counts, avg VADER compound per window. Upserted on (entity_id, metric, window_start, window_minutes).
- Attention metrics: raw count per (entity_id, source_type ∈ {social, news, market}) per window. Upserted on (entity_id, source_type, window_start, window_minutes).
- Anomaly detection: z-score vs. baseline (last `baseline_windows` historical attention windows). Requirements: current_count ≥ min_count AND history_count ≥ baseline_windows AND baseline_std ≥ min_std AND |z| ≥ z_threshold. anomaly_id = "ano_" + sha256(entity|source_type|window_start)[:28].
- Cross-signal descriptors: read from entity_metrics + market_prices; observations list (price_up_sentiment_down, price_down_sentiment_up, news_spike_price_stable, social_attention_without_news). Never implies causation.
- All anomalies/metrics upserted idempotently; re-running a cycle is safe.

**New MongoDB collections (S5 writes)**:
- `entity_metrics`: unique key (entity_id, metric, window_start, window_minutes). Fields: entity_id, metric="combined", window_start/end (datetime), window_minutes, social{count,scored,positive,neutral,negative,avg_score}, news{...}, total_mentions, computed_at.
- `attention_metrics`: unique key (entity_id, source_type, window_start, window_minutes). Fields: entity_id, source_type, window_start/end, window_minutes, count, computed_at.
- `anomaly_events`: unique key anomaly_id. Fields: anomaly_id, entity_id, source_type, window_start/end, window_minutes, observed_count, baseline_mean/std, baseline_windows_used, z_score, z_threshold, direction (spike|drop), detected_at.

**New API endpoints (S5)**:
- `GET /api/v1/entities` → `EntitiesResponse{count, items[EntitySummary]}`. All configured instruments.
- `GET /api/v1/entities/{entity_id}` → `EntitySummary` or 404.
- `GET /api/v1/entities/{entity_id}/metrics?limit=48` → `EntityMetricsResponse`. Reads `entity_metrics`. NO_DATA until analytics_job has run.
- `GET /api/v1/attention?entity=&source_type=&start=&end=&limit=200` → `AttentionResponse`. Reads `attention_metrics`. source_type must be social|news|market.
- `GET /api/v1/anomalies?entity=&source_type=&start=&end=&limit=100` → `AnomalyResponse`. Reads `anomaly_events`.
- `GET /api/v1/cross-signals?entity=&window_minutes=60&lookback_windows=24` → `CrossSignalResponse`. Computed live from entity_metrics + market_prices. window_minutes ∈ [15, 1440].
- `WS /api/v1/ws?entity=&source=` → bounded WebSocket. On connect: up to 50 recent social + 50 news + 20 anomalies. Then polls Mongo every 3s for newer events. Pings every 20s. Messages: `{type: social_post|news_article|anomaly|ping, payload: {...}, sent_at: ISO8601}`. data_mode="test" excluded. Reconnect is the client's responsibility on disconnect.

**Config settings.yaml** (unchanged; used by S5):
- `sentiment.model: vader`, `positive_threshold: 0.05`, `negative_threshold: -0.05`
- `anomaly.baseline_windows: 24`, `window_minutes: 60`, `z_threshold: 3.0`, `min_count: 5`, `min_std: 0.5`

**New env vars** (S5):
- `ANALYTICS_INTERVAL_SECONDS` (default 300): how often analytics_job runs a cycle.

All previously documented D contracts (Slices 1-4) remain unchanged.

## D6. Slice 6 frontend contracts (as coded)

**Communication**: browser → same-origin `/api/v1/*`, `/health`, WS `/api/v1/ws`. Dev: Vite proxy (`apps/web/vite.config.ts`, target `VITE_DEV_PROXY_TARGET`, default http://localhost:8000). Docker: nginx (`apps/web/nginx.conf`) proxies `/api/` (with Upgrade headers) and `/health` to `api:8000`. Cross-origin alternative: set `VITE_API_BASE_URL` (+ optional `VITE_WS_BASE_URL`) and API `CORS_ORIGINS`. `VITE_*` are build-time; in Docker they are compose build args.

**Endpoints consumed**: /health; /market/watchlist; /market/{symbol}/history (interval=1d, limit≤500, start); /entities; /entities/{id}/metrics?limit; /attention (source_type, start, limit≤1000); /anomalies; /cross-signals; /news; /social; /pipeline/status; /sources/status; WS /ws?entity=. Not consumed: /trace/{event_id}, /social/{id}, /news/{id}.

**Behavioural rules in the UI**
- Missing values render "—"/UNKNOWN/"unscored" — never 0 or "neutral". Windows that do not exist are not zero-filled.
- `data_mode=test` rows: since S7 the REST /news and /social exclude them server-side by default (the UI's client-side `isProd` filter is now belt-and-braces); analytics endpoints and WS always exclude them. Overview/Entity sentiment & activity come from `/entities/{id}/metrics` (production modes only).
- Overview fans out one `/entities/{id}/metrics` call per entity (no cross-entity endpoint exists); per-entity failures are listed in a banner.
- Activity totals sum per-entity counts: an item matching 2 entities counts twice (labelled).
- "Indexed to 100" chart and Entity "Relative performance" (1W/1M/3M/1Y) are display calculations in the browser over `/market/{symbol}/history` bars (reference bar shown; n/a when no bar at/before target). No backend endpoint for these yet.
- Staleness: header "Data FRESH/STALE/UNKNOWN" = newest `last_success_at` across sources, stale after 1h; watchlist STALE comes from API (`STALE_AFTER_DAYS`); Firehose STREAM STALE = no WS message (incl. 20s pings) for >120s; source row "status stale" = `updated_at` older than 1h.
- Polling: watchlist/anomalies/news 60s, social 30s, sources/health 30s, pipeline page 15s; paused while tab hidden; AbortController cleanup on unmount/dep change.
- WebSocket (`useLiveFeed`): reconnect backoff 1,2,4,…30s; held items capped at 300 (oldest dropped), deduped by `s:/n:/a:`+id. Pause freezes display only; incoming items are buffered (≤300) and merged on resume. **Snapshot vs live**: the server replays recent persisted items on connect with no marker, so an item is "newly ingested" only if its `ingested_at` ≥ the server `sent_at` of the first message of the session (`detected_at` for anomalies); otherwise "snapshot". Server clocks only. An item ingested between connect and the history query could be mislabelled snapshot.
- WS datetimes are ISO-8601 UTC with `Z` since S7 (`serialization.json_default`).
- Server WS types handled: social_post, news_article, anomaly, ping, error. `source_status` is documented but never sent by the server → not handled.

## E. Slice tracker
- **Slice 1 Foundation — IMPLEMENTED (not verified by live stack).**
- **Slice 2 Market path — IMPLEMENTED (not verified by live stack).**
- **Slice 3 Social (Bluesky) — IMPLEMENTED (not verified by live stack).**
- **Slice 4 News (RSS/Atom) — IMPLEMENTED (not verified by live stack).** Bug fixed in S5: `test_text_routes.py` `new()` helper had duplicate `publisher` kwarg.
- **Slice 5 Analytics — IMPLEMENTED; LOCALLY VERIFIED (pytest 87/87 passed in sandbox).**
  - Scope: VADER sentiment, entity/attention/anomaly metrics, cross-signal descriptors, 6 new API endpoints + WebSocket.
  - Files created: `sentiment.py`, `analytics_job.py`, `routes/analytics.py`, `repositories/analytics.py`, `tests/test_analytics.py`, `tests/test_analytics_routes.py`.
  - Files modified: `text_jobs.py` (sentiment in foreachBatch), `schemas.py` (S5 models), `main.py` (analytics router), `deps.py` (get_analytics_repo), `streaming.Dockerfile` (vaderSentiment), `compose.yaml` (analytics-job service), `pyproject.toml` (analytics extra), `.env.example` (ANALYTICS_INTERVAL_SECONDS), `test_text_routes.py` (pre-existing bug fix).
  - Tests executed (py_compile + pytest): `test_analytics.py` 23/23; `test_analytics_routes.py` 20/20; existing tests 44/44. Total: **87/87 passed**.
  - Tests NOT run (need Java/live stack): `test_transforms_spark.py`, `test_transforms_text_spark.py`, `test_sinks.py`, `test_sinks_text.py`, `test_kafka_out_and_run.py`, `test_news.py`, `test_social.py`.
  - Known limitations: anomaly detection requires ≥24 historical windows (24 hours at 60-min default) to produce first anomalies; sentiment back-fill processes 500 un-scored docs per cycle (staggered); VADER scores `title` for news (not full article body — unavailable per design); WebSocket uses polling not Change Streams (consistent with no replica-set requirement).
- **Slice 6 React dashboard — IMPLEMENTED (NOT LOCALLY VERIFIED).**
  - Scope: four workspaces (Overview, Entity Deep Dive, Live Firehose, Pipeline & Methodology), API layer, bounded WebSocket, loading/empty/error/stale states, nginx+Docker wiring, optional API CORS.
  - Files: see section C (`apps/web/**`, `infra/docker/web.Dockerfile`, `.dockerignore`; modified `main.py`, `compose.yaml`, `.env.example`, README, BUILD.md).
  - **Checks actually executed (sandbox, no network)**: (1) `tsc` syntax-level parse of all src files with `--ignoreConfig`: **0 syntax errors (no TS1xxx)**; remaining diagnostics were only missing-module/JSX-`any` cascades because `react`, `recharts`, `lucide-react`, `vite` types were not installed — so **this is NOT a real type check**. (2) Heuristic unused-import scan: none found. (3) `lib/format.test.ts` logic (10 tests: formatting, freshness, query building, feed dedupe/bound/snapshot-vs-live, index rebasing, period returns, activity/sentiment sums) transpiled and run under a hand-written minimal describe/it/expect harness: **10 passed, 0 failed**. This is not vitest. (4) `py_compile services/api/app/main.py` OK; `compose.yaml` parses and lists the `web` service.
  - **Checks NOT run**: `npm install`, `npm run typecheck`, `npm run build`, real `vitest`, Docker image build of `web`, any run against the live FastAPI/Mongo/Kafka/Spark stack, browser rendering of any page (layout, Recharts rendering, responsive behaviour), WebSocket against the real server, backend pytest suites (deliberately not re-run), the API CORS change under test.
  - Known gaps/risks: no package-lock.json (npm network was unavailable; first `npm install` will create it — commit it); pinned versions were chosen from memory and unverified for availability/compatibility; `Async` wrapper types and Recharts prop types may need small fixes at first `tsc`; no component/DOM tests; no ESLint config; `/trace`, `/social/{id}`, `/news/{id}` not surfaced (no event-trace UI); no cross-market optional workspace; sentiment trend/activity depend on analytics_job having produced windows (empty states shown until then); `/attention` capped at 1000 rows (banner shown when hit).
- **Slice 7 Integration & polish — IMPLEMENTED (limited verification; see below).**
  - Scope: inspect only integration seams; fix concrete blockers; add verification tooling; finalize docs.
  - Seams inspected by reading (no defects found): Mongo unique indexes vs analytics upsert keys (entity_metrics, attention_metrics, anomaly_events); compose service names/env/ports vs Dockerfiles (config copied to `/app/config`, `MARKETPULSE_CONFIG_DIR` set); nginx proxy (`/api/` with Upgrade headers, `/health`) vs API routes; frontend `types/api.ts` vs `schemas.py`; WS message envelope.
  - Defects fixed: (1) WS JSON used `default=str` → non-ISO datetimes (breaks Safari parsing and the UI's snapshot-vs-live comparison) → `json_default`; (2) REST `/social`,`/news` returned `data_mode=test` smoke records mixed with production data → default exclusion + `include_test`; (3) `vite.config.ts` used a `test` key under vite's `defineConfig` (type error under `tsc`) → `vitest/config`; (4) `React.ReactNode` without import → `import type { ReactNode }`; (5) added `web` healthcheck that exercises the nginx→api proxy.
  - Added: `scripts/verify_stack.py` (see README); no new product features.
  - **Checks actually executed (sandbox, offline)**: py_compile of all changed Python files; `test_serialization.py` 3 tests run directly: pass; `verify_stack.py` run against a stdlib fake server (13 PASS / 3 WARN / 1 FAIL — the FAIL was a deliberately failed fake source; confirms the script reports failures, warnings and passes correctly); compose YAML parse.
  - **NOT run**: pytest (FastAPI/pymongo/pytest not installable offline) — therefore the new `test_text_routes` test, the FakeRepo change, the S6 CORS change and **all earlier backend suites were not run this session**; any Spark/Kafka/Mongo/Docker execution; `npm install/typecheck/test/build`; real browser; live-source validation (Yahoo, Bluesky, any RSS feed remain UNVERIFIED); persistence-across-restart test; checkpoint restart test; WebSocket against a real server.

## F. Decisions
(All earlier decisions 1-14 unchanged.)

15. (S5) analytics_job is NOT a Spark job — it's pure Python polling MongoDB. This avoids a third JVM on a developer laptop and the complexity of multi-stream watermarked joins across market/social/news. Spark's role stays confined to Kafka→MongoDB ingestion; aggregations are performed post-persistence.
16. (S5) Sentiment scored in TWO places: (a) Spark foreachBatch (immediate, for new records); (b) analytics_job back-fill pass (retroactive, for records already persisted before S5 was deployed). Both are idempotent.
17. (S5) Cross-signal descriptors are computed live in the API layer (reading from entity_metrics) rather than stored as a separate collection — they are cheap point-in-time reads, not time-series data.
18. (S5) WebSocket uses periodic MongoDB polling (not Change Streams) because the stack uses standalone MongoDB without replica-set oplog access. Polling interval 3s is acceptable for a near-real-time display.
19. (S6) Frontend talks to the API same-origin (Vite proxy in dev, nginx in Docker) so no CORS is needed by default; CORS is an opt-in env for cross-origin setups.
20. (S6) No new backend endpoints were added. Where the API lacks an aggregate (cross-entity metrics, period returns), the UI fans out per-entity calls or does a labelled display calculation; a future backend aggregate endpoint is preferable.
21. (S6) Recharts + Tailwind 3 (not 4) chosen for stability; versions are pinned but unverified in this sandbox.
22. (S7) Test-mode records are excluded from REST lists by default rather than deleted, so smoke tests still work via `include_test=true`.
23. (S7) No new endpoints/features; `verify_stack.py` is a script, not a service.

## G. Risks / limitations
(All earlier risks unchanged, plus S7: the whole system is unverified end-to-end; backend pytest was not runnable in the S7 sandbox; `/trace/{event_id}` exists in the API but has no UI. Plus S6: see Slice 6 'Known gaps/risks' in E. Plus S5:)
- analytics_job anomalies require ≥24 windows of history — expect NO_DATA for several hours after first startup.
- VADER scores news titles only (not article bodies; full text not fetched per design). Scores are approximate.
- analytics_job back-fill processes 500 un-scored docs per cycle; with a large backlog this will take multiple cycles. `data_mode=test` docs from smoke scripts will NOT be scored (they pass the VADER filter but are excluded from analytics queries).
- WebSocket max client memory: ~100 events on connect + 20 per poll. This is appropriate for a developer dashboard.
- No Kafka-lag instrumentation (UNKNOWN in pipeline/status, as before).
- Spark checkpoint changes: bumping checkpoint suffix invalidates history; at-least-once guarantees still hold (idempotent upsert).

## H. Final run & verification checklist (user, on a machine with Docker + Node) — nothing here has been executed yet

```powershell
# 0. Config + start (repo root)
Copy-Item .env.example .env
docker compose -f infra/compose.yaml --env-file .env up -d --build     # or .\scripts\up.ps1
docker compose -f infra/compose.yaml ps                                # all Up/healthy; *-init services Exited(0)
docker compose -f infra/compose.yaml logs -f ingestion-market spark-streaming   # first market fetch takes several minutes

# 1. Unit tests on the host (needs: py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -e ".[dev]")
$env:MARKETPULSE_CONFIG_DIR="config"
pytest services/common/tests services/api/tests services/streaming/tests/test_analytics.py services/ingestion/tests

# 2. Frontend
cd apps\web; npm install; npm run typecheck; npm test; npm run build; cd ..\..    # commit apps/web/package-lock.json

# 3. Stack verification through the public surface (read-only)
python scripts/verify_stack.py                  # PASS/WARN/FAIL per check; WARN = no data yet, FAIL = real problem
python scripts/smoke_market.py --malformed --redeliver   # traces yfinance -> Kafka -> Spark -> Mongo -> API
python scripts/smoke_text.py --live-check                # injects labelled test records (hidden from UI) + reports real adapter output

# 4. Persistence across restart
docker compose -f infra/compose.yaml down        # keeps volumes
docker compose -f infra/compose.yaml --env-file .env up -d
python scripts/verify_stack.py                   # watchlist bars still present
# Deliberate clean reset: docker compose -f infra/compose.yaml down -v

# 5. Browser: http://localhost:3000  (Overview, Entity, Firehose, Pipeline)
```
Expected on a healthy first run: market data appears after the first yfinance run; social/news appear as feeds/Bluesky deliver matches; analytics windows appear after the first `analytics-job` cycle (≤5 min after matched items exist); anomalies only after ≥24 hourly windows. Some feeds in `config/sources.yaml` may be blocked/changed — the Pipeline tab will show FAILED with the error; this is expected behaviour, not a UI bug.

**If something fails**: record the exact error in section G and fix only that; likely first-run suspects are Spark Kafka-connector jar download (`fetch_spark_jars.py`), yfinance throttling, npm pin mismatches, feed URLs.

**Continuation prompt for a new account:** "Read BUILD.md in the attached MarketPulse repo. All 7 slices are IMPLEMENTED but the stack has never been run. I will paste the output of the section H checklist; fix only the concrete failures it shows, keep contracts in section D/D6 intact, record only checks you actually executed, and update BUILD.md."

## I. Cross-account reconciliation
Follow spec §16-I: read BUILD.md, inspect supplied files, diff contracts, pick authoritative versions, continue from next slice, update BUILD.md before handoff.
Executed checks in the Slice 7 session: see Slice 7 in section E (py_compile, 3 direct serializer tests, verify_stack vs fake server, compose parse; NO pytest). Slice 6 session: see Slice 6 in section E (tsc syntax-level parse only, stand-in harness 10/10, py_compile main.py, compose YAML parse). Previous-slice test suites were intentionally not re-run.
