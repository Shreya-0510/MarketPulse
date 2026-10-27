# MarketPulse — Real-Time Market Intelligence and Sentiment Analytics

BDSMA college project. Pipeline: sources → Kafka → Spark Structured Streaming → MongoDB → FastAPI → React.
**Status: all 7 slices are implemented (foundation, market, Bluesky social, RSS/Atom news, analytics, React dashboard, integration). The complete stack has NOT yet been run end-to-end by the author's tooling (no Docker/network in the authoring sandbox); run the checklist in BUILD.md §H and `python scripts/verify_stack.py`.** See `BUILD.md` for exact status, contracts and next steps. Verification commands: BUILD.md section H (Step 0 and 0b).
Not investment advice. No trading, no price prediction.

## Windows (PowerShell) quick start — from repo root
```powershell
Copy-Item .env.example .env                                   # configuration
docker compose -f infra/compose.yaml --env-file .env up -d --build   # or .\scripts\up.ps1
docker compose -f infra/compose.yaml ps                       # status
docker compose -f infra/compose.yaml logs -f api              # logs
curl.exe http://localhost:8000/health                         # API + live MongoDB probe
docker compose -f infra/compose.yaml run --rm kafka-init      # re-list topics (idempotent)
docker compose -f infra/compose.yaml down                     # stop, KEEP data
docker compose -f infra/compose.yaml down -v                  # DELIBERATE clean reset (deletes Kafka+Mongo data)
```
## Tests (host)
```powershell
py -3.12 -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
python scripts/validate_config.py
```
## End-to-end check (market path)
```powershell
docker compose -f infra/compose.yaml logs -f ingestion-market spark-streaming
python scripts/smoke_market.py                       # traces real bars: yfinance -> Kafka -> Spark -> Mongo -> API
python scripts/smoke_market.py --malformed --redeliver   # also bad-event handling + duplicate redelivery
curl.exe http://localhost:8000/api/v1/market/watchlist
curl.exe "http://localhost:8000/api/v1/market/AAPL/history?limit=5"
curl.exe http://localhost:8000/api/v1/pipeline/status
curl.exe http://localhost:8000/api/v1/sources/status
```
The first fetch takes a few minutes (11 symbols, 1 s apart, then the Spark trigger). Healthy containers alone do not prove the pipeline works; the smoke test does.

## Dashboard (Slice 6) — `apps/web`
React 18 + TypeScript + Vite + Tailwind + Recharts. Four workspaces: **Overview**, **Entity Deep Dive**, **Live Firehose**, **Pipeline & Methodology**. All numbers come from the FastAPI endpoints; missing data is shown as "—"/empty states, never as zero or placeholders.

**How it talks to the backend.** The browser uses same-origin paths (`/api/v1/*`, `/health`, WebSocket `/api/v1/ws`):
- Docker: `web` (nginx, http://localhost:3000) serves the SPA and proxies `/api` (incl. WebSocket) to `api:8000`.
- Dev: `npm run dev` (http://localhost:5173) proxies to `VITE_DEV_PROXY_TARGET` (default http://localhost:8000).
- Different origin: set `VITE_API_BASE_URL` (and optionally `VITE_WS_BASE_URL`) at build time and set `CORS_ORIGINS` on the API. See `apps/web/.env.example`.

```powershell
# Full stack including dashboard (from repo root)
Copy-Item .env.example .env
docker compose -f infra/compose.yaml --env-file .env up -d --build
# -> http://localhost:3000

# Frontend dev (API must be running on :8000, e.g. via the compose command above)
cd apps\web
npm install
npm run dev            # http://localhost:5173
npm run typecheck      # tsc --noEmit
npm test               # vitest (formatting, freshness, feed dedupe, series helpers)
npm run build          # type-check + production bundle
```
Notes: analytics panels stay empty until `analytics-job` has run a cycle (default 5 min) and anomalies need ~24 windows of history. The Firehose's "snapshot" items are recent persisted records the server replays on connect; only "newly ingested" items are new arrivals. Pausing the display does not pause ingestion.

## Final architecture
```
yfinance ─► ingestion-market ─┐
Bluesky Jetstream ─► ingestion-social ─┼─► Kafka (KRaft) ─► spark-streaming (market) ─┐
RSS/Atom ─► ingestion-news ────┘          market.raw/social.raw/news.raw ─► spark-text (social+news, VADER) ─┼─► MongoDB ─► FastAPI :8000 ─► nginx/web :3000 (React)
                                                                         analytics-job (polls Mongo) ──────────┘      REST + WebSocket /api/v1/ws
```
Adapters publish only to Kafka; Spark persists idempotently (upsert on stable `event_id`, at-least-once); `analytics-job` computes entity/attention/anomaly metrics from persisted production data. Host ports (127.0.0.1 only): web 3000, API 8000, Mongo 27017, Kafka 29092.

## Configuration
`.env` (from `.env.example`): ports, Mongo URI/DB, retention, `SPARK_TRIGGER_SECONDS`, `ANALYTICS_INTERVAL_SECONDS`, `STALE_AFTER_DAYS`, `WEB_PORT`, `VITE_API_BASE_URL`/`VITE_WS_BASE_URL` (build-time; leave empty for same-origin), `CORS_ORIGINS` (only for cross-origin). YAML in `config/`: `watchlist.yaml`, `entities.yaml`, `sources.yaml`, `settings.yaml` (topics, sentiment and anomaly thresholds, retention).

## Verifying a running stack
```powershell
python scripts/verify_stack.py            # read-only; PASS / WARN (no data yet) / FAIL; checks API, sources, Spark status, analytics, WebSocket, web proxy
python scripts/smoke_market.py            # end-to-end trace of real market bars
python scripts/smoke_text.py --live-check # injects labelled data_mode=test records (hidden from REST lists/UI) and reports real adapter output
```
`/api/v1/social` and `/api/v1/news` exclude `data_mode=test` unless `include_test=true`.

## Known limitations
- Verification status: see BUILD.md §E (Slice 7) — offline authoring; no end-to-end run recorded yet.
- yfinance is unofficial (daily bars, not live ticks, may throttle); Bluesky is one platform and not representative; some RSS endpoints may be blocked or change — failures are shown on the Pipeline tab, never replaced by fake data.
- Kafka lag and Spark micro-batch progress beyond the recorded status are not instrumented (shown UNKNOWN).
- Anomalies need ~24 hourly windows of history; VADER scores headlines only for news; delivery is at-least-once with idempotent upserts (not exactly-once).
- No GDELT, no cross-market workspace, no event-trace UI (`/api/v1/trace/{event_id}` exists), no auth (local-only, ports bound to 127.0.0.1).
- Not investment advice.
