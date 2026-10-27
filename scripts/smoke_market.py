"""End-to-end smoke test for the market path: yfinance -> Kafka -> Spark -> MongoDB -> FastAPI.
Stdlib only (+ repo's marketpulse_common.ids). Run from repo root with the stack up:
    python scripts/smoke_market.py [--symbol AAPL] [--timeout 420] [--malformed] [--redeliver]
Checks record CONTENT and identity, not just HTTP 200. Exit code 0 only if every check passes."""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "common"))
from marketpulse_common.ids import market_event_id  # noqa: E402

API = os.environ.get("API_URL", "http://localhost:8000")
COMPOSE = ["docker", "compose", "-f", "infra/compose.yaml", "--env-file", ".env"]
FAILS = []


def get(path):
    with urllib.request.urlopen(API + path, timeout=15) as r:
        return json.loads(r.read())


def check(ok, msg):
    print(("PASS " if ok else "FAIL ") + msg)
    if not ok:
        FAILS.append(msg)
    return ok


def wait(pred, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = pred()
            if v:
                return v
        except (urllib.error.URLError, OSError, KeyError, ValueError):
            pass
        time.sleep(5)
    print(f"TIMEOUT waiting for {what}")
    return None


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def comp(name):
    for c in get("/api/v1/pipeline/status")["components"]:
        if c.get("name") == name:
            return c
    return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="AAPL")
    ap.add_argument("--timeout", type=int, default=420)
    ap.add_argument("--malformed", action="store_true")
    ap.add_argument("--redeliver", action="store_true")
    a = ap.parse_args()
    sym = urllib.parse.quote(a.symbol, safe="")

    check(get("/health")["mongodb"] == "ok", "API /health reports MongoDB ok")
    hist = wait(lambda: (lambda h: h if h["count"] > 0 else None)(get(f"/api/v1/market/{sym}/history?limit=2000")), a.timeout,
                f"{a.symbol} bars to appear via API (yfinance fetch -> Kafka -> Spark -> Mongo)")
    if not check(hist is not None, f"{a.symbol} bars reached the API"):
        return finish()
    bars = hist["bars"]
    check(len({b["event_id"] for b in bars}) == len(bars), f"no duplicate event_ids ({len(bars)} bars)")
    times = [parse(b["event_time"]) for b in bars]
    check(times == sorted(times), "bars ascending by event_time")
    check(all(market_event_id("yfinance", b["symbol"], b["interval"], parse(b["event_time"])) == b["event_id"] for b in bars),
          "every event_id recomputes from (provider, symbol, interval, bar time)")
    check(all(b["close"] > 0 and b["data_mode"] == "historical" and b["source"] == "yfinance" for b in bars),
          "close>0, data_mode=historical, source=yfinance on all bars (never labelled live)")
    check(all(parse(b["processed_at"]) >= parse(b["ingested_at"]) for b in bars), "processed_at >= ingested_at")

    newest = bars[-1]
    t = get(f"/api/v1/trace/{newest['event_id']}")
    check(t["kafka"] and t["kafka"]["topic"] == "market.raw" and t["kafka"]["offset"] >= 0,
          f"trace shows Kafka provenance market.raw[{t['kafka']['partition']}]@{t['kafka']['offset']}")
    check(t["close"] == newest["close"], "trace record matches history record")

    ing, spk = comp("yfinance_daily"), comp("market_stream")
    check(ing.get("totals", {}).get("published", 0) > 0, f"ingestion published={ing.get('totals', {}).get('published')}")
    check(spk.get("totals", {}).get("valid", 0) > 0, f"spark valid={spk.get('totals', {}).get('valid')} (state {spk.get('state')})")
    wl = {i["symbol"]: i for i in get("/api/v1/market/watchlist")["items"]}
    check(wl[a.symbol]["status"] in ("OK", "STALE") and wl[a.symbol]["latest"]["close"] > 0, "watchlist row populated from persisted data")

    if a.malformed:
        before = spk.get("totals", {}).get("invalid", 0)
        n0 = get(f"/api/v1/market/{sym}/history?limit=2000")["total_in_range"]
        subprocess.run(COMPOSE + ["run", "--rm", "ingestion-market", "python", "-m", "mp_ingestion.inject_bad"], check=False)
        ok = wait(lambda: comp("market_stream").get("totals", {}).get("invalid", 0) >= before + 2, 120, "invalid counter +2")
        check(bool(ok), "Spark counted 2 malformed events as invalid")
        check(comp("market_stream").get("state") == "RUNNING", "Spark still RUNNING after bad input")
        check(get(f"/api/v1/market/{sym}/history?limit=2000")["total_in_range"] == n0, "bad events created no bars")

    if a.redeliver:
        n0 = get(f"/api/v1/market/{sym}/history?limit=2000")["total_in_range"]
        d0 = t.get("delivery_count") or 1
        subprocess.run(COMPOSE + ["run", "--rm", "ingestion-market", "python", "-m", "mp_ingestion.run_market", "--once"], check=False)
        ok = wait(lambda: get(f"/api/v1/trace/{newest['event_id']}").get("delivery_count", 0) > d0, 180, "redelivery to be consumed")
        check(bool(ok), f"redelivered event consumed again (delivery_count > {d0})")
        check(get(f"/api/v1/market/{sym}/history?limit=2000")["total_in_range"] == n0, "redelivery created no duplicate bars")
    return finish()


def finish():
    print("\nRESULT:", "ALL CHECKS PASSED" if not FAILS else f"{len(FAILS)} CHECK(S) FAILED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
