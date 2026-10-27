"""Read-only verification of a RUNNING stack through its public HTTP surface (stdlib only).

  python scripts/verify_stack.py                       # API on :8000, web on :3000
  python scripts/verify_stack.py --api http://localhost:8000 --web http://localhost:3000 [--no-web]

Prints PASS / FAIL / WARN per check and a final tally; exit code 1 if any FAIL. It never writes data and never
treats an empty system as success: "no data yet" is reported as WARN (pipeline may still be warming up).
"""
from __future__ import annotations

import argparse, base64, json, os, socket, sys, urllib.error, urllib.request
from urllib.parse import urlparse

res = {"PASS": 0, "FAIL": 0, "WARN": 0}


def out(level: str, msg: str) -> None:
    res[level] += 1
    print(f"[{level}] {msg}")


def get(base: str, path: str, timeout: float = 15):
    with urllib.request.urlopen(base + path, timeout=timeout) as r:
        return r.status, r.read()


def jget(base: str, path: str):
    st, body = get(base, path)
    return json.loads(body)


def check(name: str, fn):
    try:
        return fn()
    except urllib.error.HTTPError as e:
        out("FAIL", f"{name}: HTTP {e.code}")
    except Exception as e:  # noqa: BLE001 - report every failure mode
        out("FAIL", f"{name}: {type(e).__name__}: {e}")


def ws_handshake(base: str, path: str) -> str:
    u = urlparse(base)
    key = base64.b64encode(os.urandom(16)).decode()
    with socket.create_connection((u.hostname, u.port or 80), timeout=8) as s:
        s.sendall((f"GET {path} HTTP/1.1\r\nHost: {u.netloc}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        return s.recv(256).decode(errors="replace").split("\r\n")[0]


def verify_api(api: str) -> None:
    h = check("GET /health", lambda: jget(api, "/health"))
    if h:
        out("PASS" if h.get("mongodb") == "ok" else "FAIL", f"/health process={h.get('process')} mongodb={h.get('mongodb')} (kafka/spark are UNKNOWN by design)")
    ents = check("GET /entities", lambda: jget(api, "/api/v1/entities"))
    if ents:
        out("PASS" if ents["count"] > 0 else "FAIL", f"/entities: {ents['count']} configured")
    wl = check("GET /market/watchlist", lambda: jget(api, "/api/v1/market/watchlist"))
    if wl:
        ok = [i for i in wl["items"] if i["status"] != "NO_DATA"]
        stale = [i["symbol"] for i in wl["items"] if i["status"] == "STALE"]
        out("PASS" if ok else "WARN", f"watchlist: {len(ok)}/{len(wl['items'])} instruments have persisted bars" + (f"; STALE: {stale}" if stale else ""))
        if ok:
            sym = ok[0]["symbol"]
            hh = check(f"history {sym}", lambda: jget(api, f"/api/v1/market/{sym}/history?limit=5"))
            if hh:
                out("PASS" if hh["bars"] and hh["bars"][0].get("event_id") else "FAIL", f"history {sym}: {hh['count']} bars returned, total_in_range={hh['total_in_range']}")
    for kind in ("social", "news"):
        d = check(f"GET /{kind}", lambda k=kind: jget(api, f"/api/v1/{k}?limit=5"))
        if d:
            tests = [i for i in d["items"] if i["data_mode"] == "test"]
            out("FAIL" if tests else ("PASS" if d["total"] else "WARN"), f"/{kind}: total={d['total']} (test-mode rows leaked: {len(tests)})" if d["total"] else f"/{kind}: no persisted items yet")
    src = check("GET /sources/status", lambda: jget(api, "/api/v1/sources/status"))
    if src:
        for s in src:
            lvl = "PASS" if s["state"] == "OK" else ("WARN" if s["state"] in ("NOT_RUN", "DISABLED", "DEGRADED") else "FAIL")
            out(lvl, f"source {s['name']}: state={s['state']} verification={s['verification']} last_success={s['last_success_at']}" + (f" error={s['last_error']}" if s.get("last_error") else ""))
    ps = check("GET /pipeline/status", lambda: jget(api, "/api/v1/pipeline/status"))
    if ps:
        sp = [c for c in ps["components"] if c.get("component") == "spark"]
        out("PASS" if ps["mongodb"] == "ok" else "FAIL", f"pipeline/status mongodb={ps['mongodb']} kafka={ps['kafka']}")
        out("PASS" if sp and all(c.get("state") == "RUNNING" for c in sp) else "WARN", f"spark queries recorded: {[(c['name'], c.get('state')) for c in sp]} (status is written only after a non-empty micro-batch)")
    for path in ("/api/v1/attention?limit=5", "/api/v1/anomalies?limit=5", "/api/v1/cross-signals?entity=nvda"):
        d = check(f"GET {path}", lambda p=path: jget(api, p))
        if d:
            out("PASS" if d["status"] == "OK" else "WARN", f"{path.split('?')[0]}: status={d['status']} count={d['count']}")
    try:
        line = ws_handshake(api, "/api/v1/ws")
        out("PASS" if " 101 " in line else "FAIL", f"WebSocket handshake on API: {line}")
    except Exception as e:  # noqa: BLE001
        out("FAIL", f"WebSocket handshake on API: {type(e).__name__}: {e}")


def verify_web(web: str) -> None:
    r = check("web index", lambda: get(web, "/"))
    if r:
        out("PASS" if b'id="root"' in r[1] else "FAIL", "web: index.html served")
    e = check("web -> api proxy", lambda: jget(web, "/api/v1/entities"))
    if e:
        out("PASS" if e.get("count") else "FAIL", f"web proxy /api/v1/entities -> {e.get('count')} entities")
    try:
        line = ws_handshake(web, "/api/v1/ws")
        out("PASS" if " 101 " in line else "FAIL", f"WebSocket handshake through web proxy: {line}")
    except Exception as ex:  # noqa: BLE001
        out("FAIL", f"WebSocket handshake through web proxy: {type(ex).__name__}: {ex}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default=os.environ.get("API_URL", "http://localhost:8000"))
    ap.add_argument("--web", default=os.environ.get("WEB_URL", "http://localhost:3000"))
    ap.add_argument("--no-web", action="store_true")
    a = ap.parse_args()
    verify_api(a.api.rstrip("/"))
    if not a.no_web:
        verify_web(a.web.rstrip("/"))
    print(f"\nPASS={res['PASS']} WARN={res['WARN']} FAIL={res['FAIL']}  (WARN = no data/idle yet, not proof of failure)")
    sys.exit(1 if res["FAIL"] else 0)
