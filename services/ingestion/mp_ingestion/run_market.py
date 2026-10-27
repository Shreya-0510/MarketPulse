"""Market ingestion process.  python -m mp_ingestion.run_market [--once]

Per symbol: fetch (bounded retries) -> normalize/validate -> publish to Kafka market.raw -> account.
One symbol failing never stops the others. Re-publishing is safe: event_id is deterministic and the Spark sink upserts.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone

from marketpulse_common.config import AppConfig, ConfigError, load_config

from .kafka_out import KafkaPublisher
from .market import fetch_history, normalize_rows
from .status import record_run, run_update

log = logging.getLogger("run_market")
SOURCE_NAME = "yfinance_daily"


def has_history(db, symbol: str, interval: str) -> bool:
    try:
        return db["market_prices"].count_documents({"symbol": symbol, "interval": interval}, limit=1) > 0
    except Exception:
        return False  # Mongo unavailable -> fall back to full lookback (safe: idempotent)


def run_cycle(cfg: AppConfig, publisher, db, *, fetch=fetch_history, sleep=None, stop: threading.Event | None = None,
              now_fn=lambda: datetime.now(timezone.utc)) -> dict:
    src = next(s for s in cfg.sources if s.name == SOURCE_NAME)
    m = cfg.settings.market
    interval = src.options.get("interval", m["default_interval"])
    lookback = src.options.get("lookback_period", m["lookback_period"])
    incremental = m.get("incremental_period", "5d")
    attempts = int(m.get("max_fetch_attempts", 3))
    delay = float(m.get("request_delay_seconds", 1.0))
    topic = cfg.settings.kafka.topics.market_raw
    stop = stop or threading.Event()
    sleep = sleep or (lambda s: stop.wait(s))

    counters = dict(fetched_rows=0, normalized=0, rejected=0, duplicates_in_fetch=0, published=0, publish_failed=0)
    per_symbol: dict[str, dict] = {}
    errors: list[str] = []
    for inst in cfg.instruments:
        if stop.is_set():
            break
        period = incremental if has_history(db, inst.symbol, interval) else lookback
        rows, last_err = None, None
        for attempt in range(1, attempts + 1):
            try:
                rows = fetch(inst.symbol, period, interval, src.timeout_seconds)
                break
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                log.warning("%s fetch attempt %d/%d failed: %s", inst.symbol, attempt, attempts, last_err)
                if attempt < attempts:
                    sleep(min(2 ** attempt, 30))
        if rows is None:
            per_symbol[inst.symbol] = {"ok": False, "error": last_err, "period": period}
            errors.append(f"{inst.symbol}: {last_err}")
            continue
        res = normalize_rows(inst, rows, interval, now_fn())
        acked0, failed0 = publisher.acked, publisher.failed
        for ev in res.events:
            publisher.publish(topic, f"{inst.symbol}|{interval}", ev.model_dump(mode="json"))
        publisher.flush()
        acked, failed = publisher.acked - acked0, publisher.failed - failed0
        counters["fetched_rows"] += len(rows)
        counters["normalized"] += len(res.events)
        counters["rejected"] += len(res.rejected)
        counters["duplicates_in_fetch"] += res.duplicates
        counters["published"] += acked
        counters["publish_failed"] += failed
        ok = failed == 0 and len(res.events) > 0
        per_symbol[inst.symbol] = {"ok": ok, "period": period, "fetched": len(rows), "published": acked,
                                   "rejected": len(res.rejected), "error": None if ok else (publisher.last_error or "no valid bars")}
        if not ok:
            errors.append(f"{inst.symbol}: {per_symbol[inst.symbol]['error']}")
        log.info("%s period=%s fetched=%d published=%d rejected=%d", inst.symbol, period, len(rows), acked, len(res.rejected))
        sleep(delay)

    n_ok = sum(1 for v in per_symbol.values() if v["ok"])
    state = "OK" if n_ok == len(cfg.instruments) else ("DEGRADED" if n_ok else "FAILED")
    record_run(db, SOURCE_NAME, run_update(state=state, now=now_fn(), error="; ".join(errors[:5]) or None,
                                           counters=counters, per_symbol=per_symbol))
    return {"state": state, "counters": counters, "per_symbol": per_symbol}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="run a single cycle and exit")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        cfg = load_config()
    except ConfigError as e:
        log.error("config invalid: %s", e)
        return 2
    src = next((s for s in cfg.sources if s.name == SOURCE_NAME), None)
    if src is None or not src.enabled:
        log.error("source %s missing or disabled in sources.yaml", SOURCE_NAME)
        return 2

    from pymongo import MongoClient
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True,
                     serverSelectionTimeoutMS=5000)[os.environ.get("MONGO_DB", "marketpulse")]
    publisher = KafkaPublisher(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    while not stop.is_set():
        try:
            result = run_cycle(cfg, publisher, db, stop=stop)
        except Exception:
            log.exception("cycle crashed; will retry next interval")
            result = {"state": "FAILED"}
        if args.once:
            return 0 if result["state"] in ("OK", "DEGRADED") else 1
        stop.wait(src.poll_interval_seconds)
    log.info("shutdown")
    return 0


if __name__ == "__main__":
    sys.exit(main())
