"""RSS/Atom news ingestion.  python -m mp_ingestion.run_news [--once] [--source NAME]

One thread per enabled rss/google_news source (independent schedules and failures). Per poll: conditional GET with
bounded retries (429 honours Retry-After, capped) -> parse -> publish only articles not yet seen by this process -> Kafka news.raw.
Status per source: pipeline_status component=ingestion name=<source name>.
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone

from marketpulse_common.config import AppConfig, ConfigError, Source, load_config
from marketpulse_common.matching import EntityMatcher

from .kafka_out import KafkaPublisher
from .news import FetchError, Fetcher, google_news_urls, parse_feed, search_terms
from .status import record_run

log = logging.getLogger("run_news")
NEWS_TYPES = ("rss", "google_news")
MAX_RETRY_AFTER = 60.0


def poll_urls(src: Source, cfg: AppConfig) -> dict[str, str]:
    """feed key -> URL. Plain RSS: one feed. google_news: one search feed per entity."""
    if src.type == "google_news":
        return {f"{src.name}:{eid}": u for eid, u in google_news_urls(src.url, search_terms(cfg.entities)).items()}
    return {src.name: src.url}


def poll_source(src: Source, cfg: AppConfig, fetcher, publisher, matcher, seen: set[str], db, *,
                sleep=lambda s: None, now_fn=lambda: datetime.now(timezone.utc), attempts: int = 3,
                publish_lock=None) -> dict:
    topic = cfg.settings.kafka.topics.news_raw
    c = dict(feeds_ok=0, feeds_failed=0, entries=0, normalized=0, rejected=0, duplicates_in_feed=0, already_seen=0,
             matched=0, published=0, publish_failed=0, not_modified=0)
    errors: list[str] = []
    last_status = None
    for key, url in poll_urls(src, cfg).items():
        body, err = None, None
        for attempt in range(1, attempts + 1):
            try:
                last_status, body = fetcher.fetch(url, src.timeout_seconds)
                err = None
                break
            except FetchError as e:
                err, last_status = str(e), e.status
                log.warning("%s attempt %d/%d: %s", key, attempt, attempts, e)
                if attempt < attempts:
                    sleep(min(e.retry_after if e.retry_after else 2 ** attempt, MAX_RETRY_AFTER))
        if err:
            c["feeds_failed"] += 1
            errors.append(f"{key}: {err}")
            continue
        c["feeds_ok"] += 1
        if body is None:             # 304 Not Modified
            c["not_modified"] += 1
            continue
        res = parse_feed(body, source=src.name, options=src.options, matcher=matcher, ingested_at=now_fn(), feed_url=url)
        if res.bozo and not res.events:
            c["feeds_ok"] -= 1
            c["feeds_failed"] += 1
            errors.append(f"{key}: {res.bozo}")
            continue
        c["entries"] += res.entries
        c["normalized"] += len(res.events)
        c["rejected"] += len(res.rejected)
        c["duplicates_in_feed"] += res.duplicates
        c["matched"] += res.matched
        with (publish_lock or contextlib.nullcontext()):   # exclusive window so ack/fail deltas are attributable to this source
            a0, f0 = publisher.acked, publisher.failed
            for ev in res.events:
                if ev.event_id in seen:
                    c["already_seen"] += 1
                    continue
                publisher.publish(topic, ev.event_id, ev.model_dump(mode="json"))
                seen.add(ev.event_id)
            publisher.flush()
            c["published"] += publisher.acked - a0
            c["publish_failed"] += publisher.failed - f0
        if res.bozo:
            errors.append(f"{key}: {res.bozo}")
    now = now_fn()
    ok = c["feeds_failed"] == 0 and c["publish_failed"] == 0
    state = "OK" if ok else ("DEGRADED" if c["feeds_ok"] else "FAILED")
    set_doc = {"state": state, "last_attempt_at": now, "updated_at": now, "last_error": "; ".join(errors[:3]) or None,
               "last_http_status": last_status, "last_run": c, "type": src.type}
    if c["feeds_ok"]:
        set_doc["last_success_at"] = now
    record_run(db, src.name, {"$set": set_doc, "$inc": {f"totals.{k}": v for k, v in c.items()}})
    return {"state": state, "counters": c, "errors": errors}


def source_loop(src, cfg, publisher, db, matcher, stop: threading.Event, once: bool = False, lock=None) -> None:
    fetcher, seen = Fetcher(), set()
    while not stop.is_set():
        try:
            poll_source(src, cfg, fetcher, publisher, matcher, seen, db, sleep=lambda s: stop.wait(s), publish_lock=lock)
        except Exception:
            log.exception("%s: poll crashed; retrying next interval", src.name)
        if once:
            return
        stop.wait(max(src.poll_interval_seconds, 30))   # floor: never poll faster than every 30s
        if len(seen) > 100_000:
            seen.clear()      # bound memory; Spark upsert keeps this safe


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--source", help="only this source name")
    args = ap.parse_args(argv)
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        cfg = load_config()
    except ConfigError as e:
        log.error("config invalid: %s", e)
        return 2
    srcs = [s for s in cfg.sources if s.type in NEWS_TYPES and s.enabled and (not args.source or s.name == args.source)]
    if not srcs:
        log.error("no enabled news sources")
        return 2
    from pymongo import MongoClient
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True,
                     serverSelectionTimeoutMS=5000)[os.environ.get("MONGO_DB", "marketpulse")]
    publisher = KafkaPublisher(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    matcher, stop, lock = EntityMatcher(cfg.entities), threading.Event(), threading.Lock()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    threads = [threading.Thread(target=source_loop, args=(s, cfg, publisher, db, matcher, stop, args.once, lock),
                                name=s.name, daemon=True) for s in srcs]
    [t.start() for t in threads]
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=1)
    publisher.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
