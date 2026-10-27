"""Bluesky Jetstream ingestion.  python -m mp_ingestion.run_social

WebSocket -> parse/filter/dedupe -> Kafka social.raw (key = event_id). Reconnects forever with bounded exponential
backoff + jitter. Status (`pipeline_status` component=ingestion name=bluesky_jetstream) is flushed periodically.
No cursor/replay on reconnect: posts during a disconnect are NOT recovered (documented gap; avoids mislabelling replay as live).
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import sys
from collections import OrderedDict
from datetime import datetime, timezone
from urllib.parse import urlencode

from marketpulse_common.config import ConfigError, load_config
from marketpulse_common.matching import EntityMatcher

from .kafka_out import KafkaPublisher
from .social import INVALID, NO_MATCH, NOT_POST_CREATE, PUBLISH, parse_message

log = logging.getLogger("run_social")
SOURCE_NAME = "bluesky_jetstream"
COUNTERS = ("received", "not_post_create", "no_match", "invalid", "duplicates_in_process", "published", "publish_failed")


class SeenCache:
    """Bounded LRU of recently published event_ids (process-local dedupe; Spark upsert is the durable dedupe)."""
    def __init__(self, cap: int = 50_000):
        self.cap, self._d = cap, OrderedDict()

    def add(self, k: str) -> bool:
        """True if new."""
        if k in self._d:
            self._d.move_to_end(k)
            return False
        self._d[k] = None
        if len(self._d) > self.cap:
            self._d.popitem(last=False)
        return True


class SocialStats:
    def __init__(self):
        self.c = {k: 0 for k in COUNTERS}
        self.flushed = {k: 0 for k in COUNTERS}
        self.state, self.connected_at, self.disconnected_at = "CONNECTING", None, None
        self.last_message_at = self.last_publish_at = self.last_event_time = None
        self.last_error, self.reconnects = None, 0

    def delta(self) -> dict[str, int]:
        d = {k: self.c[k] - self.flushed[k] for k in COUNTERS}
        self.flushed = dict(self.c)
        return d

    def update_doc(self, now: datetime, delta: dict[str, int]) -> dict:
        s = {"state": self.state, "updated_at": now, "last_attempt_at": now, "last_error": self.last_error,
             "connected_at": self.connected_at, "disconnected_at": self.disconnected_at,
             "last_message_at": self.last_message_at, "last_event_time": self.last_event_time,
             "reconnects_this_process": self.reconnects}
        if self.last_publish_at:
            s["last_success_at"] = self.last_publish_at   # success = a post actually reached Kafka
        return {"$set": s, "$inc": {f"totals.{k}": v for k, v in delta.items() if v}} if any(delta.values()) else {"$set": s}


def build_url(base: str, collections: list[str]) -> str:
    return base + ("&" if "?" in base else "?") + urlencode([("wantedCollections", c) for c in collections])


def backoff_delay(attempt: int, cap: float, rnd=random.random) -> float:
    """Exponential 1,2,4.. capped at `cap`, plus up to 20% jitter."""
    base = min(cap, 2 ** max(attempt - 1, 0))
    return base * (1 + 0.2 * rnd())


async def handle_message(raw, matcher, publisher, topic, seen: SeenCache, st: SocialStats, now_fn=lambda: datetime.now(timezone.utc)):
    now = now_fn()
    st.c["received"] += 1
    st.last_message_at = now
    r = parse_message(raw, matcher, now)
    if r.outcome == NOT_POST_CREATE:
        st.c["not_post_create"] += 1
    elif r.outcome == NO_MATCH:
        st.c["no_match"] += 1
    elif r.outcome == INVALID:
        st.c["invalid"] += 1
        log.debug("invalid message: %s", r.reason)
    elif r.outcome == PUBLISH:
        ev = r.event
        if not seen.add(ev.event_id):
            st.c["duplicates_in_process"] += 1
            return
        before = publisher.failed
        publisher.publish(topic, ev.event_id, ev.model_dump(mode="json"))
        if publisher.failed > before:
            st.c["publish_failed"] += 1
        else:
            st.c["published"] += 1     # accepted by local producer; delivery acks are reconciled at flush
            st.last_publish_at, st.last_event_time = now, ev.event_time


async def run(cfg, publisher, db, stop: asyncio.Event) -> None:
    import websockets  # lazy
    src = next(s for s in cfg.sources if s.name == SOURCE_NAME)
    url = build_url(src.url, src.options.get("wanted_collections", ["app.bsky.feed.post"]))
    cap = float(src.options.get("max_backoff_seconds", 60))
    stale_after = float(src.options.get("stale_after_seconds", 120))
    matcher, topic, st, seen = EntityMatcher(cfg.entities), cfg.settings.kafka.topics.social_raw, SocialStats(), SeenCache()

    async def flusher():
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), 10)
            except asyncio.TimeoutError:
                pass
            await asyncio.to_thread(publisher.flush, 10)
            now = datetime.now(timezone.utc)
            try:
                await asyncio.to_thread(db["pipeline_status"].update_one, {"component": "ingestion", "name": SOURCE_NAME},
                                        st.update_doc(now, st.delta()), True)
            except Exception:
                log.exception("status write failed")

    ft = asyncio.create_task(flusher())
    attempt = 0
    while not stop.is_set():
        st.state = "CONNECTING"
        try:
            async with websockets.connect(url, open_timeout=src.timeout_seconds, ping_interval=20, ping_timeout=20,
                                          max_size=2**20) as ws:
                st.state, st.connected_at, attempt, st.last_error = "CONNECTED", datetime.now(timezone.utc), 0, None
                log.info("connected %s", url)
                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=stale_after)
                    except asyncio.TimeoutError:
                        raise ConnectionError(f"no message for {stale_after}s; reconnecting")
                    await handle_message(raw, matcher, publisher, topic, seen, st)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            st.last_error = f"{type(e).__name__}: {e}"[:300]
            log.warning("connection lost: %s", st.last_error)
        st.disconnected_at, st.reconnects, st.state = datetime.now(timezone.utc), st.reconnects + 1, "BACKOFF"
        attempt += 1
        d = backoff_delay(attempt, cap)
        log.info("reconnect in %.1fs (attempt %d)", d, attempt)
        try:
            await asyncio.wait_for(stop.wait(), d)
        except asyncio.TimeoutError:
            pass
    st.state = "STOPPED"
    await ft


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        cfg = load_config()
    except ConfigError as e:
        log.error("config invalid: %s", e)
        return 2
    src = next((s for s in cfg.sources if s.name == SOURCE_NAME), None)
    if src is None or not src.enabled:
        log.error("source %s missing or disabled", SOURCE_NAME)
        return 2
    from pymongo import MongoClient
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True,
                     serverSelectionTimeoutMS=5000)[os.environ.get("MONGO_DB", "marketpulse")]
    publisher = KafkaPublisher(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))

    async def _main():
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:   # Windows (non-container) fallback
                signal.signal(sig, lambda *_: stop.set())
        await run(cfg, publisher, db, stop)

    asyncio.run(_main())
    publisher.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
