"""Smoke test for the social + news paths: Kafka -> Spark -> MongoDB -> FastAPI (stack must be up).
    python scripts/smoke_text.py [--timeout 120] [--live-check]
Part 1 (injection): publishes ONE clearly-labelled data_mode=test social post and news article (plus a malformed record
to each topic) directly to Kafka, then verifies that Spark processed them: the API returns the same event_id and text, with Kafka
provenance, processed_at and no duplicate after redelivery; malformed records are counted in `dead_letters`.
  NOTE: this proves Kafka->Spark->Mongo->API only. It does NOT prove the Bluesky/RSS adapters work.
Part 2 (--live-check): reports what the REAL adapters have persisted so far (non-test posts/articles + source status).
Test records stay in Mongo (data_mode=test, entity 'nvda'); filter them out of analytics later. They expire via TTL.
Requires: pip install confluent-kafka (host) ; env KAFKA_BOOTSTRAP_SERVERS_HOST (default localhost:29092), API_URL."""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "common"))
from marketpulse_common.events import DataMode, NewsArticleEvent, SocialPostEvent  # noqa: E402
from marketpulse_common.ids import news_event_id, social_event_id  # noqa: E402

API = os.environ.get("API_URL", "http://localhost:8000")
BOOT = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
FAILS = []


def get(path):
    with urllib.request.urlopen(API + path, timeout=15) as r:
        return json.loads(r.read())


def check(ok, msg):
    print(("PASS " if ok else "FAIL ") + msg)
    if not ok:
        FAILS.append(msg)
    return ok


def wait(fn, timeout):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(3)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--live-check", action="store_true")
    a = ap.parse_args()
    from confluent_kafka import Producer
    prod = Producer({"bootstrap.servers": BOOT, "enable.idempotence": True})
    now = datetime.now(timezone.utc)
    tag = uuid.uuid4().hex[:10]
    uri = f"at://did:plc:smoketest/app.bsky.feed.post/{tag}"
    soc = SocialPostEvent(event_id=social_event_id("bluesky", uri), source="smoke_test", event_time=now, ingested_at=now,
                          data_mode=DataMode.TEST, matched_entities=["nvda"], platform_post_id=uri, author_id="did:plc:smoketest",
                          text=f"[smoke {tag}] $NVDA test post", created_at=now, language="en", relevance_score=1.0)
    url = f"https://example.invalid/smoke/{tag}"
    new = NewsArticleEvent(event_id=news_event_id(url, "SmokeTest", tag), source="smoke_test", event_time=None, ingested_at=now,
                           data_mode=DataMode.TEST, matched_entities=["nvda"], publisher="SmokeTest", title=f"[smoke {tag}] Nvidia test headline",
                           url=url, published_at=None)
    dl0 = get("/api/v1/pipeline/status")
    for topic, ev in (("social.raw", soc), ("news.raw", new)):
        d = ev.model_dump(mode="json")
        prod.produce(topic, key=ev.event_id.encode(), value=json.dumps(d).encode())
        prod.produce(topic, key=ev.event_id.encode(), value=json.dumps(d).encode())      # redelivery
        prod.produce(topic, key=b"bad", value=b'{"event_id": "x"')                      # malformed JSON
    prod.flush(15)
    print(f"published test events {soc.event_id} / {new.event_id}; waiting up to {a.timeout}s for Spark...")

    s = wait(lambda: get(f"/api/v1/social/{soc.event_id}"), a.timeout)
    if check(s is not None, "social post reached the API"):
        check(s["text"] == soc.text and s["platform_post_id"] == uri and s["data_mode"] == "test", "social content/identity preserved")
        check(s["entity_ids"] == ["nvda"] and s["kafka"]["topic"] == "social.raw" and s["processed_at"], "social has Kafka provenance + processed_at")
    n = wait(lambda: get(f"/api/v1/news/{new.event_id}"), a.timeout)
    if check(n is not None, "news article reached the API"):
        check(n["title"] == new.title and n["publisher"] == "SmokeTest" and n["data_mode"] == "test", "news content/identity preserved")
        check(n["published_at"] is None and n["timestamp_quality"] == "missing", "missing published_at preserved (not replaced)")
    time.sleep(25)  # allow the redelivered copies to be consumed
    q = get(f"/api/v1/social?entity=nvda&limit=200&include_test=true")
    check(sum(1 for i in q["items"] if i["event_id"] == soc.event_id) == 1, "redelivery did not duplicate the social post")
    q = get(f"/api/v1/news?entity=nvda&limit=200&include_test=true")
    check(sum(1 for i in q["items"] if i["event_id"] == new.event_id) == 1, "redelivery did not duplicate the news article")
    st = {c["name"]: c for c in get("/api/v1/pipeline/status")["components"] if c["component"] == "spark"}
    for name in ("social_stream", "news_stream"):
        c = st.get(name)
        check(bool(c) and c.get("state") == "RUNNING" and c.get("totals", {}).get("invalid", 0) >= 1, f"spark {name} RUNNING and counted the malformed record")

    if a.live_check:
        print("\n--- live adapter report (real data only) ---")
        for src in get("/api/v1/sources/status"):
            if src["type"] in ("jetstream", "rss", "google_news"):
                print(f"{src['name']:28} state={src['state']:12} verification={src['verification']:17} totals={src['totals']} err={src['last_error']}")
        real_s = [i for i in get("/api/v1/social?limit=200")["items"] if i["data_mode"] != "test"]
        real_n = [i for i in get("/api/v1/news?limit=200")["items"] if i["data_mode"] != "test"]
        print(f"real social posts persisted (latest 200 scanned): {len(real_s)}; real news articles: {len(real_n)}")
    print("\nALL CHECKS PASSED" if not FAILS else f"\n{len(FAILS)} CHECK(S) FAILED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
