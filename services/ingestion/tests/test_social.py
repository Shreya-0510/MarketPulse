import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from marketpulse_common.config import load_config
from marketpulse_common.matching import EntityMatcher
from mp_ingestion.run_social import SeenCache, SocialStats, backoff_delay, build_url, handle_message
from mp_ingestion.social import INVALID, NO_MATCH, NOT_POST_CREATE, PUBLISH, parse_message

CFG = load_config(Path(__file__).resolve().parents[3] / "config")
M = EntityMatcher(CFG.entities)
NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
TUS = int(datetime(2026, 10, 27, 11, 59, 58, tzinfo=timezone.utc).timestamp() * 1_000_000)


def msg(text="Buying $NVDA ahead of earnings", op="create", coll="app.bsky.feed.post", created="2026-10-27T11:59:50.000Z", **rec):
    record = {"$type": coll, "text": text, "createdAt": created, "langs": ["en"], **rec}
    return {"did": "did:plc:abc123", "time_us": TUS, "kind": "commit",
            "commit": {"rev": "r", "operation": op, "collection": coll, "rkey": "3kxyz", "cid": "bafy", "record": record}}


def test_matching_post_becomes_event_with_stable_identity():
    r = parse_message(json.dumps(msg()), M, NOW)
    assert r.outcome == PUBLISH
    e = r.event
    assert e.platform_post_id == "at://did:plc:abc123/app.bsky.feed.post/3kxyz"
    assert e.event_id.startswith("soc_") and e.event_id == parse_message(msg(), M, NOW).event.event_id
    assert e.matched_entities == ["nvda"] and e.entity_ids == ["nvda"] and e.relevance_score == 1.0
    assert e.language == "en" and e.data_mode.value == "live" and e.author_id == "did:plc:abc123"
    assert e.post_url == "https://bsky.app/profile/did:plc:abc123/post/3kxyz"
    assert e.created_at.isoformat().startswith("2026-10-27T11:59:50") and e.event_time.second == 58
    assert e.source_metadata["created_at_source"] == "record"


def test_future_created_at_falls_back_to_server_time():
    e = parse_message(msg(created="2030-01-01T00:00:00Z"), M, NOW).event
    assert e.created_at == e.event_time and e.source_metadata["created_at_source"] == "time_us"


def test_filtering_outcomes():
    assert parse_message(msg(op="delete"), M, NOW).outcome == NOT_POST_CREATE
    assert parse_message(msg(coll="app.bsky.feed.like"), M, NOW).outcome == NOT_POST_CREATE
    assert parse_message({"kind": "identity", "did": "did:plc:x"}, M, NOW).outcome == NOT_POST_CREATE
    assert parse_message(msg(text="I love apple pie"), M, NOW).outcome == NO_MATCH


def test_invalid_messages_are_classified_not_raised():
    assert parse_message("{not json", M, NOW).outcome == INVALID
    assert parse_message("[1]", M, NOW).outcome == INVALID
    assert parse_message(msg(text="  "), M, NOW).reason == "empty_text"
    bad = msg(); bad["time_us"] = "x"
    assert parse_message(bad, M, NOW).reason == "invalid_time_us"
    bad = msg(); bad["did"] = "nope"
    assert parse_message(bad, M, NOW).reason == "missing_did_rkey_or_record"


class Pub:
    def __init__(self): self.sent, self.failed, self.acked = [], 0, 0
    def publish(self, topic, key, value): self.sent.append((topic, key, value))


def test_handle_message_counts_dedupes_and_keys_by_event_id():
    pub, st, seen = Pub(), SocialStats(), SeenCache()
    for m in (msg(), msg(), msg(text="nothing relevant"), msg(op="delete"), "garbage"):
        asyncio.run(handle_message(m, M, pub, "social.raw", seen, st, now_fn=lambda: NOW))
    assert st.c["received"] == 5 and st.c["published"] == 1 and st.c["duplicates_in_process"] == 1
    assert st.c["no_match"] == 1 and st.c["not_post_create"] == 1 and st.c["invalid"] == 1
    topic, key, value = pub.sent[0]
    assert topic == "social.raw" and key == value["event_id"] and value["text"].startswith("Buying")
    assert st.last_event_time is not None and st.last_publish_at == NOW


def test_seen_cache_is_bounded():
    c = SeenCache(cap=2)
    assert c.add("a") and c.add("b") and c.add("c") and c.add("a")   # 'a' evicted then re-added


def test_backoff_is_bounded_and_grows():
    d = [backoff_delay(i, 60, rnd=lambda: 0.0) for i in range(1, 9)]
    assert d[:4] == [1, 2, 4, 8] and max(d) == 60 and backoff_delay(20, 60, rnd=lambda: 1.0) == 72.0


def test_status_delta_flush_only_reports_new_counts():
    st = SocialStats(); st.c["published"] = 3
    d = st.delta(); assert d["published"] == 3 and st.delta()["published"] == 0
    doc = st.update_doc(NOW, {"published": 3, "received": 0})
    assert doc["$inc"] == {"totals.published": 3} and "last_success_at" not in doc["$set"]


def test_build_url():
    assert build_url("wss://x/subscribe", ["app.bsky.feed.post"]) == "wss://x/subscribe?wantedCollections=app.bsky.feed.post"
