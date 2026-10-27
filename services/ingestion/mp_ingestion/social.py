"""Bluesky Jetstream message parsing -> SocialPostEvent. Pure functions (no network / Kafka).

Jetstream commit message shape (JSON):
  {"did": "did:plc:..", "time_us": 1725911162329308, "kind": "commit",
   "commit": {"rev": "..", "operation": "create", "collection": "app.bsky.feed.post", "rkey": "..", "cid": "..",
              "record": {"$type": "app.bsky.feed.post", "text": "..", "createdAt": "2024-..Z", "langs": ["en"]}}}
Decisions: only post *creates* are kept; only posts matching a configured entity are published (the public firehose
is far larger than the watchlist needs); event_time = Jetstream time_us (when the network observed the event);
created_at = author-claimed createdAt when parseable and not > 5 min in the future, else time_us.
Language = first tag of the author-declared `langs` (no detection library; None if absent).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from marketpulse_common.events import DataMode, SocialPostEvent
from marketpulse_common.ids import social_event_id
from marketpulse_common.matching import EntityMatcher, matched_ids

POST_COLLECTION = "app.bsky.feed.post"
PLATFORM = "bluesky"
MAX_TEXT = 3000
FUTURE_SKEW = timedelta(minutes=5)

# outcome labels
PUBLISH, NOT_POST_CREATE, NO_MATCH, INVALID = "publish", "not_post_create", "no_match", "invalid"


@dataclass
class ParseResult:
    outcome: str
    event: SocialPostEvent | None = None
    reason: str | None = None


def _parse_ts(v: Any) -> datetime | None:
    if not isinstance(v, str):
        return None
    try:
        d = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    return d.astimezone(timezone.utc) if d.tzinfo else None


def parse_message(raw: str | bytes | dict, matcher: EntityMatcher, ingested_at: datetime,
                  source: str = "bluesky_jetstream", data_mode: DataMode = DataMode.LIVE) -> ParseResult:
    try:
        msg = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except (ValueError, UnicodeDecodeError):
        return ParseResult(INVALID, reason="unparseable_json")
    if not isinstance(msg, dict):
        return ParseResult(INVALID, reason="not_an_object")
    if msg.get("kind") != "commit":
        return ParseResult(NOT_POST_CREATE)                  # identity/account events
    c = msg.get("commit")
    if not isinstance(c, dict) or c.get("operation") != "create" or c.get("collection") != POST_COLLECTION:
        return ParseResult(NOT_POST_CREATE)
    did, rkey, rec, tus = msg.get("did"), c.get("rkey"), c.get("record"), msg.get("time_us")
    if not (isinstance(did, str) and did.startswith("did:") and isinstance(rkey, str) and rkey and isinstance(rec, dict)):
        return ParseResult(INVALID, reason="missing_did_rkey_or_record")
    if not isinstance(tus, int) or tus <= 0:
        return ParseResult(INVALID, reason="invalid_time_us")
    text = rec.get("text")
    if not isinstance(text, str) or not text.strip():
        return ParseResult(INVALID, reason="empty_text")
    matches = matcher.match(text)
    if not matches:
        return ParseResult(NO_MATCH)

    server_time = datetime.fromtimestamp(tus / 1_000_000, tz=timezone.utc)
    claimed = _parse_ts(rec.get("createdAt"))
    created_ok = claimed is not None and claimed <= ingested_at + FUTURE_SKEW
    langs = rec.get("langs")
    lang = langs[0] if isinstance(langs, list) and langs and isinstance(langs[0], str) else None
    uri = f"at://{did}/{POST_COLLECTION}/{rkey}"
    try:
        ev = SocialPostEvent(
            event_id=social_event_id(PLATFORM, uri), source=source, event_time=server_time, ingested_at=ingested_at,
            data_mode=data_mode, matched_entities=matched_ids(matches),
            source_metadata={"platform": PLATFORM, "collection": POST_COLLECTION, "rkey": rkey,
                             "cid": str(c.get("cid") or ""), "created_at_source": "record" if created_ok else "time_us",
                             "match_terms": ",".join(sorted({h for m in matches for h in m.hits}))[:200]},
            platform_post_id=uri, author_id=did, text=text[:MAX_TEXT],
            created_at=claimed if created_ok else server_time, language=lang,
            relevance_score=max(m.score for m in matches), post_url=f"https://bsky.app/profile/{did}/post/{rkey}")
    except ValueError as e:
        return ParseResult(INVALID, reason=f"contract_violation: {str(e)[:120]}")
    return ParseResult(PUBLISH, event=ev)
