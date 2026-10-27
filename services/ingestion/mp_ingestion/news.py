"""RSS/Atom entry normalization -> NewsArticleEvent, plus conditional-GET fetch. Headlines/summaries/links only (no article scraping).

Decisions: published_at comes only from the feed (published, else updated); missing -> None + timestamp_quality="missing"
(never ingestion time). Summary = HTML-stripped, whitespace-collapsed, <= 500 chars. All articles are kept (matched_entities may be empty).
Publisher: source option `publisher`; for aggregator feeds (options.aggregator) the entry's <source> title is used and
publisher_verified=False (inferred). Identity: canonical URL (ids.news_event_id).
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

from marketpulse_common.events import DataMode, NewsArticleEvent
from marketpulse_common.ids import news_event_id
from marketpulse_common.matching import EntityMatcher, matched_ids

_TAG = re.compile(r"<[^>]+>")
MAX_SUMMARY, MAX_TITLE = 500, 500


class FetchError(RuntimeError):
    def __init__(self, msg: str, status: int | None = None, retry_after: float | None = None):
        super().__init__(msg)
        self.status, self.retry_after = status, retry_after


def clean_text(s: str | None, limit: int) -> str | None:
    if not s:
        return None
    t = " ".join(html.unescape(_TAG.sub(" ", s)).split())
    return t[:limit] or None


def _entry_time(e: Any) -> datetime | None:
    for k in ("published_parsed", "updated_parsed"):
        v = e.get(k)
        if v:
            try:
                return datetime(*v[:6], tzinfo=timezone.utc)   # feedparser normalizes to UTC struct_time
            except (TypeError, ValueError):
                continue
    return None


@dataclass
class FeedResult:
    events: list[NewsArticleEvent] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)
    entries: int = 0
    duplicates: int = 0
    bozo: str | None = None          # feed was malformed but partially parsed
    matched: int = 0


def parse_feed(content: bytes, *, source: str, options: dict, matcher: EntityMatcher, ingested_at: datetime,
               feed_url: str, parsed=None) -> FeedResult:
    """`parsed` allows tests to inject a feedparser-like result; normally feedparser.parse(content)."""
    if parsed is None:
        import feedparser  # lazy
        parsed = feedparser.parse(content)
    res = FeedResult(entries=len(parsed.entries))
    if parsed.get("bozo") and not parsed.entries:
        res.bozo = f"unparseable feed: {type(parsed.get('bozo_exception')).__name__}"
        return res
    if parsed.get("bozo"):
        res.bozo = f"malformed feed (partially parsed): {type(parsed.get('bozo_exception')).__name__}"
    aggregator = bool(options.get("aggregator"))
    default_pub = options.get("publisher") or source
    seen: set[str] = set()
    for e in parsed.entries:
        title = clean_text(e.get("title"), MAX_TITLE)
        link = (e.get("link") or "").strip() or None
        if not title:
            res.rejected.append("missing_title")
            continue
        if link and not link.lower().startswith(("http://", "https://")):
            link = None
        publisher, verified = default_pub, not aggregator
        src_title = (e.get("source") or {}).get("title") if isinstance(e.get("source"), dict) else None
        if aggregator and src_title:
            publisher = src_title
        summary = clean_text(e.get("summary") or e.get("description"), MAX_SUMMARY)
        if aggregator:
            summary = None if summary and title and summary.startswith(title[:40]) else summary  # GN summaries just echo the title
        eid = news_event_id(link, publisher, title)
        if eid in seen:
            res.duplicates += 1
            continue
        seen.add(eid)
        pub = _entry_time(e)
        matches = matcher.match(f"{title}. {summary or ''}")
        try:
            ev = NewsArticleEvent(
                event_id=eid, source=source, event_time=pub, ingested_at=ingested_at, data_mode=DataMode.POLLED,
                matched_entities=matched_ids(matches), publisher=publisher, publisher_verified=verified, title=title,
                summary=summary, url=link, published_at=pub,
                source_metadata={"feed_url": feed_url[:300], "guid": str(e.get("id") or "")[:300],
                                 "publisher_inferred": str(not verified).lower()})
        except ValueError as ex:
            res.rejected.append(f"contract_violation: {str(ex)[:100]}")
            continue
        res.matched += 1 if ev.matched_entities else 0
        res.events.append(ev)
    return res


def google_news_urls(base: str, entity_terms: dict[str, str]) -> dict[str, str]:
    """entity_id -> Google News RSS search URL (last 1 day, en-US). Aggregator: discovery only."""
    return {eid: f"{base}?q={quote_plus(term + ' when:1d')}&hl=en-US&gl=US&ceid=US:en" for eid, term in entity_terms.items()}


def search_terms(entities: dict[str, Any]) -> dict[str, str]:
    """One query term per entity: first strong term that is a name (not a cashtag / dotted symbol / ALL-CAPS ticker)."""
    out = {}
    for eid, r in entities.items():
        for t in r.strong_terms:
            if not t.startswith("$") and "." not in t[1:] and not (t.isupper() and " " not in t):
                out[eid] = t
                break
    return out


class Fetcher:
    """httpx wrapper with conditional GET (ETag / Last-Modified, kept in memory per URL)."""
    def __init__(self, client=None, user_agent: str = "MarketPulse/0.4 (academic project; RSS reader)"):
        if client is None:
            import httpx
            client = httpx.Client(headers={"User-Agent": user_agent, "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"},
                                  follow_redirects=True)
        self.client = client
        self.validators: dict[str, dict[str, str]] = {}

    def fetch(self, url: str, timeout: float) -> tuple[int, bytes | None]:
        """(status, body). 304 -> (304, None). Raises FetchError on network error / 4xx / 5xx."""
        h = {}
        v = self.validators.get(url, {})
        if v.get("etag"):
            h["If-None-Match"] = v["etag"]
        if v.get("last_modified"):
            h["If-Modified-Since"] = v["last_modified"]
        try:
            r = self.client.get(url, headers=h, timeout=timeout)
        except Exception as e:
            raise FetchError(f"{type(e).__name__}: {e}"[:300]) from e
        if r.status_code == 304:
            return 304, None
        if r.status_code >= 400:
            ra = r.headers.get("Retry-After", "")
            raise FetchError(f"HTTP {r.status_code}", r.status_code, float(ra) if ra.isdigit() else None)
        nv = {k: r.headers[hk] for k, hk in (("etag", "ETag"), ("last_modified", "Last-Modified")) if hk in r.headers}
        if nv:
            self.validators[url] = nv
        return r.status_code, r.content
