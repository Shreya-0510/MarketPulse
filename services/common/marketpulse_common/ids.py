"""Deterministic event identity. Never use random IDs for events that must dedupe across retries."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PREFIXES = ("utm_",)
_TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "cmpid", "ocid"}


def _digest(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def _utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def market_event_id(provider: str, symbol: str, interval: str, bar_time: datetime) -> str:
    """Identity = provider + symbol + interval + bar timestamp (UTC, second precision)."""
    return "mkt_" + _digest(provider.lower(), symbol.upper(), interval, _utc_iso(bar_time))


def social_event_id(platform: str, post_uri: str) -> str:
    """Identity = platform + stable post identifier (Bluesky: AT URI)."""
    if not post_uri:
        raise ValueError("post_uri required")
    return "soc_" + _digest(platform.lower(), post_uri)


def canonicalize_url(url: str) -> str:
    """Lowercase scheme/host, drop fragment and tracking params, sort query, trim trailing slash."""
    p = urlsplit(url.strip())
    query = sorted(
        (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
        if not k.lower().startswith(_TRACKING_PREFIXES) and k.lower() not in _TRACKING_KEYS
    )
    path = p.path.rstrip("/") if p.path not in ("", "/") else p.path
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), path, urlencode(query), ""))


def news_event_id(url: str | None, publisher: str, title: str) -> str:
    """Identity = canonical URL; fallback to publisher + normalized title when URL is missing."""
    if url:
        return "new_" + _digest("url", canonicalize_url(url))
    return "new_" + _digest("title", publisher.lower(), " ".join(title.lower().split()))
