from datetime import datetime, timedelta, timezone

import pytest

from marketpulse_common.ids import canonicalize_url, market_event_id, news_event_id, social_event_id

T = datetime(2025, 1, 2, 21, 0, tzinfo=timezone.utc)


def test_market_id_deterministic_and_tz_invariant():
    ist = timezone(timedelta(hours=5, minutes=30))
    a = market_event_id("yfinance", "aapl", "1d", T)
    assert a == market_event_id("yfinance", "AAPL", "1d", T.astimezone(ist))
    assert a != market_event_id("yfinance", "AAPL", "1h", T)
    assert a.startswith("mkt_")


def test_market_id_rejects_naive():
    with pytest.raises(ValueError):
        market_event_id("yfinance", "AAPL", "1d", datetime(2025, 1, 2))


def test_social_id():
    u = "at://did:plc:abc/app.bsky.feed.post/3k"
    assert social_event_id("bluesky", u) == social_event_id("Bluesky", u)
    with pytest.raises(ValueError):
        social_event_id("bluesky", "")


def test_canonicalize_url_strips_tracking():
    assert canonicalize_url("HTTPS://Example.com/a/b/?utm_source=x&id=2&fbclid=z#frag") == "https://example.com/a/b?id=2"


def test_news_id_same_article_same_id_and_fallback():
    a = news_event_id("https://x.com/a?utm_campaign=1", "X", "T")
    assert a == news_event_id("https://x.com/a", "X", "T")
    assert news_event_id(None, "X", "Hello  World") == news_event_id(None, "x", "hello world")
    assert news_event_id(None, "X", "t") != a
