from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from marketpulse_common.events import DataMode, MarketBarEvent, NewsArticleEvent, SocialPostEvent

NOW = datetime(2025, 1, 3, tzinfo=timezone.utc)


def bar(**kw):
    base = dict(event_id="mkt_12345678", source="yfinance", event_time=NOW, ingested_at=NOW,
                data_mode=DataMode.HISTORICAL, symbol="AAPL", entity_id="aapl", market="US",
                currency="USD", interval="1d", open=1, high=3, low=1, close=2, volume=10)
    base.update(kw)
    return MarketBarEvent(**base)


def test_bar_valid_and_defaults_entity_ids():
    b = bar()
    assert b.entity_ids == ["aapl"] and b.schema_version == "1.0"


def test_bar_normalizes_to_utc():
    ist = timezone(timedelta(hours=5, minutes=30))
    assert bar(event_time=NOW.astimezone(ist)).event_time.tzinfo == timezone.utc


@pytest.mark.parametrize("kw", [dict(event_time=None), dict(data_mode=DataMode.LIVE), dict(close=0),
                                dict(high=0.5, low=1), dict(volume=-1),
                                dict(event_time=datetime(2025, 1, 1)), dict(bogus=1)])
def test_bar_invalid(kw):
    with pytest.raises((ValidationError, ValueError)):
        bar(**kw)


def test_bar_preserves_missing():
    b = bar(open=None, volume=None)
    assert b.open is None and b.volume is None


def test_social_entities_sync_and_conflict():
    kw = dict(event_id="soc_12345678", source="bluesky", ingested_at=NOW, data_mode=DataMode.LIVE,
              platform_post_id="at://x", author_id="did:plc:1", text="hi", created_at=NOW)
    assert SocialPostEvent(**kw, matched_entities=["aapl"]).entity_ids == ["aapl"]
    with pytest.raises(ValidationError):
        SocialPostEvent(**kw, matched_entities=["aapl"], entity_ids=["msft"])


def test_news_missing_published_at_never_uses_ingested_at():
    n = NewsArticleEvent(event_id="new_12345678", source="rss", ingested_at=NOW,
                         data_mode=DataMode.POLLED, publisher="X", title="t")
    assert n.published_at is None and n.timestamp_quality == "missing" and n.publisher_verified is False
