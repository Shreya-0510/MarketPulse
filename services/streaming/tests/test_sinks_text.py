from datetime import datetime, timezone

from mp_streaming.sinks_text import news_doc, news_ops, social_doc, social_ops

US = int(datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc).timestamp() * 1_000_000)


def base(**kw):
    r = dict(event_id="soc_abcdef12", schema_version="1.0", event_type="social_post", source="s", entity_ids=["nvda"],
             matched_entities=["nvda"], sentiment_label=None, sentiment_score=None, data_mode="live", source_metadata={"a": "b"},
             event_time_us=US, ingested_at_us=US, processed_at_us=US + 5000, processing_delay_ms=5,
             kafka_topic="social.raw", kafka_partition=1, kafka_offset=9)
    r.update(kw)
    return r


def test_social_doc_and_idempotent_op():
    r = base(platform_post_id="at://x", author_id="did:plc:x", text="t", created_at_us=US, language="en", relevance_score=0.9, post_url="u")
    d = social_doc(r)
    assert d["created_at"] == datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc) and d["kafka"]["offset"] == 9
    op = social_ops([r, r])[0]
    assert op._filter == {"event_id": "soc_abcdef12"} and op._upsert is True and op._doc["$inc"] == {"delivery_count": 1}


def test_news_doc_preserves_missing_published_at():
    r = base(event_id="new_abcdef12", event_type="news_article", data_mode="polled", publisher="P", publisher_verified=None,
             title="T", summary=None, url=None, published_at_us=None, timestamp_quality="missing")
    d = news_doc(r)
    assert d["published_at"] is None and d["timestamp_quality"] == "missing" and d["publisher_verified"] is False
    assert news_ops([r])[0]._filter == {"event_id": "new_abcdef12"}
