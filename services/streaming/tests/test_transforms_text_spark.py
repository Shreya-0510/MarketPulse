"""Needs pyspark + Java 17 (pip install -e '.[spark]'); skipped otherwise."""
import json

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from mp_streaming.transforms_text import classify_news, classify_social, valid_news, valid_social  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    try:
        s = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate()
    except Exception as e:  # no JVM
        pytest.skip(f"Spark unavailable: {e}")
    yield s
    s.stop()


def kdf(spark, values):
    return spark.createDataFrame([("t", 0, i, None, v.encode()) for i, v in enumerate(values)],
                                 "topic string, partition int, offset long, timestamp timestamp, value binary")


def soc(**kw):
    d = dict(event_id="soc_abcdef12", schema_version="1.0", event_type="social_post", source="b", event_time="2026-10-27T12:00:00Z",
             ingested_at="2026-10-27T12:00:01Z", data_mode="live", entity_ids=["nvda"], matched_entities=["nvda"], source_metadata={"k": "v"},
             platform_post_id="at://x", author_id="did:plc:x", text="hi $NVDA", created_at="2026-10-27T11:59:59Z", language="en")
    d.update(kw)
    return json.dumps(d)


def test_social_classification_and_dedupe(spark):
    rows = [soc(), soc(), "not json", soc(text=" ", event_id="soc_zzzzzzzz"), soc(data_mode="historical", event_id="soc_yyyyyyyy"),
            soc(event_type="news_article", event_id="soc_wwwwwwww"), soc(sentiment_label="great", event_id="soc_vvvvvvvv")]
    p = classify_social(kdf(spark, rows)).persist()
    errs = {r["offset"]: r["error"] for r in p.select("offset", "error").collect()}
    assert errs == {0: None, 1: None, 2: "unparseable_json", 3: "empty_text", 4: "invalid_data_mode", 5: "wrong_event_type", 6: "invalid_sentiment"}
    v = valid_social(p).collect()
    assert len(v) == 1 and v[0]["event_id"] == "soc_abcdef12" and v[0]["processing_delay_ms"] is not None
    assert v[0]["created_at_us"] == 1793102399000000


def test_news_missing_published_at_is_kept(spark):
    base = dict(event_id="new_abcdef12", schema_version="1.0", event_type="news_article", source="x", event_time=None,
                ingested_at="2026-10-27T12:00:01Z", data_mode="polled", entity_ids=[], matched_entities=[], source_metadata={},
                publisher="P", title="T", published_at=None)
    rows = [json.dumps(base), json.dumps({**base, "event_id": "new_bbbbbbbb", "published_at": "2026-10-27T10:00:00Z"}),
            json.dumps({**base, "event_id": "new_cccccccc", "published_at": "garbage"}), json.dumps({**base, "event_id": "new_dddddddd", "title": ""})]
    p = classify_news(kdf(spark, rows)).persist()
    assert [r["error"] for r in p.orderBy("offset").select("error").collect()] == [None, None, "invalid_published_at", "missing_title"]
    out = {r["event_id"]: r for r in valid_news(p).collect()}
    assert out["new_abcdef12"]["published_at_us"] is None and out["new_abcdef12"]["timestamp_quality"] == "missing"
    assert out["new_bbbbbbbb"]["timestamp_quality"] == "source"
