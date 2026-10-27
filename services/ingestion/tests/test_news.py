import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from marketpulse_common.config import load_config
from marketpulse_common.matching import EntityMatcher
from mp_ingestion.news import FetchError, Fetcher, clean_text, google_news_urls, parse_feed, search_terms
from mp_ingestion.run_news import poll_source

CFG = load_config(Path(__file__).resolve().parents[3] / "config")
M = EntityMatcher(CFG.entities)
NOW = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
feedparser = pytest.importorskip("feedparser")

RSS = b"""<?xml version="1.0"?><rss version="2.0"><channel><title>t</title>
<item><title>Nvidia shares jump &amp; record</title><link>https://ex.com/a?utm_source=x</link>
<description>&lt;p&gt;NVDA   rallies on AI demand&lt;/p&gt;</description><pubDate>Tue, 27 Oct 2026 10:00:00 GMT</pubDate></item>
<item><title>Same story</title><link>https://ex.com/a/</link><pubDate>Tue, 27 Oct 2026 10:00:00 GMT</pubDate></item>
<item><title>No date, no match</title><link>https://ex.com/b</link></item>
<item><link>https://ex.com/c</link></item></channel></rss>"""
ATOM = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>t</title>
<entry><title>HDFC Bank results</title><link href="https://ex.com/h"/><id>urn:1</id>
<updated>2026-10-27T09:30:00+05:30</updated><summary>Quarterly update</summary></entry></feed>"""
OPTS = {"publisher": "ExPub"}


def parse(b, opts=OPTS):
    return parse_feed(b, source="ex", options=opts, matcher=M, ingested_at=NOW, feed_url="https://ex.com/rss")


def test_rss_normalization_dedupe_and_quality():
    r = parse(RSS)
    assert r.entries == 4 and len(r.events) == 2 and r.duplicates == 1 and r.rejected == ["missing_title"]
    a, b = r.events
    assert a.title == "Nvidia shares jump & record" and a.summary == "NVDA rallies on AI demand"
    assert a.matched_entities == ["nvda"] and a.publisher == "ExPub" and a.publisher_verified
    assert a.published_at == datetime(2026, 10, 27, 10, 0, tzinfo=timezone.utc) and a.timestamp_quality == "source"
    assert a.data_mode.value == "polled" and a.event_id.startswith("new_")
    assert b.published_at is None and b.timestamp_quality == "missing" and b.matched_entities == []
    assert b.ingested_at == NOW                       # ingestion time kept separate
    assert r.matched == 1


def test_atom_timezone_converted_to_utc():
    e = parse(ATOM).events[0]
    assert e.published_at == datetime(2026, 10, 27, 4, 0, tzinfo=timezone.utc) and e.matched_entities == ["hdfcbank"]


def test_garbage_feed_reported_not_raised():
    r = parse(b"<html>not a feed")
    assert r.events == [] and r.bozo


def test_aggregator_publisher_not_verified():
    p = SimpleNamespace(bozo=False, entries=[{"title": "Infosys wins deal - Reuters", "link": "https://news.google.com/x",
                                              "source": {"title": "Reuters"}}], get=lambda k, d=None: None)
    e = parse_feed(b"", source="gn", options={"aggregator": True}, matcher=M, ingested_at=NOW, feed_url="u", parsed=p).events[0]
    assert e.publisher == "Reuters" and e.publisher_verified is False and e.source_metadata["publisher_inferred"] == "true"


def test_clean_text_and_google_urls():
    assert clean_text("<b>a</b>  &amp; b", 10) == "a & b" and clean_text("", 5) is None and len(clean_text("x" * 50, 5)) == 5
    terms = search_terms(CFG.entities)
    assert "aapl" not in terms or terms["aapl"] == "Apple Inc"
    assert "nvda" in terms and "$" not in terms["nvda"]
    u = google_news_urls("https://news.google.com/rss/search", {"nvda": "NVIDIA"})["nvda"]
    assert "q=NVIDIA+when%3A1d" in u


class FakeResp:
    def __init__(self, code, body=b"", headers=None): self.status_code, self.content, self.headers = code, body, headers or {}


class FakeClient:
    def __init__(self, resps): self.resps, self.calls = list(resps), []
    def get(self, url, headers, timeout):
        self.calls.append(headers)
        r = self.resps.pop(0)
        if isinstance(r, Exception): raise r
        return r


def test_conditional_get_uses_etag_and_handles_304_and_errors():
    c = FakeClient([FakeResp(200, b"x", {"ETag": "E1", "Last-Modified": "LM"}), FakeResp(304), FakeResp(429, headers={"Retry-After": "7"}), TimeoutError("t")])
    f = Fetcher(client=c)
    assert f.fetch("u", 5) == (200, b"x") and f.fetch("u", 5) == (304, None)
    assert c.calls[1] == {"If-None-Match": "E1", "If-Modified-Since": "LM"}
    with pytest.raises(FetchError) as ei:
        f.fetch("u", 5)
    assert ei.value.status == 429 and ei.value.retry_after == 7.0
    with pytest.raises(FetchError):
        f.fetch("u", 5)


class Pub:
    def __init__(self): self.sent, self.acked, self.failed, self.last_error = [], 0, 0, None
    def publish(self, t, k, v): self.sent.append((t, k, v))
    def flush(self): self.acked = len(self.sent); return self.acked, self.failed


class DB(dict):
    def __missing__(self, k):
        self[k] = SimpleNamespace(calls=[], update_one=lambda *a, **kw: self[k].calls.append(a)); return self[k]


class FakeFetcher:
    def __init__(self, outcomes): self.o = list(outcomes)
    def fetch(self, url, timeout):
        r = self.o.pop(0)
        if isinstance(r, Exception): raise r
        return r


def src(name="cnbc_top"):
    return next(s for s in CFG.sources if s.name == name)


def test_poll_publishes_once_then_skips_seen_and_records_status():
    pub, db, seen = Pub(), DB(), set()
    f = FakeFetcher([(200, RSS), (200, RSS)])
    r1 = poll_source(src(), CFG, f, pub, M, seen, db, now_fn=lambda: NOW)
    r2 = poll_source(src(), CFG, f, pub, M, seen, db, now_fn=lambda: NOW)
    assert r1["state"] == "OK" and r1["counters"]["published"] == 2 and r2["counters"]["published"] == 0
    assert r2["counters"]["already_seen"] == 2 and len(pub.sent) == 2
    assert pub.sent[0][0] == "news.raw" and pub.sent[0][1] == pub.sent[0][2]["event_id"]
    name, upd = db["pipeline_status"].calls[-1][0], db["pipeline_status"].calls[-1][1]
    assert name == {"component": "ingestion", "name": "cnbc_top"} and upd["$set"]["last_success_at"] == NOW


def test_poll_retries_then_fails_without_raising_and_not_modified_is_ok():
    sleeps = []
    db = DB()
    f = FakeFetcher([FetchError("HTTP 503", 503)] * 3)
    r = poll_source(src(), CFG, f, Pub(), M, set(), db, sleep=sleeps.append, now_fn=lambda: NOW)
    assert r["state"] == "FAILED" and sleeps == [2, 4] and "HTTP 503" in r["errors"][0]
    upd = db["pipeline_status"].calls[-1][1]
    assert "last_success_at" not in upd["$set"] and upd["$set"]["last_http_status"] == 503
    r = poll_source(src(), CFG, FakeFetcher([(304, None)]), Pub(), M, set(), db, now_fn=lambda: NOW)
    assert r["state"] == "OK" and r["counters"]["not_modified"] == 1


def test_one_source_failing_does_not_affect_another():
    db, pub = DB(), Pub()
    bad = poll_source(src("cnbc_top"), CFG, FakeFetcher([FetchError("boom")] * 3), pub, M, set(), db, now_fn=lambda: NOW)
    good = poll_source(src("livemint_news"), CFG, FakeFetcher([(200, ATOM)]), pub, M, set(), db, now_fn=lambda: NOW)
    assert bad["state"] == "FAILED" and good["state"] == "OK" and len(pub.sent) == 1
