from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import deps, main
from app.repositories.text import TextRepo
from marketpulse_common.config import load_config

CFG = load_config(Path(__file__).resolve().parents[3] / "config")
NOW = datetime.now(timezone.utc).replace(microsecond=0)


def common(i, **kw):
    t = NOW - timedelta(minutes=i)
    d = dict(event_id=f"id_{i:08d}", source="s", data_mode="live", entity_ids=["nvda"], event_time=t, ingested_at=t,
             processed_at=t, processing_delay_ms=3, source_metadata={}, kafka={"topic": "x", "partition": 0, "offset": i}, delivery_count=1)
    d.update(kw)
    return d


def soc(i, **kw): return common(i, platform_post_id=f"at://{i}", author_id="did:plc:x", text=f"post {i}", created_at=NOW - timedelta(minutes=i), **kw)
def new(i, **kw):
    defaults = dict(source="cnbc_top", data_mode="polled", publisher="CNBC", title=f"h{i}", published_at=NOW - timedelta(minutes=i))
    defaults.update(kw)
    return common(i, **defaults)


class FakeRepo(TextRepo):
    def __init__(self, docs, tf): self.docs, self.tf = docs, tf
    def list(self, *, entity, source, publisher, sentiment, start, end, limit, offset, include_test=False):
        sel = [d for d in self.docs if (include_test or d["data_mode"] != "test") and (not entity or entity in d["entity_ids"]) and (not source or d["source"] == source)
               and (not publisher or d.get("publisher") == publisher) and (not sentiment or d.get("sentiment_label") == sentiment)
               and (not start or (d[self.tf] and d[self.tf] >= start)) and (not end or (d[self.tf] and d[self.tf] <= end))]
        sel.sort(key=lambda d: d[self.tf] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return sel[offset:offset + limit], len(sel)
    def by_event_id(self, e): return next((d for d in self.docs if d["event_id"] == e), None)


@pytest.fixture
def client():
    s = FakeRepo([soc(1), soc(2, entity_ids=["aapl"]), soc(3, sentiment_label="positive", sentiment_score=0.6)], "created_at")
    n = FakeRepo([new(1), new(5, publisher="Mint", source="livemint_news"), new(9, published_at=None, timestamp_quality="missing", url=None)], "published_at")
    main.app.dependency_overrides[deps.get_social_repo] = lambda: s
    main.app.dependency_overrides[deps.get_news_repo] = lambda: n
    main.app.dependency_overrides[deps.get_config] = lambda: CFG
    yield TestClient(main.app)
    main.app.dependency_overrides.clear()


def test_social_list_filters_pagination_and_content(client):
    r = client.get("/api/v1/social").json()
    assert r["status"] == "OK" and r["total"] == 3 and [i["text"] for i in r["items"]] == ["post 1", "post 2", "post 3"]
    assert r["items"][0]["sentiment_label"] is None and r["items"][0]["kafka"]["offset"] == 1
    assert client.get("/api/v1/social?entity=aapl").json()["total"] == 1
    assert client.get("/api/v1/social?sentiment=positive").json()["items"][0]["sentiment_score"] == 0.6
    p = client.get("/api/v1/social?limit=1&offset=1").json()
    assert p["count"] == 1 and p["total"] == 3 and p["items"][0]["event_id"] == "id_00000002"


def test_news_list_publisher_filter_missing_time_sorted_last_and_excluded_by_window(client):
    r = client.get("/api/v1/news").json()
    assert [i["publisher"] for i in r["items"]] == ["CNBC", "Mint", "CNBC"]
    assert r["items"][2]["published_at"] is None and r["items"][2]["timestamp_quality"] == "missing"
    assert client.get("/api/v1/news?publisher=Mint").json()["total"] == 1
    start = (NOW - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    assert client.get("/api/v1/news", params={"start": start}).json()["total"] == 2


def test_validation_and_empty_states(client):
    assert client.get("/api/v1/social?limit=1000").status_code == 422
    assert client.get("/api/v1/social?sentiment=bullish").status_code == 422
    assert client.get("/api/v1/news?entity=zzz").status_code == 404
    s, e = NOW.isoformat().replace("+00:00", "Z"), (NOW - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    assert client.get("/api/v1/news", params={"start": s, "end": e}).status_code == 422
    r = client.get("/api/v1/social?entity=msft").json()
    assert r["status"] == "NO_DATA" and r["items"] == [] and r["note"]


def test_drilldown_by_event_id(client):
    assert client.get("/api/v1/social/id_00000001").json()["author_id"] == "did:plc:x"
    assert client.get("/api/v1/news/id_00000005").json()["publisher"] == "Mint"
    assert client.get("/api/v1/news/nope").status_code == 404


def test_test_mode_records_hidden_unless_requested():
    s = FakeRepo([soc(1), soc(2, data_mode="test")], "created_at")
    n = FakeRepo([new(1), new(2, data_mode="test")], "published_at")
    main.app.dependency_overrides[deps.get_social_repo] = lambda: s
    main.app.dependency_overrides[deps.get_news_repo] = lambda: n
    c = TestClient(main.app)
    try:
        assert c.get("/api/v1/social").json()["total"] == 1
        assert c.get("/api/v1/social?include_test=true").json()["total"] == 2
        assert c.get("/api/v1/news").json()["total"] == 1
        assert c.get("/api/v1/news?include_test=true").json()["total"] == 2
    finally:
        main.app.dependency_overrides.clear()
