from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import deps, main
from app.repositories.market import MarketRepo, StatusRepo
from app.services.market import change_stats, data_age_hours
from marketpulse_common.config import load_config

CFG = load_config(Path(__file__).resolve().parents[3] / "config")
NOW = datetime.now(timezone.utc)


def bar(sym, days_ago, close, **kw):
    t = (NOW - timedelta(days=days_ago)).replace(microsecond=0)
    d = dict(event_id=f"mkt_{sym}{days_ago:04d}xx", symbol=sym, entity_id=sym.lower(), interval="1d", event_time=t, open=1.0,
             high=2.0, low=0.5, close=close, adjusted_close=None, volume=None, currency="USD", data_mode="historical",
             source="yfinance", ingested_at=t, processed_at=t, processing_delay_ms=5, market="US", source_metadata={},
             kafka={"topic": "market.raw", "partition": 0, "offset": 3}, delivery_count=1)
    d.update(kw)
    return d


class FakeRepo(MarketRepo):
    def __init__(self, bars): self.bars = sorted(bars, key=lambda b: b["event_time"], reverse=True)
    def latest_bars(self, s, i, n): return [b for b in self.bars if b["symbol"] == s][:n]
    def bar_count(self, s, i): return len([b for b in self.bars if b["symbol"] == s])
    def history(self, s, i, start, end, limit):
        sel = [b for b in self.bars if b["symbol"] == s and (not start or b["event_time"] >= start) and (not end or b["event_time"] <= end)]
        return list(reversed(sel[:limit])), len(sel)
    def by_event_id(self, e): return next((b for b in self.bars if b["event_id"] == e), None)


class FakeStatus(StatusRepo):
    def components(self): return [{"component": "ingestion", "name": "yfinance_daily", "state": "OK", "last_success_at": NOW, "totals": {"published": 5}}]
    def ping(self): return True


@pytest.fixture
def client():
    repo = FakeRepo([bar("AAPL", 3, 100.0), bar("AAPL", 2, 110.0), bar("AAPL", 1, 99.0), bar("MSFT", 10, 50.0)])
    main.app.dependency_overrides[deps.get_market_repo] = lambda: repo
    main.app.dependency_overrides[deps.get_status_repo] = lambda: FakeStatus()
    main.app.dependency_overrides[deps.get_config] = lambda: CFG
    yield TestClient(main.app)
    main.app.dependency_overrides.clear()


def test_change_stats_definition_and_safety():
    assert change_stats(110.0, 100.0) == (10.0, 10.0)
    assert change_stats(5.0, 0.0) == (None, None) and change_stats(5.0, None) == (None, None)
    assert data_age_hours(NOW - timedelta(hours=3), NOW) == pytest.approx(3.0)


def test_watchlist_values_status_and_empty_states(client):
    items = {i["symbol"]: i for i in client.get("/api/v1/market/watchlist").json()["items"]}
    a = items["AAPL"]
    assert a["status"] == "OK" and a["latest"]["close"] == 99.0 and a["previous_close"] == 110.0
    assert a["change_pct"] == pytest.approx(-10.0) and a["bars_available"] == 3
    assert items["MSFT"]["status"] == "STALE"            # 10 days old > 4
    assert items["NVDA"]["status"] == "NO_DATA" and items["NVDA"]["latest"] is None   # never fabricated


def test_history_bounds_order_and_truncation(client):
    r = client.get("/api/v1/market/aapl/history?limit=2").json()
    assert r["count"] == 2 and r["total_in_range"] == 3 and r["truncated"] is True
    closes = [b["close"] for b in r["bars"]]
    assert closes == [110.0, 99.0]                       # newest 2, ascending
    assert r["currency"] == "USD" and r["bars"][0]["event_time"].endswith(("Z", "+00:00"))


def test_history_validation_and_errors(client):
    assert client.get("/api/v1/market/ZZZ/history").status_code == 404
    assert client.get("/api/v1/market/AAPL/history?limit=99999").status_code == 422
    assert client.get("/api/v1/market/AAPL/history?interval=1m").status_code == 422
    s, e = NOW.isoformat().replace("+00:00", "Z"), (NOW - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    assert client.get("/api/v1/market/AAPL/history", params={"start": s, "end": e}).status_code == 422


def test_history_empty_state(client):
    r = client.get("/api/v1/market/RELIANCE.NS/history").json()
    assert r["status"] == "NO_DATA" and r["bars"] == [] and r["note"]


def test_trace_returns_kafka_provenance_or_404(client):
    eid = client.get("/api/v1/market/AAPL/history").json()["bars"][0]["event_id"]
    t = client.get(f"/api/v1/trace/{eid}").json()
    assert t["kafka"] == {"topic": "market.raw", "partition": 0, "offset": 3} and t["delivery_count"] == 1
    assert client.get("/api/v1/trace/nope").status_code == 404


def test_sources_status_is_truthful(client):
    s = {x["name"]: x for x in client.get("/api/v1/sources/status").json()}
    assert s["yfinance_daily"]["state"] == "OK" and s["yfinance_daily"]["verification"] == "OBSERVED_SUCCESS"
    assert s["cnbc_top"]["state"] == "NOT_RUN" and s["cnbc_top"]["verification"] == "UNVERIFIED"
    assert s["ft_home"]["state"] == "DISABLED" or s["ft_home"]["state"] == "NOT_IMPLEMENTED"
    assert client.get("/api/v1/pipeline/status").json()["kafka"] == "UNKNOWN"
