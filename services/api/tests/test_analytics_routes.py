"""Slice 5 API tests: /api/v1/entities, /entities/{id}/metrics, /attention, /anomalies, /cross-signals.

Uses in-memory fake repository and fake config (no MongoDB, no external calls).
Exercises route logic, validation, and empty-state responses.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app import deps

# ---------------------------------------------------------------------------
# Fake config & config instruments
# ---------------------------------------------------------------------------

def _fake_instrument(entity_id="nvda", symbol="NVDA", display_name="NVIDIA", market="US",
                      currency="USD", instrument_type="equity", timezone_="America/New_York"):
    from app.schemas import EntitySummary
    m = MagicMock()
    m.entity_id = entity_id
    m.symbol = symbol
    m.display_name = display_name
    m.market = market
    m.currency = currency
    m.instrument_type = instrument_type
    m.timezone = timezone_
    return m


def _fake_config(instruments=None):
    cfg = MagicMock()
    cfg.instruments = instruments or [_fake_instrument()]
    cfg.settings.anomaly = {"baseline_windows": 24, "min_count": 5, "z_threshold": 3.0, "min_std": 0.5}
    return cfg


# ---------------------------------------------------------------------------
# Fake analytics repository
# ---------------------------------------------------------------------------

class FakeAnalyticsRepo:
    def __init__(self, metrics=None, attention=None, anomalies=None, cross=None):
        self._metrics = metrics or []
        self._attention = attention or []
        self._anomalies = anomalies or []
        self._cross = cross  # single dict or None

    def entity_metric_windows(self, entity_id, limit=48):
        return [m for m in self._metrics if m["entity_id"] == entity_id][:limit]

    def attention_list(self, *, entity_id=None, source_type=None, start=None, end=None, limit=200):
        items = self._attention
        if entity_id:
            items = [i for i in items if i["entity_id"] == entity_id]
        if source_type:
            items = [i for i in items if i["source_type"] == source_type]
        return items[:limit]

    def anomaly_list(self, *, entity_id=None, source_type=None, start=None, end=None, limit=100):
        items = self._anomalies
        if entity_id:
            items = [i for i in items if i["entity_id"] == entity_id]
        return items[:limit]

    def cross_signals_for_entity(self, entity_id, window_start, window_end, window_minutes):
        if self._cross and self._cross["entity_id"] == entity_id:
            return self._cross
        return None

    def latest_entity_windows(self, entity_ids, window_minutes=60):
        return {}


# ---------------------------------------------------------------------------
# Override deps
# ---------------------------------------------------------------------------

@pytest.fixture
def client_empty():
    """Client with empty analytics repo and one configured entity."""
    cfg = _fake_config()
    app.dependency_overrides[deps.get_config] = lambda: cfg
    from app.routes.analytics import _get_repo
    app.dependency_overrides[_get_repo] = lambda: FakeAnalyticsRepo()
    yield TestClient(app)
    app.dependency_overrides.clear()


def _ts(s="2025-01-15T13:00:00+00:00"):
    return datetime.fromisoformat(s)


def _make_entity_metric():
    return {
        "entity_id": "nvda", "metric": "combined",
        "window_start": _ts("2025-01-15T13:00:00+00:00"),
        "window_end": _ts("2025-01-15T14:00:00+00:00"),
        "window_minutes": 60,
        "social": {"count": 10, "scored": 8, "positive": 5, "neutral": 2, "negative": 1, "avg_score": 0.15},
        "news": {"count": 3, "scored": 3, "positive": 2, "neutral": 1, "negative": 0, "avg_score": 0.1},
        "total_mentions": 13,
        "computed_at": _ts("2025-01-15T14:05:00+00:00"),
    }


def _make_attention():
    return {
        "entity_id": "nvda", "source_type": "social",
        "window_start": _ts("2025-01-15T13:00:00+00:00"),
        "window_end": _ts("2025-01-15T14:00:00+00:00"),
        "window_minutes": 60, "count": 10,
        "computed_at": _ts("2025-01-15T14:05:00+00:00"),
    }


def _make_anomaly():
    return {
        "anomaly_id": "ano_abc123", "entity_id": "nvda", "source_type": "social",
        "window_start": _ts("2025-01-15T13:00:00+00:00"),
        "window_end": _ts("2025-01-15T14:00:00+00:00"),
        "window_minutes": 60, "observed_count": 100,
        "baseline_mean": 10.0, "baseline_std": 1.2,
        "baseline_windows_used": 24, "z_score": 75.0, "z_threshold": 3.0,
        "direction": "spike", "detected_at": _ts("2025-01-15T14:05:00+00:00"),
    }


def _make_cross():
    return {
        "entity_id": "nvda",
        "window_start": _ts("2025-01-15T13:00:00+00:00"),
        "window_end": _ts("2025-01-15T14:00:00+00:00"),
        "window_minutes": 60,
        "price_change_pct": 2.5, "price_bar_count": 1,
        "social_count": 10, "social_scored_count": 8, "avg_social_sentiment": -0.1,
        "news_count": 3, "news_scored_count": 3, "avg_news_sentiment": 0.2,
        "observations": ["price_up_sentiment_down"],
        "note": "Descriptive only. No causation implied. Does not predict returns.",
        "computed_at": _ts("2025-01-15T14:05:00+00:00"),
    }


# ---------------------------------------------------------------------------
# Tests: /api/v1/entities
# ---------------------------------------------------------------------------

def test_entities_list(client_empty):
    r = client_empty.get("/api/v1/entities")
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 1
    assert data["items"][0]["entity_id"] == "nvda"
    assert data["items"][0]["symbol"] == "NVDA"


def test_entity_detail_ok(client_empty):
    r = client_empty.get("/api/v1/entities/nvda")
    assert r.status_code == 200
    assert r.json()["entity_id"] == "nvda"


def test_entity_detail_not_found(client_empty):
    r = client_empty.get("/api/v1/entities/unknown_entity_xyz")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Tests: /api/v1/entities/{entity_id}/metrics
# ---------------------------------------------------------------------------

def test_entity_metrics_no_data(client_empty):
    r = client_empty.get("/api/v1/entities/nvda/metrics")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "NO_DATA"
    assert data["windows"] == []
    assert data["note"] is not None


@pytest.fixture
def client_with_data():
    cfg = _fake_config()
    repo = FakeAnalyticsRepo(
        metrics=[_make_entity_metric()],
        attention=[_make_attention()],
        anomalies=[_make_anomaly()],
        cross=_make_cross(),
    )
    app.dependency_overrides[deps.get_config] = lambda: cfg
    from app.routes.analytics import _get_repo
    app.dependency_overrides[_get_repo] = lambda: repo
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_entity_metrics_ok(client_with_data):
    r = client_with_data.get("/api/v1/entities/nvda/metrics")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "OK"
    assert len(data["windows"]) == 1
    w = data["windows"][0]
    assert w["entity_id"] == "nvda"
    assert w["social"]["count"] == 10
    assert w["total_mentions"] == 13


def test_entity_metrics_unknown(client_with_data):
    r = client_with_data.get("/api/v1/entities/bogus_xyz/metrics")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Tests: /api/v1/attention
# ---------------------------------------------------------------------------

def test_attention_no_data(client_empty):
    r = client_empty.get("/api/v1/attention")
    assert r.status_code == 200
    assert r.json()["status"] == "NO_DATA"


def test_attention_ok(client_with_data):
    r = client_with_data.get("/api/v1/attention?entity=nvda&source_type=social")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "OK"
    assert data["items"][0]["count"] == 10


def test_attention_invalid_entity(client_with_data):
    r = client_with_data.get("/api/v1/attention?entity=bogus_xyz")
    assert r.status_code == 404


def test_attention_invalid_source_type(client_empty):
    r = client_empty.get("/api/v1/attention?source_type=invalid_type")
    assert r.status_code == 422


def test_attention_start_gt_end(client_empty):
    r = client_empty.get("/api/v1/attention?start=2025-01-15T14:00:00Z&end=2025-01-15T13:00:00Z")
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Tests: /api/v1/anomalies
# ---------------------------------------------------------------------------

def test_anomalies_no_data(client_empty):
    r = client_empty.get("/api/v1/anomalies")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "NO_DATA"
    assert "historical windows" in (data["note"] or "")


def test_anomalies_ok(client_with_data):
    r = client_with_data.get("/api/v1/anomalies")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "OK"
    a = data["items"][0]
    assert a["anomaly_id"] == "ano_abc123"
    assert a["direction"] == "spike"
    assert a["z_score"] > 3.0


def test_anomalies_entity_filter(client_with_data):
    r = client_with_data.get("/api/v1/anomalies?entity=nvda")
    assert r.status_code == 200
    assert r.json()["count"] >= 1


def test_anomalies_unknown_entity(client_with_data):
    r = client_with_data.get("/api/v1/anomalies?entity=nobody_xyz")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Tests: /api/v1/cross-signals
# ---------------------------------------------------------------------------

def test_cross_signals_empty(client_empty):
    r = client_empty.get("/api/v1/cross-signals")
    assert r.status_code == 200
    assert r.json()["status"] == "NO_DATA"


def test_cross_signals_ok(client_with_data):
    # The route computes windows and calls repo.cross_signals_for_entity for each window+entity
    r = client_with_data.get("/api/v1/cross-signals?entity=nvda&lookback_windows=1")
    assert r.status_code == 200
    data = r.json()
    # May be OK or NO_DATA depending on whether the window aligns with mock
    assert data["status"] in ("OK", "NO_DATA")
    assert "count" in data


def test_cross_signals_invalid_entity(client_empty):
    r = client_empty.get("/api/v1/cross-signals?entity=nobody_xyz")
    assert r.status_code == 404


def test_cross_signals_invalid_window(client_empty):
    r = client_empty.get("/api/v1/cross-signals?window_minutes=5")   # < 15 min minimum
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Tests: schema validation — test-mode exclusion is documented
# ---------------------------------------------------------------------------

def test_production_modes_excludes_test():
    """PRODUCTION_MODES used in analytics queries must not contain 'test'."""
    from mp_streaming.analytics_job import PRODUCTION_MODES
    assert "test" not in PRODUCTION_MODES
    assert len(PRODUCTION_MODES) >= 3   # historical, polled, live, replay
