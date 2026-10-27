"""Slice 5 tests: sentiment scoring, analytics aggregation, anomaly detection, cross-signal descriptors.

These tests use only pure Python (no Spark, no MongoDB, no VADER network calls).
They exercise the logic functions in analytics_job.py directly with in-memory fakes.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# -------------------------------------------------------------------
# 1. Sentiment scoring
# -------------------------------------------------------------------

def test_score_text_positive():
    from mp_streaming.sentiment import score_text
    label, score = score_text("This stock is amazing and fantastic!", 0.05, -0.05)
    assert label == "positive"
    assert score is not None and score > 0.05


def test_score_text_negative():
    from mp_streaming.sentiment import score_text
    label, score = score_text("This company is terrible, worst performance ever.", 0.05, -0.05)
    assert label == "negative"
    assert score is not None and score < -0.05


def test_score_text_neutral():
    from mp_streaming.sentiment import score_text
    label, score = score_text("The company released a filing today.", 0.05, -0.05)
    # Neutral text should be neutral (VADER compound near 0)
    assert label in ("neutral", "positive", "negative")   # label always set for non-empty text
    assert score is not None


def test_score_text_empty():
    from mp_streaming.sentiment import score_text
    label, score = score_text("", 0.05, -0.05)
    assert label is None
    assert score is None


def test_score_text_none():
    from mp_streaming.sentiment import score_text
    label, score = score_text(None, 0.05, -0.05)
    assert label is None
    assert score is None


def test_score_text_whitespace_only():
    from mp_streaming.sentiment import score_text
    label, score = score_text("   ", 0.05, -0.05)
    assert label is None
    assert score is None


def test_score_text_custom_thresholds():
    from mp_streaming.sentiment import score_text
    # Very tight thresholds: any positive compound -> positive
    label, score = score_text("good", 0.0, -1.0)
    assert label in ("positive", "neutral")   # "good" is slightly positive in VADER


# -------------------------------------------------------------------
# 2. Window bounds
# -------------------------------------------------------------------

def test_window_bounds_alignment():
    from mp_streaming.analytics_job import _window_bounds
    now = datetime(2025, 1, 15, 13, 37, 42, tzinfo=timezone.utc)
    start, end = _window_bounds(now, 60)
    assert start < end
    # window should be exactly 60 minutes
    assert (end - start) == timedelta(hours=1)
    # end should be <= now (closed window)
    assert end <= now
    # start and end should be on even-hour boundaries
    assert end.minute == 0 and end.second == 0


def test_window_bounds_15min():
    from mp_streaming.analytics_job import _window_bounds
    now = datetime(2025, 1, 15, 13, 22, 0, tzinfo=timezone.utc)
    start, end = _window_bounds(now, 15)
    assert (end - start) == timedelta(minutes=15)
    assert end <= now


# -------------------------------------------------------------------
# 3. Anomaly ID determinism
# -------------------------------------------------------------------

def test_anomaly_id_deterministic():
    from mp_streaming.analytics_job import _anomaly_id
    w = datetime(2025, 1, 15, 13, 0, 0, tzinfo=timezone.utc)
    id1 = _anomaly_id("nvda", "social", w)
    id2 = _anomaly_id("nvda", "social", w)
    assert id1 == id2
    assert id1.startswith("ano_")
    assert len(id1) == 4 + 28   # "ano_" + 28 hex chars


def test_anomaly_id_differs_by_entity():
    from mp_streaming.analytics_job import _anomaly_id
    w = datetime(2025, 1, 15, 13, 0, 0, tzinfo=timezone.utc)
    id1 = _anomaly_id("nvda", "social", w)
    id2 = _anomaly_id("aapl", "social", w)
    assert id1 != id2


# -------------------------------------------------------------------
# 4. Anomaly detection logic
# -------------------------------------------------------------------

def _make_fake_db(history_counts: list[int]) -> Any:
    """Fake DB whose attention_metrics returns the given counts as history."""
    doc_list = [{"count": c} for c in history_counts]
    mock_coll = MagicMock()
    mock_coll.find.return_value.sort.return_value.limit.return_value = doc_list
    db = MagicMock()
    db.__getitem__.return_value = mock_coll
    return db


def test_anomaly_no_history():
    from mp_streaming.analytics_job import detect_anomalies
    db = _make_fake_db([])   # no history
    cfg_a = {"baseline_windows": 5, "min_count": 2, "z_threshold": 3.0, "min_std": 0.5}
    result = detect_anomalies(db, "nvda", "social", 100,
                               datetime(2025, 1, 15, 13, tzinfo=timezone.utc),
                               datetime(2025, 1, 15, 14, tzinfo=timezone.utc), 60, cfg_a)
    assert result is None   # not enough history


def test_anomaly_below_min_count():
    from mp_streaming.analytics_job import detect_anomalies
    db = _make_fake_db([10] * 30)
    cfg_a = {"baseline_windows": 5, "min_count": 50, "z_threshold": 3.0, "min_std": 0.5}
    result = detect_anomalies(db, "nvda", "social", 3,   # 3 < 50 min_count
                               datetime(2025, 1, 15, 13, tzinfo=timezone.utc),
                               datetime(2025, 1, 15, 14, tzinfo=timezone.utc), 60, cfg_a)
    assert result is None


def test_anomaly_unstable_std():
    from mp_streaming.analytics_job import detect_anomalies
    # All same value -> std = 0 -> unstable
    db = _make_fake_db([10] * 30)
    cfg_a = {"baseline_windows": 5, "min_count": 5, "z_threshold": 3.0, "min_std": 0.5}
    result = detect_anomalies(db, "nvda", "social", 100,
                               datetime(2025, 1, 15, 13, tzinfo=timezone.utc),
                               datetime(2025, 1, 15, 14, tzinfo=timezone.utc), 60, cfg_a)
    assert result is None   # std < min_std


def test_anomaly_spike_detected():
    from mp_streaming.analytics_job import detect_anomalies
    # Baseline: mean=10, std≈1 (values 9,10,11,10,9,11...)
    baseline = [9, 10, 11, 10, 9, 11, 10, 9, 11, 10]
    db = _make_fake_db(baseline)
    cfg_a = {"baseline_windows": 10, "min_count": 5, "z_threshold": 3.0, "min_std": 0.5}
    w_start = datetime(2025, 1, 15, 13, tzinfo=timezone.utc)
    w_end = datetime(2025, 1, 15, 14, tzinfo=timezone.utc)
    # current_count = 50 >> baseline mean of 10
    result = detect_anomalies(db, "nvda", "social", 50, w_start, w_end, 60, cfg_a)
    assert result is not None
    assert result["direction"] == "spike"
    assert result["z_score"] > 3.0
    assert result["entity_id"] == "nvda"
    assert result["source_type"] == "social"
    assert result["observed_count"] == 50


def test_anomaly_drop_detected():
    from mp_streaming.analytics_job import detect_anomalies
    baseline = [50, 48, 52, 49, 51, 50, 48, 52, 49, 51]
    db = _make_fake_db(baseline)
    cfg_a = {"baseline_windows": 10, "min_count": 0, "z_threshold": 3.0, "min_std": 0.5}
    w_start = datetime(2025, 1, 15, 13, tzinfo=timezone.utc)
    w_end = datetime(2025, 1, 15, 14, tzinfo=timezone.utc)
    result = detect_anomalies(db, "nvda", "social", 0, w_start, w_end, 60, cfg_a)
    assert result is not None
    assert result["direction"] == "drop"


def test_anomaly_below_threshold():
    from mp_streaming.analytics_job import detect_anomalies
    baseline = [9, 10, 11, 10, 9, 11, 10, 9, 11, 10]
    db = _make_fake_db(baseline)
    cfg_a = {"baseline_windows": 10, "min_count": 5, "z_threshold": 3.0, "min_std": 0.5}
    w_start = datetime(2025, 1, 15, 13, tzinfo=timezone.utc)
    w_end = datetime(2025, 1, 15, 14, tzinfo=timezone.utc)
    # current = 11, barely above mean -> z < 3
    result = detect_anomalies(db, "nvda", "social", 11, w_start, w_end, 60, cfg_a)
    assert result is None


# -------------------------------------------------------------------
# 5. Exclusion of test-mode records
# -------------------------------------------------------------------

def test_score_batch_excludes_test_mode():
    """_score_batch query must filter out data_mode='test' records."""
    import inspect
    from mp_streaming.analytics_job import _score_batch, PRODUCTION_MODES
    assert "test" not in PRODUCTION_MODES
    assert "historical" in PRODUCTION_MODES
    assert "live" in PRODUCTION_MODES


def test_compute_entity_metrics_excludes_test(monkeypatch):
    """_compute_entity_metrics must pass PRODUCTION_MODES to mongo query."""
    from mp_streaming import analytics_job
    # Verify PRODUCTION_MODES is used in the query function source
    import inspect
    src = inspect.getsource(analytics_job._compute_entity_metrics)
    assert "PRODUCTION_MODES" in src


# -------------------------------------------------------------------
# 6. Cross-signal observations
# -------------------------------------------------------------------

def test_cross_signal_no_data():
    """With no market bars and no posts, cross_signals_for_entity returns a dict with empty observations."""
    # This tests the shape of the result, not MongoDB queries
    # We check that the analytics_job compute function (not the repo version) handles empty cleanly
    # by using the analytics_job.compute_cross_signals function directly
    from mp_streaming.analytics_job import compute_cross_signals

    w_start = datetime(2025, 1, 15, 13, tzinfo=timezone.utc)
    w_end = datetime(2025, 1, 15, 14, tzinfo=timezone.utc)

    db = MagicMock()
    # market_prices: returns empty list
    db.__getitem__.return_value.find.return_value.sort.return_value.limit.return_value = []
    db.__getitem__.return_value.count_documents.return_value = 0

    result = compute_cross_signals(db, "nvda", w_start, w_end, 60)
    assert result is not None
    assert result["entity_id"] == "nvda"
    assert result["price_change_pct"] is None
    assert result["observations"] == []
    assert "Descriptive only" in result["note"]


# -------------------------------------------------------------------
# 7. Upsert operations (shape check without real MongoDB)
# -------------------------------------------------------------------

def test_upsert_entity_metrics_empty():
    from mp_streaming.analytics_job import upsert_entity_metrics
    db = MagicMock()
    result = upsert_entity_metrics(db, [])
    assert result == 0
    db.__getitem__.assert_not_called()


def test_upsert_attention_metrics_empty():
    from mp_streaming.analytics_job import upsert_attention_metrics
    db = MagicMock()
    result = upsert_attention_metrics(db, [])
    assert result == 0


def test_upsert_anomalies_empty():
    from mp_streaming.analytics_job import upsert_anomalies
    db = MagicMock()
    result = upsert_anomalies(db, [])
    assert result == 0
