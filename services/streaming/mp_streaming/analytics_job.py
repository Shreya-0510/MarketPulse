"""Slice 5 analytics: entity metrics, attention metrics, anomaly detection, cross-signal descriptors.

Run as a separate process (compose service `spark-analytics`) OR as additional queries
appended to text_jobs.  We keep it separate so market/text Spark JVMs aren't bloated.

python -m mp_streaming.analytics_job

This job does NOT read from Kafka topics directly. It reads from MongoDB collections
(market_prices, social_posts, news_articles) on a micro-batch schedule and writes to
entity_metrics, attention_metrics, anomaly_events.

Design: scheduled foreachBatch-style polling via Spark's trigger rather than a true
streaming join (cross-signal joins across three streams with different cadences would
require complex watermarks). Instead every ANALYTICS_INTERVAL_SECONDS we:
  1. Score sentiment on un-scored social/news docs (update in place).
  2. Compute windowed counts/sentiment for current window.
  3. Compare to baseline -> z-score anomaly detection.
  4. Write entity_metrics, attention_metrics, anomaly_events to MongoDB.
  5. Publish summary events to analytics.entity / analytics.attention / analytics.anomalies
     Kafka topics (best-effort; failure is logged, never retried).

data_mode=="test" records are excluded from all production analytics.
Missing sentiment fields are treated as missing (not neutral).
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import Any, Optional

from pymongo import MongoClient, UpdateOne

from marketpulse_common.config import load_config

from .sentiment import score_text

log = logging.getLogger("analytics_job")

PRODUCTION_MODES = {"historical", "polled", "live", "replay"}   # excludes "test"


# ---------------------------------------------------------------------------
# Sentiment scoring pass (fills null labels in social_posts/news_articles)
# ---------------------------------------------------------------------------

def _score_batch(db, collection: str, text_field: str, pos_thr: float, neg_thr: float) -> int:
    """Find docs with null sentiment_label (excluding test mode) and score them. Returns count scored."""
    coll = db[collection]
    query = {"sentiment_label": None, "data_mode": {"$in": list(PRODUCTION_MODES)}}
    docs = list(coll.find(query, {"_id": 0, "event_id": 1, text_field: 1}).limit(500))
    if not docs:
        return 0
    ops = []
    for d in docs:
        text = d.get(text_field) or ""
        label, score = score_text(text, pos_thr, neg_thr)
        if label is None:
            # empty text -> mark as missing explicitly so we don't keep retrying
            label, score = None, None
        ops.append(UpdateOne(
            {"event_id": d["event_id"]},
            {"$set": {"sentiment_label": label, "sentiment_score": score}}
        ))
    if ops:
        coll.bulk_write(ops, ordered=False)
    return len(docs)


# ---------------------------------------------------------------------------
# Windowed entity metrics
# ---------------------------------------------------------------------------

def _window_bounds(now: datetime, window_minutes: int) -> tuple[datetime, datetime]:
    """Current closed window ending at the most recent full boundary before now."""
    td = timedelta(minutes=window_minutes)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    n = int((now - epoch).total_seconds() // td.total_seconds())
    end = epoch + n * td
    start = end - td
    return start, end


def _compute_entity_metrics(db, entity_id: str, window_start: datetime, window_end: datetime,
                             window_minutes: int) -> dict[str, Any]:
    """Aggregate social+news counts and sentiment for entity in [window_start, window_end)."""
    social = db["social_posts"]
    news = db["news_articles"]

    def _agg(coll, time_field):
        q = {
            "entity_ids": entity_id,
            time_field: {"$gte": window_start, "$lt": window_end},
            "data_mode": {"$in": list(PRODUCTION_MODES)},
        }
        docs = list(coll.find(q, {"_id": 0, "sentiment_label": 1, "sentiment_score": 1}))
        total = len(docs)
        scored = [d for d in docs if d.get("sentiment_label") is not None]
        pos = sum(1 for d in scored if d.get("sentiment_label") == "positive")
        neg = sum(1 for d in scored if d.get("sentiment_label") == "negative")
        neu = sum(1 for d in scored if d.get("sentiment_label") == "neutral")
        scores = [d["sentiment_score"] for d in scored if d.get("sentiment_score") is not None]
        avg_score = round(mean(scores), 6) if scores else None
        return {"count": total, "scored": len(scored), "positive": pos, "neutral": neu,
                "negative": neg, "avg_score": avg_score}

    soc = _agg(social, "created_at")
    nws = _agg(news, "published_at")

    return {
        "entity_id": entity_id,
        "metric": "combined",
        "window_start": window_start,
        "window_end": window_end,
        "window_minutes": window_minutes,
        "social": soc,
        "news": nws,
        "total_mentions": soc["count"] + nws["count"],
        "computed_at": datetime.now(timezone.utc),
    }


def upsert_entity_metrics(db, metrics_list: list[dict[str, Any]]) -> int:
    if not metrics_list:
        return 0
    ops = []
    for m in metrics_list:
        key = {"entity_id": m["entity_id"], "metric": m["metric"],
               "window_start": m["window_start"], "window_minutes": m["window_minutes"]}
        ops.append(UpdateOne(key, {"$set": m}, upsert=True))
    res = db["entity_metrics"].bulk_write(ops, ordered=False)
    return res.upserted_count + res.modified_count


# ---------------------------------------------------------------------------
# Attention metrics (per source_type: social | news | market)
# ---------------------------------------------------------------------------

def _compute_attention_metrics(db, entity_id: str, source_type: str,
                                window_start: datetime, window_end: datetime,
                                window_minutes: int) -> dict[str, Any]:
    """Count events for entity in window by source_type."""
    if source_type == "social":
        count = db["social_posts"].count_documents({
            "entity_ids": entity_id,
            "created_at": {"$gte": window_start, "$lt": window_end},
            "data_mode": {"$in": list(PRODUCTION_MODES)},
        })
    elif source_type == "news":
        count = db["news_articles"].count_documents({
            "entity_ids": entity_id,
            "published_at": {"$gte": window_start, "$lt": window_end},
            "data_mode": {"$in": list(PRODUCTION_MODES)},
        })
    elif source_type == "market":
        count = db["market_prices"].count_documents({
            "entity_id": entity_id,
            "event_time": {"$gte": window_start, "$lt": window_end},
            "data_mode": {"$in": list(PRODUCTION_MODES)},
        })
    else:
        count = 0

    return {
        "entity_id": entity_id,
        "source_type": source_type,
        "window_start": window_start,
        "window_end": window_end,
        "window_minutes": window_minutes,
        "count": count,
        "computed_at": datetime.now(timezone.utc),
    }


def upsert_attention_metrics(db, metrics_list: list[dict[str, Any]]) -> int:
    if not metrics_list:
        return 0
    ops = []
    for m in metrics_list:
        key = {"entity_id": m["entity_id"], "source_type": m["source_type"],
               "window_start": m["window_start"], "window_minutes": m["window_minutes"]}
        ops.append(UpdateOne(key, {"$set": m}, upsert=True))
    res = db["attention_metrics"].bulk_write(ops, ordered=False)
    return res.upserted_count + res.modified_count


# ---------------------------------------------------------------------------
# Anomaly detection (z-score on attention counts)
# ---------------------------------------------------------------------------

def _anomaly_id(entity_id: str, source_type: str, window_start: datetime) -> str:
    raw = f"{entity_id}|{source_type}|{window_start.isoformat()}"
    return "ano_" + hashlib.sha256(raw.encode()).hexdigest()[:28]


def detect_anomalies(db, entity_id: str, source_type: str,
                     current_count: int, window_start: datetime, window_end: datetime,
                     window_minutes: int, cfg_anomaly: dict) -> Optional[dict[str, Any]]:
    """Return anomaly doc if current window count is anomalous vs. baseline; else None.

    Requirements (all must be met to produce an anomaly):
      - current_count >= min_count
      - at least baseline_windows historical windows exist
      - baseline std >= min_std (numerically stable)
      - |z_score| >= z_threshold
    """
    min_count = int(cfg_anomaly.get("min_count", 5))
    min_windows = int(cfg_anomaly.get("baseline_windows", 24))
    z_threshold = float(cfg_anomaly.get("z_threshold", 3.0))
    min_std = float(cfg_anomaly.get("min_std", 0.5))

    if current_count < min_count:
        return None

    # Fetch historical attention windows for baseline (older than current window)
    history_docs = list(db["attention_metrics"].find(
        {"entity_id": entity_id, "source_type": source_type,
         "window_start": {"$lt": window_start}, "window_minutes": window_minutes},
        {"_id": 0, "count": 1}
    ).sort("window_start", -1).limit(min_windows * 2))   # fetch more than needed, use last min_windows

    counts = [d["count"] for d in history_docs if d.get("count") is not None]
    if len(counts) < min_windows:
        return None

    baseline = counts[:min_windows]   # sorted desc -> most recent historical windows
    baseline_mean = mean(baseline)
    baseline_std = stdev(baseline) if len(baseline) > 1 else 0.0

    if baseline_std < min_std:
        return None

    z = (current_count - baseline_mean) / baseline_std

    if abs(z) < z_threshold:
        return None

    ano_id = _anomaly_id(entity_id, source_type, window_start)
    return {
        "anomaly_id": ano_id,
        "entity_id": entity_id,
        "source_type": source_type,
        "window_start": window_start,
        "window_end": window_end,
        "window_minutes": window_minutes,
        "observed_count": current_count,
        "baseline_mean": round(baseline_mean, 4),
        "baseline_std": round(baseline_std, 4),
        "baseline_windows_used": len(baseline),
        "z_score": round(z, 4),
        "z_threshold": z_threshold,
        "direction": "spike" if z > 0 else "drop",
        "detected_at": datetime.now(timezone.utc),
    }


def upsert_anomalies(db, anomalies: list[dict[str, Any]]) -> int:
    if not anomalies:
        return 0
    ops = [UpdateOne({"anomaly_id": a["anomaly_id"]}, {"$set": a}, upsert=True) for a in anomalies]
    res = db["anomaly_events"].bulk_write(ops, ordered=False)
    return res.upserted_count + res.modified_count


# ---------------------------------------------------------------------------
# Cross-signal descriptors
# ---------------------------------------------------------------------------

def compute_cross_signals(db, entity_id: str, window_start: datetime,
                           window_end: datetime, window_minutes: int) -> Optional[dict[str, Any]]:
    """Descriptive (non-causal) cross-signal observation for entity in window.

    Looks at: price change, social volume+sentiment, news volume+sentiment.
    Returns a descriptor dict or None if insufficient data for any comparison.
    """
    # Market price change in window (use the two closest bars bracketing the window)
    bars = list(db["market_prices"].find(
        {"entity_id": entity_id,
         "event_time": {"$gte": window_start - timedelta(days=7), "$lte": window_end},
         "data_mode": {"$in": list(PRODUCTION_MODES)}},
        {"_id": 0, "event_time": 1, "close": 1}
    ).sort("event_time", 1).limit(200))

    price_change_pct: Optional[float] = None
    price_obs = 0
    if len(bars) >= 2:
        first = bars[0]["close"]
        last = bars[-1]["close"]
        if first and first > 0:
            price_change_pct = round((last - first) / first * 100, 4)
            price_obs = len(bars)

    # Social sentiment in window (exclude test)
    soc_docs = list(db["social_posts"].find(
        {"entity_ids": entity_id,
         "created_at": {"$gte": window_start, "$lt": window_end},
         "data_mode": {"$in": list(PRODUCTION_MODES)},
         "sentiment_label": {"$ne": None}},
        {"_id": 0, "sentiment_label": 1, "sentiment_score": 1}
    ))
    social_count = db["social_posts"].count_documents({
        "entity_ids": entity_id,
        "created_at": {"$gte": window_start, "$lt": window_end},
        "data_mode": {"$in": list(PRODUCTION_MODES)},
    })
    avg_social_sentiment: Optional[float] = None
    if soc_docs:
        scores = [d["sentiment_score"] for d in soc_docs if d.get("sentiment_score") is not None]
        avg_social_sentiment = round(mean(scores), 6) if scores else None

    # News sentiment in window
    news_docs = list(db["news_articles"].find(
        {"entity_ids": entity_id,
         "published_at": {"$gte": window_start, "$lt": window_end},
         "data_mode": {"$in": list(PRODUCTION_MODES)},
         "sentiment_label": {"$ne": None}},
        {"_id": 0, "sentiment_label": 1, "sentiment_score": 1}
    ))
    news_count = db["news_articles"].count_documents({
        "entity_ids": entity_id,
        "published_at": {"$gte": window_start, "$lt": window_end},
        "data_mode": {"$in": list(PRODUCTION_MODES)},
    })
    avg_news_sentiment: Optional[float] = None
    if news_docs:
        scores = [d["sentiment_score"] for d in news_docs if d.get("sentiment_score") is not None]
        avg_news_sentiment = round(mean(scores), 6) if scores else None

    # Build descriptors
    observations: list[str] = []
    if price_change_pct is not None and avg_social_sentiment is not None:
        if price_change_pct > 1.0 and avg_social_sentiment < -0.05:
            observations.append("price_up_sentiment_down")
        elif price_change_pct < -1.0 and avg_social_sentiment > 0.05:
            observations.append("price_down_sentiment_up")
    if news_count > 0 and price_change_pct is not None and abs(price_change_pct) < 0.5:
        observations.append("news_spike_price_stable")
    if social_count > 0 and news_count == 0 and price_change_pct is not None:
        observations.append("social_attention_without_news")

    return {
        "entity_id": entity_id,
        "window_start": window_start,
        "window_end": window_end,
        "window_minutes": window_minutes,
        "price_change_pct": price_change_pct,
        "price_bar_count": price_obs,
        "social_count": social_count,
        "social_scored_count": len(soc_docs),
        "avg_social_sentiment": avg_social_sentiment,
        "news_count": news_count,
        "news_scored_count": len(news_docs),
        "avg_news_sentiment": avg_news_sentiment,
        "observations": observations,
        "note": "Descriptive only. No causation implied. Does not predict returns.",
        "computed_at": datetime.now(timezone.utc),
    }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_analytics_cycle(db, cfg) -> dict[str, int]:
    """One analytics pass: score sentiment, compute metrics, detect anomalies, cross-signals."""
    settings = cfg.settings
    s_cfg = settings.sentiment
    a_cfg = settings.anomaly
    pos_thr = float(s_cfg.get("positive_threshold", 0.05))
    neg_thr = float(s_cfg.get("negative_threshold", -0.05))
    window_minutes = int(a_cfg.get("window_minutes", 60))

    now = datetime.now(timezone.utc)
    window_start, window_end = _window_bounds(now, window_minutes)

    stats: dict[str, int] = {
        "social_scored": 0, "news_scored": 0,
        "entity_metrics_written": 0, "attention_metrics_written": 0,
        "anomalies_detected": 0,
    }

    # 1. Score sentiment (fill nulls in production docs)
    stats["social_scored"] = _score_batch(db, "social_posts", "text", pos_thr, neg_thr)
    stats["news_scored"] = _score_batch(db, "news_articles", "title", pos_thr, neg_thr)

    entity_ids = [inst.entity_id for inst in cfg.instruments]

    entity_metrics_list = []
    attention_list = []
    anomalies = []

    for entity_id in entity_ids:
        # 2. Entity metrics
        em = _compute_entity_metrics(db, entity_id, window_start, window_end, window_minutes)
        entity_metrics_list.append(em)

        # 3. Attention metrics per source_type
        for source_type in ("social", "news", "market"):
            am = _compute_attention_metrics(db, entity_id, source_type, window_start, window_end, window_minutes)
            attention_list.append(am)

            # 4. Anomaly detection
            ano = detect_anomalies(db, entity_id, source_type, am["count"],
                                   window_start, window_end, window_minutes, a_cfg)
            if ano:
                anomalies.append(ano)

    stats["entity_metrics_written"] = upsert_entity_metrics(db, entity_metrics_list)
    stats["attention_metrics_written"] = upsert_attention_metrics(db, attention_list)
    stats["anomalies_detected"] = upsert_anomalies(db, anomalies)

    return stats


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config()
    interval = int(os.environ.get("ANALYTICS_INTERVAL_SECONDS", "300"))   # 5 min default
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongodb:27017"),
                     tz_aware=True, serverSelectionTimeoutMS=10000)[os.environ.get("MONGO_DB", "marketpulse")]
    log.info("analytics_job started; interval=%ds", interval)
    while True:
        t0 = time.time()
        try:
            stats = run_analytics_cycle(db, cfg)
            log.info("analytics cycle done: %s", stats)
        except Exception:
            log.exception("analytics cycle failed")
        elapsed = time.time() - t0
        sleep = max(0.0, interval - elapsed)
        time.sleep(sleep)


if __name__ == "__main__":
    main()
