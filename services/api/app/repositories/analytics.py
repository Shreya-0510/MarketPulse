"""Read-only access to entity_metrics, attention_metrics, anomaly_events, social_posts/news_articles
for Slice 5 analytics API endpoints.  No writes happen here; analytics_job.py owns the writes.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

PROJ = {"_id": 0}


class MongoAnalyticsRepo:
    def __init__(self, db):
        self.db = db

    # ------------------------------------------------------------------
    # Entity metrics
    # ------------------------------------------------------------------
    def entity_metric_windows(self, entity_id: str, limit: int = 48) -> list[dict[str, Any]]:
        """Most recent `limit` entity-metric windows for entity, newest first."""
        return list(
            self.db["entity_metrics"].find(
                {"entity_id": entity_id, "metric": "combined"},
                PROJ
            ).sort("window_start", -1).limit(limit)
        )

    # ------------------------------------------------------------------
    # Attention metrics
    # ------------------------------------------------------------------
    def attention_list(self, *, entity_id: str | None = None, source_type: str | None = None,
                       start: datetime | None = None, end: datetime | None = None,
                       limit: int = 200) -> list[dict[str, Any]]:
        f: dict[str, Any] = {}
        if entity_id:
            f["entity_id"] = entity_id
        if source_type:
            f["source_type"] = source_type
        if start or end:
            f["window_start"] = {
                **({"$gte": start} if start else {}),
                **({"$lte": end} if end else {}),
            }
        return list(
            self.db["attention_metrics"].find(f, PROJ).sort("window_start", -1).limit(limit)
        )

    # ------------------------------------------------------------------
    # Anomaly events
    # ------------------------------------------------------------------
    def anomaly_list(self, *, entity_id: str | None = None, source_type: str | None = None,
                     start: datetime | None = None, end: datetime | None = None,
                     limit: int = 100) -> list[dict[str, Any]]:
        f: dict[str, Any] = {}
        if entity_id:
            f["entity_id"] = entity_id
        if source_type:
            f["source_type"] = source_type
        if start or end:
            f["detected_at"] = {
                **({"$gte": start} if start else {}),
                **({"$lte": end} if end else {}),
            }
        return list(
            self.db["anomaly_events"].find(f, PROJ).sort("detected_at", -1).limit(limit)
        )

    # ------------------------------------------------------------------
    # Cross-signals (computed live from persisted collections)
    # ------------------------------------------------------------------
    def cross_signals_for_entity(self, entity_id: str, window_start: datetime, window_end: datetime,
                                  window_minutes: int) -> dict[str, Any] | None:
        """Read pre-computed entity_metrics for the window; build cross-signal view."""
        em = self.db["entity_metrics"].find_one(
            {"entity_id": entity_id, "window_start": window_start, "window_minutes": window_minutes},
            PROJ
        )
        # Get market bar count in window
        bar_count = self.db["market_prices"].count_documents({
            "entity_id": entity_id,
            "event_time": {"$gte": window_start, "$lt": window_end},
            "data_mode": {"$in": ["historical", "polled", "live", "replay"]},
        })
        # Price change
        bars = list(self.db["market_prices"].find(
            {"entity_id": entity_id,
             "event_time": {"$gte": window_start - datetime.resolution * 1, "$lte": window_end},
             "data_mode": {"$in": ["historical", "polled", "live", "replay"]}},
            {"_id": 0, "event_time": 1, "close": 1}
        ).sort("event_time", 1).limit(500))
        price_change_pct = None
        if len(bars) >= 2:
            f, l = bars[0]["close"], bars[-1]["close"]
            if f and f > 0:
                price_change_pct = round((l - f) / f * 100, 4)

        observations = []
        avg_soc = em["social"]["avg_score"] if em else None
        avg_news = em["news"]["avg_score"] if em else None
        soc_count = em["social"]["count"] if em else 0
        news_count = em["news"]["count"] if em else 0

        if price_change_pct is not None and avg_soc is not None:
            if price_change_pct > 1.0 and avg_soc < -0.05:
                observations.append("price_up_sentiment_down")
            elif price_change_pct < -1.0 and avg_soc > 0.05:
                observations.append("price_down_sentiment_up")
        if news_count > 0 and price_change_pct is not None and abs(price_change_pct) < 0.5:
            observations.append("news_spike_price_stable")
        if soc_count > 0 and news_count == 0 and price_change_pct is not None:
            observations.append("social_attention_without_news")

        from datetime import timezone
        return {
            "entity_id": entity_id,
            "window_start": window_start,
            "window_end": window_end,
            "window_minutes": window_minutes,
            "price_change_pct": price_change_pct,
            "price_bar_count": bar_count,
            "social_count": soc_count,
            "social_scored_count": em["social"]["scored"] if em else 0,
            "avg_social_sentiment": avg_soc,
            "news_count": news_count,
            "news_scored_count": em["news"]["scored"] if em else 0,
            "avg_news_sentiment": avg_news,
            "observations": observations,
            "note": "Descriptive only. No causation implied. Does not predict returns.",
            "computed_at": em["computed_at"] if em else datetime.now(timezone.utc),
        }

    def latest_entity_windows(self, entity_ids: list[str], window_minutes: int = 60) -> dict[str, dict]:
        """Latest entity_metrics doc per entity_id (for the /entities listing)."""
        out = {}
        for eid in entity_ids:
            doc = self.db["entity_metrics"].find_one(
                {"entity_id": eid, "metric": "combined", "window_minutes": window_minutes},
                PROJ,
                sort=[("window_start", -1)]
            )
            if doc:
                out[eid] = doc
        return out
