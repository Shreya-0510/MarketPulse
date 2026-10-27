"""Mongo documents/ops for social_posts and news_articles. Idempotent upsert on event_id; at-least-once delivery."""
from __future__ import annotations

from typing import Any

from pymongo import UpdateOne

from .sinks import us_to_dt


def _base(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": r["event_id"], "schema_version": r["schema_version"], "event_type": r["event_type"],
        "source": r["source"], "entity_ids": list(r["entity_ids"] or []), "matched_entities": list(r["matched_entities"] or []),
        "sentiment_label": r["sentiment_label"], "sentiment_score": r["sentiment_score"], "data_mode": r["data_mode"],
        "source_metadata": dict(r["source_metadata"] or {}),
        "event_time": us_to_dt(r["event_time_us"]), "ingested_at": us_to_dt(r["ingested_at_us"]),
        "processed_at": us_to_dt(r["processed_at_us"]), "processing_delay_ms": r["processing_delay_ms"],
        "kafka": {"topic": r["kafka_topic"], "partition": r["kafka_partition"], "offset": r["kafka_offset"]},
    }


def social_doc(r: dict[str, Any]) -> dict[str, Any]:
    d = _base(r)
    d.update(platform_post_id=r["platform_post_id"], author_id=r["author_id"], text=r["text"],
             created_at=us_to_dt(r["created_at_us"]), language=r["language"], relevance_score=r["relevance_score"],
             post_url=r["post_url"])
    return d


def news_doc(r: dict[str, Any]) -> dict[str, Any]:
    d = _base(r)
    d.update(publisher=r["publisher"], publisher_verified=bool(r["publisher_verified"]), title=r["title"],
             summary=r["summary"], url=r["url"], published_at=us_to_dt(r["published_at_us"]),
             timestamp_quality=r["timestamp_quality"])
    return d


def _ops(rows, to_doc) -> list[UpdateOne]:
    out = []
    for r in rows:
        d = to_doc(r)
        out.append(UpdateOne({"event_id": d["event_id"]},
                             {"$set": d, "$setOnInsert": {"first_processed_at": d["processed_at"]}, "$inc": {"delivery_count": 1}},
                             upsert=True))
    return out


def social_ops(rows): return _ops(rows, social_doc)
def news_ops(rows): return _ops(rows, news_doc)
