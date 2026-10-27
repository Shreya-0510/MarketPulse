"""MongoDB sink helpers. Idempotent: upsert keyed on event_id; delivery is at-least-once (see BUILD.md)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pymongo import UpdateOne

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def us_to_dt(us: int | None) -> datetime | None:
    return None if us is None else EPOCH + timedelta(microseconds=int(us))


def market_doc(row: dict[str, Any]) -> dict[str, Any]:
    """Processed market record as stored in `market_prices`."""
    return {
        "event_id": row["event_id"], "schema_version": row["schema_version"], "event_type": row["event_type"],
        "source": row["source"], "symbol": row["symbol"], "entity_id": row["entity_id"],
        "entity_ids": list(row["entity_ids"]), "market": row["market"], "currency": row["currency"],
        "interval": row["interval"], "open": row["open"], "high": row["high"], "low": row["low"],
        "close": row["close"], "adjusted_close": row["adjusted_close"], "volume": row["volume"],
        "data_mode": row["data_mode"], "source_metadata": dict(row["source_metadata"] or {}),
        "event_time": us_to_dt(row["event_time_us"]), "ingested_at": us_to_dt(row["ingested_at_us"]),
        "processed_at": us_to_dt(row["processed_at_us"]), "processing_delay_ms": row["processing_delay_ms"],
        "kafka": {"topic": row["kafka_topic"], "partition": row["kafka_partition"], "offset": row["kafka_offset"]},
    }


def market_ops(rows: list[dict[str, Any]]) -> list[UpdateOne]:
    ops = []
    for r in rows:
        d = market_doc(r)
        ops.append(UpdateOne(
            {"event_id": d["event_id"]},
            {"$set": d, "$setOnInsert": {"first_processed_at": d["processed_at"]}, "$inc": {"delivery_count": 1}},
            upsert=True))
    return ops


def dead_letter_ops(rows: list[dict[str, Any]], now: datetime, max_raw: int = 4096) -> list[UpdateOne]:
    return [UpdateOne(
        {"topic": r["topic"], "partition": r["partition"], "offset": r["offset"]},
        {"$set": {"reason": r["error"], "raw": (r["raw"] or "")[:max_raw], "kafka_timestamp": r["kafka_timestamp"],
                  "recorded_at": now}}, upsert=True) for r in rows]


def status_update(*, stats: dict[str, int], now: datetime, batch_id: int, topic: str, checkpoint: str) -> dict:
    return {
        "$set": {"state": "RUNNING", "last_batch_id": batch_id, "last_batch_at": now, "last_batch": stats,
                 "updated_at": now, "topic": topic, "checkpoint": checkpoint, "last_error": None},
        "$inc": {f"totals.{k}": int(v) for k, v in stats.items()},
    }


def error_status_update(now: datetime, err: str) -> dict:
    return {"$set": {"state": "ERROR", "last_error": err[:500], "updated_at": now}}
