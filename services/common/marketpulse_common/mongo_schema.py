"""MongoDB collections and indexes. ensure_indexes is idempotent and never drops data."""
from __future__ import annotations

import logging
import os
import sys

from pymongo import ASCENDING, DESCENDING, MongoClient

log = logging.getLogger("mongo_init")

# collection -> list of (keys, options)
INDEXES: dict[str, list[tuple[list[tuple[str, int]], dict]]] = {
    "market_prices": [
        ([("event_id", ASCENDING)], {"unique": True}),
        ([("symbol", ASCENDING), ("interval", ASCENDING), ("event_time", DESCENDING)], {}),
        ([("entity_id", ASCENDING), ("event_time", DESCENDING)], {}),
    ],
    "news_articles": [
        ([("event_id", ASCENDING)], {"unique": True}),
        ([("entity_ids", ASCENDING), ("published_at", DESCENDING)], {}),
        ([("publisher", ASCENDING), ("published_at", DESCENDING)], {}),
        ([("ingested_at", DESCENDING)], {}),
    ],
    "social_posts": [
        ([("event_id", ASCENDING)], {"unique": True}),
        ([("entity_ids", ASCENDING), ("created_at", DESCENDING)], {}),
        ([("sentiment_label", ASCENDING), ("created_at", DESCENDING)], {}),
        ([("ingested_at", DESCENDING)], {}),
    ],
    "entity_metrics": [
        ([("entity_id", ASCENDING), ("metric", ASCENDING), ("window_start", ASCENDING), ("window_minutes", ASCENDING)], {"unique": True}),
    ],
    "attention_metrics": [
        ([("entity_id", ASCENDING), ("source_type", ASCENDING), ("window_start", ASCENDING), ("window_minutes", ASCENDING)], {"unique": True}),
    ],
    "anomaly_events": [
        ([("anomaly_id", ASCENDING)], {"unique": True}),
        ([("entity_id", ASCENDING), ("detected_at", DESCENDING)], {}),
    ],
    "dead_letters": [   # malformed/invalid Kafka records seen by Spark (keyed by Kafka position)
        ([("topic", ASCENDING), ("partition", ASCENDING), ("offset", ASCENDING)], {"unique": True}),
        ([("recorded_at", DESCENDING)], {}),
    ],
    "pipeline_status": [
        ([("component", ASCENDING), ("name", ASCENDING)], {"unique": True}),
    ],
}


# Retention (TTL) on ingested_at for raw text collections; days from env (defaults mirror config/settings.yaml retention).
TTL_ENV = {"social_posts": ("SOCIAL_RETENTION_DAYS", 14), "news_articles": ("NEWS_RETENTION_DAYS", 30)}
TTL_KEY = [("ingested_at", DESCENDING)]


def ttl_seconds(coll: str) -> int | None:
    if coll not in TTL_ENV:
        return None
    env, default = TTL_ENV[coll]
    days = int(os.environ.get(env, default))
    return days * 86400 if days > 0 else None       # 0 = keep forever


def ensure_indexes(db) -> dict[str, int]:
    from pymongo.errors import OperationFailure
    created: dict[str, int] = {}
    for coll, specs in INDEXES.items():
        for keys, opts in specs:
            opts = dict(opts)
            ttl = ttl_seconds(coll) if keys == TTL_KEY else None
            if ttl:
                opts["expireAfterSeconds"] = ttl
            try:
                db[coll].create_index(keys, **opts)
            except OperationFailure as e:
                if ttl and e.code in (85, 86):   # index exists with other options (e.g. pre-TTL or changed retention) -> collMod
                    db.command("collMod", coll, index={"keyPattern": {"ingested_at": -1}, "expireAfterSeconds": ttl})
                else:
                    raise
        created[coll] = len(specs)
    return created


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    name = os.environ.get("MONGO_DB", "marketpulse")
    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    result = ensure_indexes(client[name])
    log.info("indexes ensured: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
