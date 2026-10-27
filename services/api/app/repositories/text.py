"""Social / news persistence access (small interfaces so tests can use in-memory fakes)."""
from __future__ import annotations

from datetime import datetime
from typing import Any

PROJ = {"_id": 0}


class TextRepo:
    def list(self, *, entity: str | None, source: str | None, publisher: str | None, sentiment: str | None,
             start: datetime | None, end: datetime | None, limit: int, offset: int,
             include_test: bool = False) -> tuple[list[dict], int]:
        raise NotImplementedError

    def by_event_id(self, event_id: str) -> dict | None:
        raise NotImplementedError


class MongoTextRepo(TextRepo):
    def __init__(self, db, collection: str, time_field: str):
        self.c, self.tf = db[collection], time_field

    def list(self, *, entity, source, publisher, sentiment, start, end, limit, offset, include_test=False):
        f: dict[str, Any] = {} if include_test else {"data_mode": {"$ne": "test"}}   # smoke/test records are not production data
        if entity: f["entity_ids"] = entity
        if source: f["source"] = source
        if publisher: f["publisher"] = publisher
        if sentiment: f["sentiment_label"] = sentiment
        if start or end:
            f[self.tf] = {**({"$gte": start} if start else {}), **({"$lte": end} if end else {})}
        total = self.c.count_documents(f)
        # newest first; articles with missing published_at sort last (null < dates in descending order)
        docs = list(self.c.find(f, PROJ).sort([(self.tf, -1), ("ingested_at", -1)]).skip(offset).limit(limit))
        return docs, total

    def by_event_id(self, event_id):
        return self.c.find_one({"event_id": event_id}, PROJ)
