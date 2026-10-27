"""Persistence access. Routes depend on these small interfaces so tests can substitute in-memory fakes."""
from __future__ import annotations

from datetime import datetime
from typing import Any

PROJ = {"_id": 0}


class MarketRepo:
    def latest_bars(self, symbol: str, interval: str, n: int) -> list[dict]: raise NotImplementedError
    def bar_count(self, symbol: str, interval: str) -> int: raise NotImplementedError
    def history(self, symbol: str, interval: str, start: datetime | None, end: datetime | None,
                limit: int) -> tuple[list[dict], int]: raise NotImplementedError
    def by_event_id(self, event_id: str) -> dict | None: raise NotImplementedError


class StatusRepo:
    def components(self) -> list[dict]: raise NotImplementedError
    def ping(self) -> bool: raise NotImplementedError


class MongoMarketRepo(MarketRepo):
    def __init__(self, db): self.c = db["market_prices"]

    def latest_bars(self, symbol, interval, n):
        return list(self.c.find({"symbol": symbol, "interval": interval}, PROJ).sort("event_time", -1).limit(n))

    def bar_count(self, symbol, interval):
        return self.c.count_documents({"symbol": symbol, "interval": interval})

    def history(self, symbol, interval, start, end, limit):
        f: dict[str, Any] = {"symbol": symbol, "interval": interval}
        if start or end:
            f["event_time"] = {**({"$gte": start} if start else {}), **({"$lte": end} if end else {})}
        total = self.c.count_documents(f)
        docs = list(self.c.find(f, PROJ).sort("event_time", -1).limit(limit))  # newest `limit`, returned ascending
        docs.reverse()
        return docs, total

    def by_event_id(self, event_id):
        return self.c.find_one({"event_id": event_id}, PROJ)


class MongoStatusRepo(StatusRepo):
    def __init__(self, db): self.db = db

    def components(self):
        return list(self.db["pipeline_status"].find({}, PROJ).sort([("component", 1), ("name", 1)]))

    def ping(self):
        self.db.client.admin.command("ping")
        return True
