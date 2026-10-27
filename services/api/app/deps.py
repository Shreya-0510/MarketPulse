"""Shared dependencies: Mongo client (tz-aware UTC), config, repositories."""
from __future__ import annotations

import os
from functools import lru_cache

from pymongo import MongoClient

from marketpulse_common.config import AppConfig, load_config

_client: MongoClient | None = None


def mongo_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
                              serverSelectionTimeoutMS=2000, tz_aware=True)
    return _client


def get_db():
    return mongo_client()[os.environ.get("MONGO_DB", "marketpulse")]


@lru_cache
def get_config() -> AppConfig:
    return load_config()


def get_market_repo():
    from .repositories.market import MongoMarketRepo
    return MongoMarketRepo(get_db())


def get_status_repo():
    from .repositories.market import MongoStatusRepo
    return MongoStatusRepo(get_db())


def get_social_repo():
    from .repositories.text import MongoTextRepo
    return MongoTextRepo(get_db(), "social_posts", "created_at")


def get_news_repo():
    from .repositories.text import MongoTextRepo
    return MongoTextRepo(get_db(), "news_articles", "published_at")


def get_analytics_repo():
    from .repositories.analytics import MongoAnalyticsRepo
    return MongoAnalyticsRepo(get_db())
