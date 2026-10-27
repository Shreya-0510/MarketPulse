"""Shared event contracts (schema 1.0). All timestamps are timezone-aware and normalized to UTC."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from . import SCHEMA_VERSION


class DataMode(str, Enum):
    HISTORICAL = "historical"
    POLLED = "polled"
    LIVE = "live"
    REPLAY = "replay"
    TEST = "test"


class EventType(str, Enum):
    MARKET_BAR = "market_bar"
    SOCIAL_POST = "social_post"
    NEWS_ARTICLE = "news_article"
    ANALYTICS_UPDATE = "analytics_update"
    OPERATIONAL = "operational"


class SentimentLabel(str, Enum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


def _to_utc(v: Optional[datetime]) -> Optional[datetime]:
    return v.astimezone(timezone.utc) if v is not None else None


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False)

    event_id: str = Field(min_length=8)
    schema_version: str = SCHEMA_VERSION
    event_type: EventType
    source: str = Field(min_length=1)
    event_time: Optional[AwareDatetime] = None      # source time; None when unknown
    ingested_at: AwareDatetime
    processed_at: Optional[AwareDatetime] = None    # set by Spark
    data_mode: DataMode
    entity_ids: list[str] = Field(default_factory=list)
    source_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_time", "ingested_at", "processed_at")
    @classmethod
    def _utc(cls, v):
        return _to_utc(v)


class MarketBarEvent(EventEnvelope):
    event_type: Literal[EventType.MARKET_BAR] = EventType.MARKET_BAR
    symbol: str
    entity_id: str
    market: str
    currency: str
    interval: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float
    adjusted_close: Optional[float] = None
    volume: Optional[float] = None

    @model_validator(mode="after")
    def _check(self):
        if self.event_time is None:
            raise ValueError("market bars require event_time (bar timestamp)")
        if self.data_mode == DataMode.LIVE:
            raise ValueError("market bars must not be labelled 'live' (yfinance is not an exchange feed)")
        if self.close <= 0:
            raise ValueError("close must be > 0")
        if self.high is not None and self.low is not None and self.high < self.low:
            raise ValueError("high < low")
        if self.volume is not None and self.volume < 0:
            raise ValueError("volume < 0")
        if not self.entity_ids:
            self.entity_ids = [self.entity_id]
        return self


class _TextEvent(EventEnvelope):
    matched_entities: list[str] = Field(default_factory=list)
    sentiment_label: Optional[SentimentLabel] = None
    sentiment_score: Optional[float] = Field(default=None, ge=-1.0, le=1.0)

    @model_validator(mode="after")
    def _sync_entities(self):
        if not self.entity_ids:
            self.entity_ids = list(self.matched_entities)
        elif not self.matched_entities:
            self.matched_entities = list(self.entity_ids)
        elif set(self.entity_ids) != set(self.matched_entities):
            raise ValueError("entity_ids and matched_entities disagree")
        return self


class SocialPostEvent(_TextEvent):
    event_type: Literal[EventType.SOCIAL_POST] = EventType.SOCIAL_POST
    platform_post_id: str          # Bluesky AT URI
    author_id: str                 # DID only; no profile data stored
    text: str
    created_at: AwareDatetime
    language: Optional[str] = None
    relevance_score: Optional[float] = None
    post_url: Optional[str] = None

    @field_validator("created_at")
    @classmethod
    def _utc_created(cls, v):
        return _to_utc(v)


class NewsArticleEvent(_TextEvent):
    event_type: Literal[EventType.NEWS_ARTICLE] = EventType.NEWS_ARTICLE
    publisher: str
    publisher_verified: bool = False   # False when inferred from an aggregator
    title: str
    summary: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[AwareDatetime] = None
    timestamp_quality: Literal["source", "missing"] = "source"

    @field_validator("published_at")
    @classmethod
    def _utc_pub(cls, v):
        return _to_utc(v)

    @model_validator(mode="after")
    def _quality(self):
        if self.published_at is None:
            self.timestamp_quality = "missing"   # never substitute ingested_at
        return self
