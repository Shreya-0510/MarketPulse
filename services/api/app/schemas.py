"""Pydantic response contracts for /api/v1 (mirrored by frontend types in Slice 6)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel


class KafkaProvenance(BaseModel):
    topic: str
    partition: int
    offset: int


class Bar(BaseModel):
    event_id: str
    symbol: str
    entity_id: str
    interval: str
    event_time: datetime
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float
    adjusted_close: Optional[float] = None
    volume: Optional[float] = None
    currency: str
    data_mode: str
    source: str
    ingested_at: datetime
    processed_at: Optional[datetime] = None
    processing_delay_ms: Optional[int] = None
    source_metadata: dict[str, str] = {}


class TraceResponse(Bar):
    market: str
    delivery_count: Optional[int] = None
    first_processed_at: Optional[datetime] = None
    kafka: Optional[KafkaProvenance] = None


class HistoryResponse(BaseModel):
    symbol: str
    entity_id: str
    currency: str
    interval: str
    status: Literal["OK", "NO_DATA"]
    count: int                 # bars returned
    total_in_range: int        # bars matching the filter
    truncated: bool            # True when total_in_range > count (newest `limit` returned)
    bars: list[Bar]
    note: Optional[str] = None


class LatestBar(BaseModel):
    close: float
    bar_time: datetime
    interval: str
    data_mode: str
    is_provisional: bool       # daily bar for the current session; close may still change


class WatchlistItem(BaseModel):
    entity_id: str
    symbol: str
    display_name: str
    market: str
    exchange: Optional[str]
    instrument_type: str
    currency: str
    timezone: str
    status: Literal["OK", "STALE", "NO_DATA"]
    latest: Optional[LatestBar] = None
    previous_close: Optional[float] = None
    change_abs: Optional[float] = None
    change_pct: Optional[float] = None
    change_basis: str = "latest bar close vs previous available daily bar close"
    bars_available: int = 0
    data_age_hours: Optional[float] = None


class WatchlistResponse(BaseModel):
    generated_at: datetime
    stale_after_days: float
    items: list[WatchlistItem]


class PipelineStatusResponse(BaseModel):
    checked_at: datetime
    mongodb: str
    kafka: str = "UNKNOWN"
    components: list[dict[str, Any]]


class SourceStatus(BaseModel):
    name: str
    type: str
    url: str
    enabled: bool
    implemented: bool
    state: str                 # OK | DEGRADED | FAILED | NOT_RUN | DISABLED | NOT_IMPLEMENTED
    verification: str          # UNVERIFIED until a successful fetch has been recorded
    last_attempt_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_error: Optional[str] = None
    updated_at: Optional[datetime] = None   # when the adapter last wrote status; judge staleness from this
    totals: dict[str, int] = {}


class _TextItem(BaseModel):
    event_id: str
    source: str
    data_mode: str
    entity_ids: list[str] = []
    sentiment_label: Optional[str] = None      # null until Slice 5 computes sentiment
    sentiment_score: Optional[float] = None
    event_time: Optional[datetime] = None
    ingested_at: datetime
    processed_at: Optional[datetime] = None
    processing_delay_ms: Optional[int] = None
    source_metadata: dict[str, str] = {}
    kafka: Optional[KafkaProvenance] = None
    delivery_count: Optional[int] = None


class SocialPost(_TextItem):
    platform_post_id: str
    author_id: str
    text: str
    created_at: datetime
    language: Optional[str] = None
    relevance_score: Optional[float] = None
    post_url: Optional[str] = None


class NewsArticle(_TextItem):
    publisher: str
    publisher_verified: bool = False
    title: str
    summary: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = None
    timestamp_quality: Literal["source", "missing"] = "source"


class _Page(BaseModel):
    status: Literal["OK", "NO_DATA"]
    count: int
    total: int
    limit: int
    offset: int
    note: Optional[str] = None


class SocialResponse(_Page):
    items: list[SocialPost]


class NewsResponse(_Page):
    items: list[NewsArticle]


# ---------------------------------------------------------------------------
# Slice 5 — Analytics schemas
# ---------------------------------------------------------------------------

class SentimentDistribution(BaseModel):
    positive: int = 0
    neutral: int = 0
    negative: int = 0
    scored: int = 0
    total: int = 0
    avg_score: Optional[float] = None   # mean VADER compound; null when no scored docs


class SignalStats(BaseModel):
    count: int
    scored: int
    positive: int
    neutral: int
    negative: int
    avg_score: Optional[float] = None


class EntityMetrics(BaseModel):
    entity_id: str
    metric: str
    window_start: datetime
    window_end: datetime
    window_minutes: int
    social: SignalStats
    news: SignalStats
    total_mentions: int
    computed_at: datetime


class EntityMetricsResponse(BaseModel):
    entity_id: str
    display_name: str
    symbol: str
    market: str
    currency: str
    status: Literal["OK", "NO_DATA"]
    windows: list[EntityMetrics]
    note: Optional[str] = None


class AttentionMetric(BaseModel):
    entity_id: str
    source_type: str           # social | news | market
    window_start: datetime
    window_end: datetime
    window_minutes: int
    count: int
    computed_at: datetime


class AttentionResponse(BaseModel):
    status: Literal["OK", "NO_DATA"]
    count: int
    items: list[AttentionMetric]
    note: Optional[str] = None


class AnomalyEvent(BaseModel):
    anomaly_id: str
    entity_id: str
    source_type: str
    window_start: datetime
    window_end: datetime
    window_minutes: int
    observed_count: int
    baseline_mean: float
    baseline_std: float
    baseline_windows_used: int
    z_score: float
    z_threshold: float
    direction: str             # spike | drop
    detected_at: datetime


class AnomalyResponse(BaseModel):
    status: Literal["OK", "NO_DATA"]
    count: int
    items: list[AnomalyEvent]
    note: Optional[str] = None


class CrossSignal(BaseModel):
    entity_id: str
    window_start: datetime
    window_end: datetime
    window_minutes: int
    price_change_pct: Optional[float] = None
    price_bar_count: int = 0
    social_count: int = 0
    social_scored_count: int = 0
    avg_social_sentiment: Optional[float] = None
    news_count: int = 0
    news_scored_count: int = 0
    avg_news_sentiment: Optional[float] = None
    observations: list[str] = []
    note: str = "Descriptive only. No causation implied. Does not predict returns."
    computed_at: datetime


class CrossSignalResponse(BaseModel):
    status: Literal["OK", "NO_DATA"]
    count: int
    items: list[CrossSignal]
    note: Optional[str] = None


class EntitySummary(BaseModel):
    entity_id: str
    display_name: str
    symbol: str
    market: str
    currency: str
    instrument_type: str


class EntitiesResponse(BaseModel):
    count: int
    items: list[EntitySummary]


# WebSocket message envelope
class WSMessage(BaseModel):
    type: str       # social_post | news_article | anomaly | source_status | ping
    payload: Any
    sent_at: str    # ISO-8601 UTC
