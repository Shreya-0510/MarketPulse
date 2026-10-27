"""GET /api/v1/social and /api/v1/news (+ /{event_id} drill-down). Read persisted data only."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import deps
from ..schemas import NewsArticle, NewsResponse, SocialPost, SocialResponse

router = APIRouter(prefix="/api/v1")
MAX_LIMIT = 200
Sentiment = Query(None, pattern="^(positive|neutral|negative)$")


def _aware(dt: datetime | None) -> datetime | None:
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _window(start, end):
    start, end = _aware(start), _aware(end)
    if start and end and start > end:
        raise HTTPException(422, "start must be <= end")
    return start, end


def _entity(entity: str | None, cfg) -> str | None:
    if entity and entity not in {i.entity_id for i in cfg.instruments}:
        raise HTTPException(404, f"entity {entity!r} is not configured")
    return entity


@router.get("/social", response_model=SocialResponse)
def social(entity: Optional[str] = None, source: Optional[str] = None, sentiment: Optional[str] = Sentiment,
           start: Optional[datetime] = None, end: Optional[datetime] = None,
           limit: int = Query(50, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0, le=100_000),
           include_test: bool = Query(False, description="include data_mode=test records (smoke tests only)"),
           repo=Depends(deps.get_social_repo), cfg=Depends(deps.get_config)):
    """Entity-matched Bluesky posts, newest `created_at` first. start/end filter on created_at (author-claimed, clamped)."""
    start, end = _window(start, end)
    docs, total = repo.list(entity=_entity(entity, cfg), source=source, publisher=None, sentiment=sentiment,
                            start=start, end=end, limit=limit, offset=offset, include_test=include_test)
    return SocialResponse(status="OK" if total else "NO_DATA", count=len(docs), total=total, limit=limit, offset=offset,
                          items=[SocialPost(**d) for d in docs],
                          note=None if total else "No persisted social posts match (pipeline may not have processed any yet).")


@router.get("/news", response_model=NewsResponse)
def news(entity: Optional[str] = None, source: Optional[str] = None, publisher: Optional[str] = None,
         sentiment: Optional[str] = Sentiment, start: Optional[datetime] = None, end: Optional[datetime] = None,
         limit: int = Query(50, ge=1, le=MAX_LIMIT), offset: int = Query(0, ge=0, le=100_000),
         include_test: bool = Query(False, description="include data_mode=test records (smoke tests only)"),
         repo=Depends(deps.get_news_repo), cfg=Depends(deps.get_config)):
    """Articles newest `published_at` first. start/end filter on published_at, so articles lacking a
    publication time are excluded when a time filter is applied (never assigned their ingestion time)."""
    start, end = _window(start, end)
    docs, total = repo.list(entity=_entity(entity, cfg), source=source, publisher=publisher, sentiment=sentiment,
                            start=start, end=end, limit=limit, offset=offset, include_test=include_test)
    return NewsResponse(status="OK" if total else "NO_DATA", count=len(docs), total=total, limit=limit, offset=offset,
                        items=[NewsArticle(**d) for d in docs],
                        note=None if total else "No persisted articles match (pipeline may not have processed any yet).")


@router.get("/social/{event_id}", response_model=SocialPost)
def social_one(event_id: str, repo=Depends(deps.get_social_repo)):
    doc = repo.by_event_id(event_id)
    if doc is None:
        raise HTTPException(404, "event_id not found in social_posts")
    return SocialPost(**doc)


@router.get("/news/{event_id}", response_model=NewsArticle)
def news_one(event_id: str, repo=Depends(deps.get_news_repo)):
    doc = repo.by_event_id(event_id)
    if doc is None:
        raise HTTPException(404, "event_id not found in news_articles")
    return NewsArticle(**doc)
