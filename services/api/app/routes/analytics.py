"""Slice 5 analytics routes: entities, entity metrics, attention, anomalies, cross-signals, WebSocket.

All endpoints read from persisted MongoDB data only.  No blocking external calls.
Calculations performed by analytics_job.py (separate process); API only reads results.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from pymongo.errors import PyMongoError

from .. import deps
from ..schemas import (
    AnomalyEvent, AnomalyResponse, AttentionMetric, AttentionResponse,
    CrossSignal, CrossSignalResponse, EntitiesResponse, EntityMetrics,
    EntityMetricsResponse, EntitySummary, WSMessage,
)

from ..serialization import json_default

router = APIRouter(prefix="/api/v1")
log = logging.getLogger("analytics.routes")

_SourceType = Query(None, pattern="^(social|news|market)$")


def _aware(dt: datetime | None) -> datetime | None:
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _window(start, end):
    start, end = _aware(start), _aware(end)
    if start and end and start > end:
        raise HTTPException(422, "start must be <= end")
    return start, end


def _resolve_entity(entity_id: str | None, cfg) -> str | None:
    if entity_id and entity_id not in {i.entity_id for i in cfg.instruments}:
        raise HTTPException(404, f"entity_id {entity_id!r} is not configured")
    return entity_id


def _get_repo():
    from ..repositories.analytics import MongoAnalyticsRepo
    return MongoAnalyticsRepo(deps.get_db())


# ---------------------------------------------------------------------------
# GET /api/v1/entities
# ---------------------------------------------------------------------------

@router.get("/entities", response_model=EntitiesResponse)
def entities_list(cfg=Depends(deps.get_config)):
    """List all configured entities with identity fields."""
    items = [
        EntitySummary(
            entity_id=i.entity_id,
            display_name=i.display_name,
            symbol=i.symbol,
            market=i.market,
            currency=i.currency,
            instrument_type=i.instrument_type,
        )
        for i in cfg.instruments
    ]
    return EntitiesResponse(count=len(items), items=items)


# ---------------------------------------------------------------------------
# GET /api/v1/entities/{entity_id}
# ---------------------------------------------------------------------------

@router.get("/entities/{entity_id}", response_model=EntitySummary)
def entity_detail(entity_id: str, cfg=Depends(deps.get_config)):
    inst = next((i for i in cfg.instruments if i.entity_id == entity_id), None)
    if inst is None:
        raise HTTPException(404, f"entity_id {entity_id!r} is not configured")
    return EntitySummary(
        entity_id=inst.entity_id,
        display_name=inst.display_name,
        symbol=inst.symbol,
        market=inst.market,
        currency=inst.currency,
        instrument_type=inst.instrument_type,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/entities/{entity_id}/metrics
# ---------------------------------------------------------------------------

@router.get("/entities/{entity_id}/metrics", response_model=EntityMetricsResponse)
def entity_metrics(entity_id: str, limit: int = Query(48, ge=1, le=200),
                   cfg=Depends(deps.get_config), repo=Depends(_get_repo)):
    """Up to `limit` recent entity-metric windows (newest first).
    Windows are produced by analytics_job every ANALYTICS_INTERVAL_SECONDS.
    Returns NO_DATA when analytics_job hasn't run yet.
    """
    inst = next((i for i in cfg.instruments if i.entity_id == entity_id), None)
    if inst is None:
        raise HTTPException(404, f"entity_id {entity_id!r} is not configured")
    docs = repo.entity_metric_windows(entity_id, limit=limit)
    windows = []
    for d in docs:
        try:
            windows.append(EntityMetrics(**d))
        except Exception:
            pass   # malformed row skipped; keeps API healthy even if schema evolves
    return EntityMetricsResponse(
        entity_id=entity_id,
        display_name=inst.display_name,
        symbol=inst.symbol,
        market=inst.market,
        currency=inst.currency,
        status="OK" if windows else "NO_DATA",
        windows=windows,
        note=None if windows else "No entity metrics yet — analytics_job may not have completed a full cycle.",
    )


# ---------------------------------------------------------------------------
# GET /api/v1/attention
# ---------------------------------------------------------------------------

@router.get("/attention", response_model=AttentionResponse)
def attention(entity: Optional[str] = None, source_type: Optional[str] = _SourceType,
              start: Optional[datetime] = None, end: Optional[datetime] = None,
              limit: int = Query(200, ge=1, le=1000),
              cfg=Depends(deps.get_config), repo=Depends(_get_repo)):
    """Windowed attention counts (social/news/market events per entity per window)."""
    start, end = _window(start, end)
    entity = _resolve_entity(entity, cfg)
    docs = repo.attention_list(entity_id=entity, source_type=source_type, start=start, end=end, limit=limit)
    items = []
    for d in docs:
        try:
            items.append(AttentionMetric(**d))
        except Exception:
            pass
    return AttentionResponse(
        status="OK" if items else "NO_DATA",
        count=len(items),
        items=items,
        note=None if items else "No attention metrics yet — analytics_job may not have completed a cycle.",
    )


# ---------------------------------------------------------------------------
# GET /api/v1/anomalies
# ---------------------------------------------------------------------------

@router.get("/anomalies", response_model=AnomalyResponse)
def anomalies(entity: Optional[str] = None, source_type: Optional[str] = _SourceType,
              start: Optional[datetime] = None, end: Optional[datetime] = None,
              limit: int = Query(100, ge=1, le=500),
              cfg=Depends(deps.get_config), repo=Depends(_get_repo)):
    """Detected attention anomalies (z-score >= threshold vs. rolling baseline).

    An anomaly requires at minimum `anomaly.baseline_windows` historical windows,
    `anomaly.min_count` observed events, and non-negligible baseline std.
    No anomalies appear until sufficient history accumulates.
    """
    start, end = _window(start, end)
    entity = _resolve_entity(entity, cfg)
    docs = repo.anomaly_list(entity_id=entity, source_type=source_type, start=start, end=end, limit=limit)
    items = []
    for d in docs:
        try:
            items.append(AnomalyEvent(**d))
        except Exception:
            pass
    return AnomalyResponse(
        status="OK" if items else "NO_DATA",
        count=len(items),
        items=items,
        note=None if items else (
            "No anomalies detected yet. Anomaly detection requires at least "
            f"{deps.get_config().settings.anomaly.get('baseline_windows', 24)} historical "
            "windows and sufficient event volume."
        ),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/cross-signals
# ---------------------------------------------------------------------------

@router.get("/cross-signals", response_model=CrossSignalResponse)
def cross_signals(entity: Optional[str] = None,
                  window_minutes: int = Query(60, ge=15, le=1440),
                  lookback_windows: int = Query(24, ge=1, le=168),
                  cfg=Depends(deps.get_config), repo=Depends(_get_repo)):
    """Descriptive cross-signal analysis per entity for the most recent `lookback_windows` windows.

    Covers: price change, social volume+sentiment, news volume+sentiment, and descriptive observations.
    DOES NOT imply causation or predict returns.
    """
    now = datetime.now(timezone.utc)
    td = timedelta(minutes=window_minutes)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    n = int((now - epoch).total_seconds() // td.total_seconds())
    current_end = epoch + n * td

    target_entities = (
        [i for i in cfg.instruments if i.entity_id == entity] if entity
        else list(cfg.instruments)
    )
    if entity and not target_entities:
        raise HTTPException(404, f"entity_id {entity!r} is not configured")

    items = []
    for inst in target_entities:
        for i in range(lookback_windows):
            w_end = current_end - i * td
            w_start = w_end - td
            try:
                cs = repo.cross_signals_for_entity(inst.entity_id, w_start, w_end, window_minutes)
                if cs:
                    items.append(CrossSignal(**cs))
            except Exception:
                pass   # skip malformed; keep other entities/windows

    return CrossSignalResponse(
        status="OK" if items else "NO_DATA",
        count=len(items),
        items=items,
        note=None if items else "No cross-signal data available — insufficient persisted data.",
    )


# ---------------------------------------------------------------------------
# WebSocket /api/v1/ws
# ---------------------------------------------------------------------------

PING_INTERVAL = 20   # seconds between server pings to detect dead connections
POLL_INTERVAL = 3    # seconds between MongoDB polls for new events
MAX_HISTORY = 50     # recent events to send on connect


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, entity: Optional[str] = None,
                              source: Optional[str] = None):
    """Bounded live-update WebSocket.

    On connect: sends up to MAX_HISTORY recent social+news events and recent anomalies.
    Then polls MongoDB every POLL_INTERVAL seconds for newer events.
    Sends ping frames every PING_INTERVAL seconds.
    data_mode=="test" excluded.
    No unbounded history is sent. Client must handle reconnect.
    """
    await ws.accept()
    db = deps.get_db()
    cfg = deps.get_config()

    # Validate entity filter
    if entity and entity not in {i.entity_id for i in cfg.instruments}:
        await ws.send_text(json.dumps({
            "type": "error", "payload": {"message": f"unknown entity_id {entity!r}"}, "sent_at": _now_iso()
        }))
        await ws.close(code=1008)
        return

    def _send(msg_type: str, payload: dict) -> str:
        return json.dumps({"type": msg_type, "payload": payload, "sent_at": _now_iso()}, default=json_default)

    # Track highest ingested_at seen so far to detect new events
    now_dt = datetime.now(timezone.utc)
    # Send recent history on connect
    try:
        soc_f: dict = {"data_mode": {"$nin": ["test"]}}
        if entity:
            soc_f["entity_ids"] = entity
        if source:
            soc_f["source"] = source
        recent_soc = list(db["social_posts"].find(soc_f, {"_id": 0}).sort("ingested_at", -1).limit(MAX_HISTORY))
        for doc in reversed(recent_soc):
            await ws.send_text(_send("social_post", doc))

        news_f: dict = {"data_mode": {"$nin": ["test"]}}
        if entity:
            news_f["entity_ids"] = entity
        if source:
            news_f["source"] = source
        recent_news = list(db["news_articles"].find(news_f, {"_id": 0}).sort("ingested_at", -1).limit(MAX_HISTORY))
        for doc in reversed(recent_news):
            await ws.send_text(_send("news_article", doc))

        recent_ano = list(db["anomaly_events"].find(
            {"entity_id": entity} if entity else {}, {"_id": 0}
        ).sort("detected_at", -1).limit(20))
        for doc in reversed(recent_ano):
            await ws.send_text(_send("anomaly", doc))

        # Cursor: highest ingested_at seen
        cursor_soc = max((d["ingested_at"] for d in recent_soc), default=now_dt)
        cursor_news = max((d["ingested_at"] for d in recent_news), default=now_dt)
        cursor_ano = max((d["detected_at"] for d in recent_ano), default=now_dt)

    except Exception:
        log.exception("ws: error sending history")
        cursor_soc = cursor_news = cursor_ano = now_dt

    last_ping = asyncio.get_event_loop().time()

    try:
        while True:
            await asyncio.sleep(POLL_INTERVAL)

            # Ping check
            now_loop = asyncio.get_event_loop().time()
            if now_loop - last_ping >= PING_INTERVAL:
                await ws.send_text(_send("ping", {"server_time": _now_iso()}))
                last_ping = now_loop

            # Poll for new social posts
            try:
                new_soc_f: dict = {"ingested_at": {"$gt": cursor_soc}, "data_mode": {"$nin": ["test"]}}
                if entity:
                    new_soc_f["entity_ids"] = entity
                if source:
                    new_soc_f["source"] = source
                new_soc = list(db["social_posts"].find(new_soc_f, {"_id": 0}).sort("ingested_at", 1).limit(20))
                for doc in new_soc:
                    await ws.send_text(_send("social_post", doc))
                    cursor_soc = max(cursor_soc, doc["ingested_at"])
            except PyMongoError:
                pass

            # Poll for new news articles
            try:
                new_news_f: dict = {"ingested_at": {"$gt": cursor_news}, "data_mode": {"$nin": ["test"]}}
                if entity:
                    new_news_f["entity_ids"] = entity
                if source:
                    new_news_f["source"] = source
                new_news = list(db["news_articles"].find(new_news_f, {"_id": 0}).sort("ingested_at", 1).limit(20))
                for doc in new_news:
                    await ws.send_text(_send("news_article", doc))
                    cursor_news = max(cursor_news, doc["ingested_at"])
            except PyMongoError:
                pass

            # Poll for new anomalies
            try:
                new_ano_f: dict = {"detected_at": {"$gt": cursor_ano}}
                if entity:
                    new_ano_f["entity_id"] = entity
                new_ano = list(db["anomaly_events"].find(new_ano_f, {"_id": 0}).sort("detected_at", 1).limit(10))
                for doc in new_ano:
                    await ws.send_text(_send("anomaly", doc))
                    cursor_ano = max(cursor_ano, doc["detected_at"])
            except PyMongoError:
                pass

    except WebSocketDisconnect:
        log.info("ws: client disconnected (entity=%s source=%s)", entity, source)
    except Exception:
        log.exception("ws: unexpected error")
        try:
            await ws.close(code=1011)
        except Exception:
            pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
