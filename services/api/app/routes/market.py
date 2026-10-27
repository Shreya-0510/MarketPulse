from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import deps
from ..schemas import Bar, HistoryResponse, LatestBar, TraceResponse, WatchlistItem, WatchlistResponse
from ..services.market import change_stats, data_age_hours

router = APIRouter(prefix="/api/v1")
SUPPORTED_INTERVALS = {"1d"}
MAX_LIMIT = 2000


def _stale_days() -> float:
    import os
    return float(os.environ.get("STALE_AFTER_DAYS", "4"))   # covers weekends/holidays for daily bars


def _aware(dt: datetime | None) -> datetime | None:
    return dt if dt is None or dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@router.get("/market/watchlist", response_model=WatchlistResponse)
def watchlist(interval: str = Query("1d"), repo=Depends(deps.get_market_repo), cfg=Depends(deps.get_config)):
    if interval not in SUPPORTED_INTERVALS:
        raise HTTPException(422, f"unsupported interval; supported: {sorted(SUPPORTED_INTERVALS)}")
    now, stale = datetime.now(timezone.utc), _stale_days()
    items = []
    for inst in cfg.instruments:
        base = inst.model_dump()
        bars = repo.latest_bars(inst.symbol, interval, 2)
        if not bars:
            items.append(WatchlistItem(**base, status="NO_DATA"))
            continue
        cur = bars[0]
        prev = bars[1]["close"] if len(bars) > 1 else None
        d_abs, d_pct = change_stats(cur["close"], prev)
        age = data_age_hours(cur["event_time"], now)
        items.append(WatchlistItem(
            **base, status="STALE" if age > stale * 24 else "OK",
            latest=LatestBar(close=cur["close"], bar_time=cur["event_time"], interval=cur["interval"], data_mode=cur["data_mode"],
                             is_provisional=cur.get("source_metadata", {}).get("bar_may_be_provisional") == "true"),
            previous_close=prev, change_abs=d_abs, change_pct=d_pct,
            bars_available=repo.bar_count(inst.symbol, interval), data_age_hours=round(age, 2)))
    return WatchlistResponse(generated_at=now, stale_after_days=stale, items=items)


@router.get("/market/{symbol}/history", response_model=HistoryResponse)
def history(symbol: str, interval: str = Query("1d"), start: Optional[datetime] = None, end: Optional[datetime] = None,
            limit: int = Query(500, ge=1, le=MAX_LIMIT), repo=Depends(deps.get_market_repo), cfg=Depends(deps.get_config)):
    inst = next((i for i in cfg.instruments if i.symbol.upper() == symbol.upper()), None)
    if inst is None:
        raise HTTPException(404, f"symbol {symbol!r} is not in the configured watchlist")
    if interval not in SUPPORTED_INTERVALS:
        raise HTTPException(422, f"unsupported interval; supported: {sorted(SUPPORTED_INTERVALS)}")
    start, end = _aware(start), _aware(end)
    if start and end and start > end:
        raise HTTPException(422, "start must be <= end")
    docs, total = repo.history(inst.symbol, interval, start, end, limit)
    return HistoryResponse(
        symbol=inst.symbol, entity_id=inst.entity_id, currency=inst.currency, interval=interval,
        status="OK" if docs else "NO_DATA", count=len(docs), total_in_range=total, truncated=total > len(docs),
        bars=[Bar(**d) for d in docs],
        note=None if docs else "No persisted bars for this selection (pipeline may not have processed data yet).")


@router.get("/trace/{event_id}", response_model=TraceResponse)
def trace(event_id: str, repo=Depends(deps.get_market_repo)):
    """Return the persisted record for an event_id, including Kafka partition/offset it was consumed from."""
    doc = repo.by_event_id(event_id)
    if doc is None:
        raise HTTPException(404, "event_id not found in market_prices")
    return TraceResponse(**doc)
