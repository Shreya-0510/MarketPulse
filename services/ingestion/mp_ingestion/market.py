"""Market adapter core: fetch (yfinance) and normalize bars into MarketBarEvent.

normalize_rows is pure and fully unit-tested; fetch_history is the only code touching yfinance.
Daily bar timestamp = yfinance index value (exchange-local midnight of the session date), converted to UTC.
It is NOT the exchange close time and NOT a trade tick.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from marketpulse_common.config import Instrument
from marketpulse_common.events import DataMode, MarketBarEvent
from marketpulse_common.ids import market_event_id

log = logging.getLogger("market")
PROVIDER = "yfinance"


class FetchError(RuntimeError):
    pass


@dataclass
class NormalizeResult:
    events: list[MarketBarEvent] = field(default_factory=list)
    rejected: list[tuple[str, str]] = field(default_factory=list)  # (bar timestamp or '?', reason)
    duplicates: int = 0


def _num(v: Any) -> float | None:
    """Preserve missingness: NaN/inf/non-numeric -> None (never invent values)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def normalize_rows(instrument: Instrument, rows: list[dict], interval: str, ingested_at: datetime,
                   data_mode: DataMode = DataMode.HISTORICAL) -> NormalizeResult:
    res = NormalizeResult()
    seen: set[str] = set()
    for r in rows:
        ts = r.get("timestamp")
        label = ts.isoformat() if isinstance(ts, datetime) else "?"
        if not isinstance(ts, datetime) or ts.tzinfo is None:
            res.rejected.append((label, "bad_or_naive_timestamp"))
            continue
        close = _num(r.get("close"))
        if close is None or close <= 0:
            res.rejected.append((label, "invalid_close"))
            continue
        local_date = ts.date()  # yfinance index is exchange-local
        provisional = local_date >= ingested_at.astimezone(ts.tzinfo).date()
        try:
            ev = MarketBarEvent(
                event_id=market_event_id(PROVIDER, instrument.symbol, interval, ts),
                source=PROVIDER,
                event_time=ts,
                ingested_at=ingested_at,
                data_mode=data_mode,
                symbol=instrument.symbol,
                entity_id=instrument.entity_id,
                market=instrument.market,
                currency=instrument.currency,
                interval=interval,
                open=_num(r.get("open")),
                high=_num(r.get("high")),
                low=_num(r.get("low")),
                close=close,
                adjusted_close=_num(r.get("adj_close")),
                volume=_num(r.get("volume")),
                source_metadata={  # string values only (Spark parses a string map)
                    "exchange": instrument.exchange or "",
                    "exchange_timezone": instrument.timezone,
                    "session_date_local": local_date.isoformat(),
                    "bar_time_semantics": "exchange_local_session_start" if interval == "1d" else "bar_start",
                    "bar_may_be_provisional": "true" if provisional else "false",
                },
            )
        except (ValidationError, ValueError) as e:
            res.rejected.append((label, f"validation: {str(e).splitlines()[0]}"))
            continue
        if ev.event_id in seen:
            res.duplicates += 1
            continue
        seen.add(ev.event_id)
        res.events.append(ev)
    return res


def fetch_history(symbol: str, period: str, interval: str, timeout: int = 30) -> list[dict]:
    """Download OHLCV via yfinance (unofficial Yahoo interface). Raises FetchError on empty/failed responses."""
    import yfinance as yf  # lazy: keeps unit tests free of the dependency

    try:
        df = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False,
                                       actions=False, raise_errors=True, timeout=timeout)
    except Exception as e:  # yfinance raises assorted exception types
        raise FetchError(f"{type(e).__name__}: {e}") from e
    if df is None or df.empty:
        raise FetchError("empty response from yfinance")
    rows = []
    for ts, row in df.iterrows():
        rows.append({
            "timestamp": ts.to_pydatetime(),
            "open": row.get("Open"), "high": row.get("High"), "low": row.get("Low"),
            "close": row.get("Close"), "adj_close": row.get("Adj Close"), "volume": row.get("Volume"),
        })
    return rows
