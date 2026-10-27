"""Market metric definitions (also documented in BUILD.md section D)."""
from __future__ import annotations

from datetime import datetime


def change_stats(latest: float | None, reference: float | None) -> tuple[float | None, float | None]:
    """abs = latest - reference ; pct = (latest - reference) / reference * 100. None if reference missing/<=0."""
    if latest is None or reference is None or reference <= 0:
        return None, None
    return latest - reference, (latest - reference) / reference * 100.0


def data_age_hours(bar_time: datetime, now: datetime) -> float:
    return max((now - bar_time).total_seconds(), 0.0) / 3600.0
