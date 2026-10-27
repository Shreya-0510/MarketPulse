"""Stdlib-only JSON helpers (kept dependency-free so they can be unit-tested without FastAPI)."""
from __future__ import annotations

from datetime import datetime, timezone


def json_default(o):
    """json.dumps(default=...) hook: datetimes -> ISO-8601 UTC with explicit offset (JS/Safari-parseable).
    Naive datetimes are treated as UTC (Mongo stores UTC). Anything else falls back to str()."""
    if isinstance(o, datetime):
        if o.tzinfo is None:
            o = o.replace(tzinfo=timezone.utc)
        return o.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(o)
