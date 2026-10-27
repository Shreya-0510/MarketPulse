"""Operational status of an adapter, stored in Mongo `pipeline_status` (component=ingestion)."""
from __future__ import annotations

import logging
from datetime import datetime

log = logging.getLogger("status")


def run_update(*, state: str, now: datetime, error: str | None, counters: dict[str, int],
               per_symbol: dict[str, dict]) -> dict:
    """state: OK (all symbols ok) | DEGRADED (some failed) | FAILED (none succeeded)."""
    set_doc = {"state": state, "last_attempt_at": now, "last_error": error, "last_run": counters,
               "per_symbol": per_symbol, "updated_at": now}
    if state in ("OK", "DEGRADED"):
        set_doc["last_success_at"] = now
    return {"$set": set_doc, "$inc": {f"totals.{k}": int(v) for k, v in counters.items()}}


def record_run(db, name: str, update: dict) -> None:
    """Never let a status-write failure kill the adapter."""
    try:
        db["pipeline_status"].update_one({"component": "ingestion", "name": name}, update, upsert=True)
    except Exception:
        log.exception("could not write pipeline_status")
