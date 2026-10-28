from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pymongo.errors import PyMongoError

from .. import deps
from ..schemas import PipelineStatusResponse, SourceStatus

router = APIRouter(prefix="/api/v1")
IMPLEMENTED_TYPES = {"yfinance", "jetstream", "rss", "google_news"}   # adapters that exist in code; updated per slice


@router.get("/pipeline/status", response_model=PipelineStatusResponse)
def pipeline_status(repo=Depends(deps.get_status_repo)):
    """Raw operational documents (ingestion + spark). Kafka lag/health is not instrumented yet -> UNKNOWN."""
    try:
        repo.ping()
        mongo = "ok"
    except PyMongoError as e:
        return PipelineStatusResponse(checked_at=datetime.now(timezone.utc), mongodb=f"unavailable: {type(e).__name__}", components=[])
    return PipelineStatusResponse(checked_at=datetime.now(timezone.utc), mongodb=mongo, components=repo.components())


@router.get("/sources/status", response_model=list[SourceStatus])
def sources_status(repo=Depends(deps.get_status_repo), cfg=Depends(deps.get_config)):
    docs = {d["name"]: d for d in repo.components() if d.get("component") == "ingestion"}
    out = []
    for s in cfg.sources:
        d = docs.get(s.name, {})
        implemented = s.type in IMPLEMENTED_TYPES
        # A persisted failure from an earlier run must not override a source
        # that is now disabled in configuration.
        state = "DISABLED" if not s.enabled else (
            d.get("state") or ("NOT_RUN" if implemented else "NOT_IMPLEMENTED")
        )
        out.append(SourceStatus(
            name=s.name, type=s.type, url=s.url, enabled=s.enabled, implemented=implemented, state=state,
            verification="OBSERVED_SUCCESS" if s.enabled and d.get("last_success_at") else "UNVERIFIED",
            last_attempt_at=d.get("last_attempt_at") if s.enabled else None,
            last_success_at=d.get("last_success_at") if s.enabled else None,
            last_error=d.get("last_error") if s.enabled else None,
            updated_at=d.get("updated_at") if s.enabled else None,
            totals=d.get("totals", {}) if s.enabled else {}))
    return out
