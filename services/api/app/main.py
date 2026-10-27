"""MarketPulse API. Routes read persisted data only; no external source calls inside requests."""
from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from . import deps
from .routes import market, ops, text, analytics

app = FastAPI(title="MarketPulse API", version="0.5.0")
app.include_router(market.router)
app.include_router(ops.router)
app.include_router(text.router)
app.include_router(analytics.router)

# Optional CORS: only needed when the browser calls the API on a different origin (VITE_API_BASE_URL set).
# Same-origin setups (Vite dev proxy, nginx container) need none. Comma-separated list, empty = disabled.
_origins = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]
if _origins:
    app.add_middleware(CORSMiddleware, allow_origins=_origins, allow_methods=["GET"], allow_headers=["*"])


@app.exception_handler(PyMongoError)
async def mongo_unavailable(_: Request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": f"database unavailable: {type(exc).__name__}"})


@app.get("/health")
def health() -> dict:
    """process=ok means only that this API process runs. MongoDB is probed live; Kafka/Spark are UNKNOWN here
    (see /api/v1/pipeline/status for recorded component state)."""
    try:
        deps.mongo_client().admin.command("ping")
        mongo = "ok"
    except PyMongoError as e:
        mongo = f"unavailable: {type(e).__name__}"
    return {"process": "ok", "mongodb": mongo, "kafka": "UNKNOWN", "spark": "UNKNOWN",
            "checked_at": datetime.now(timezone.utc).isoformat()}
