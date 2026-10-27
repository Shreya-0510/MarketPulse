from fastapi.testclient import TestClient
from pymongo.errors import ServerSelectionTimeoutError

from app import deps, main


def test_health_reports_mongo_down_truthfully(monkeypatch):
    class Bad:
        class admin:
            @staticmethod
            def command(*a, **k): raise ServerSelectionTimeoutError("x")
    monkeypatch.setattr(deps, "mongo_client", lambda: Bad)
    body = TestClient(main.app).get("/health").json()
    assert body["process"] == "ok" and body["mongodb"].startswith("unavailable")
    assert body["kafka"] == "UNKNOWN" and body["spark"] == "UNKNOWN"


def test_health_mongo_ok(monkeypatch):
    class Good:
        class admin:
            @staticmethod
            def command(*a, **k): return {"ok": 1}
    monkeypatch.setattr(deps, "mongo_client", lambda: Good)
    assert TestClient(main.app).get("/health").json()["mongodb"] == "ok"
