import threading
from datetime import datetime, timezone

from marketpulse_common.config import load_config
from mp_ingestion.kafka_out import KafkaPublisher
from mp_ingestion.run_market import run_cycle
from pathlib import Path

CFG = load_config(Path(__file__).resolve().parents[3] / "config")
IST = timezone.utc


class FakeProducer:
    def __init__(self, buffer_errors=0, fail=False):
        self.sent, self.buffer_errors, self.fail, self.cbs = [], buffer_errors, fail, []

    def produce(self, topic, key, value, on_delivery):
        if self.buffer_errors:
            self.buffer_errors -= 1
            raise BufferError
        self.sent.append((topic, key, value))
        self.cbs.append((on_delivery, "boom" if self.fail else None))

    def poll(self, t):
        pass

    def flush(self, timeout):
        for cb, err in self.cbs:
            cb(err, None)
        self.cbs = []
        return 0


def test_publisher_counts_and_retries_buffer_full():
    p = KafkaPublisher("x", producer=FakeProducer(buffer_errors=2))
    p.publish("t", "k", {"a": 1})
    assert p.flush() == (1, 0)


def test_publisher_counts_failures():
    p = KafkaPublisher("x", producer=FakeProducer(fail=True))
    p.publish("t", "k", {"a": 1})
    assert p.flush() == (0, 1) and p.last_error == "boom"


class FakeColl:
    def __init__(self, store, name): self.store, self.name = store, name
    def count_documents(self, f, limit=0): return 1 if f["symbol"] in self.store.get("hist", ()) else 0
    def update_one(self, f, u, upsert=False): self.store["status"] = (f, u)


class FakeDB(dict):
    def __init__(self): super().__init__(); self.store = {"hist": {"MSFT"}}
    def __getitem__(self, name): return FakeColl(self.store, name)


def fake_fetch_factory(calls, bad="NVDA"):
    def fetch(symbol, period, interval, timeout):
        calls.append((symbol, period))
        if symbol == bad:
            raise RuntimeError("blocked")
        return [dict(timestamp=datetime(2025, 1, 2, tzinfo=IST), open=1, high=2, low=1, close=1.5, adj_close=1.5, volume=5)]
    return fetch


def test_one_symbol_failure_does_not_stop_others_and_status_degraded():
    calls, db, pub = [], FakeDB(), KafkaPublisher("x", producer=FakeProducer())
    out = run_cycle(CFG, pub, db, fetch=fake_fetch_factory(calls), sleep=lambda s: None, stop=threading.Event())
    assert out["state"] == "DEGRADED"
    assert out["per_symbol"]["NVDA"]["ok"] is False and out["per_symbol"]["AAPL"]["ok"] is True
    assert sum(1 for s, _ in calls if s == "NVDA") == 3            # bounded retries
    assert out["counters"]["published"] == len(CFG.instruments) - 1
    f, u = db.store["status"]
    assert f == {"component": "ingestion", "name": "yfinance_daily"} and u["$set"]["state"] == "DEGRADED"
    assert "last_success_at" in u["$set"]


def test_incremental_period_used_when_history_exists():
    calls = []
    run_cycle(CFG, KafkaPublisher("x", producer=FakeProducer()), FakeDB(), fetch=fake_fetch_factory(calls, bad="-"),
              sleep=lambda s: None, stop=threading.Event())
    periods = dict(calls)
    assert periods["MSFT"] == "5d" and periods["AAPL"] == "1y"
