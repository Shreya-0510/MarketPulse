from datetime import datetime, timezone

from mp_streaming.sinks import dead_letter_ops, market_doc, market_ops, status_update, us_to_dt

ROW = dict(event_id="mkt_abcdef12", schema_version="1.0", event_type="market_bar", source="yfinance", symbol="AAPL",
           entity_id="aapl", entity_ids=["aapl"], market="US", currency="USD", interval="1d", open=1.0, high=2.0,
           low=0.5, close=1.5, adjusted_close=None, volume=None, data_mode="historical", source_metadata={"a": "b"},
           event_time_us=1735776000_000000, ingested_at_us=1735862400_000000, processed_at_us=1735862401_500000,
           processing_delay_ms=1500, kafka_topic="market.raw", kafka_partition=1, kafka_offset=42)


def test_us_to_dt_exact_utc():
    assert us_to_dt(1735776000_123456) == datetime(2025, 1, 2, 0, 0, 0, 123456, tzinfo=timezone.utc)
    assert us_to_dt(None) is None


def test_market_doc_preserves_missing_and_provenance():
    d = market_doc(ROW)
    assert d["adjusted_close"] is None and d["volume"] is None
    assert d["kafka"] == {"topic": "market.raw", "partition": 1, "offset": 42}
    assert d["event_time"].tzinfo is not None and d["processed_at"] > d["ingested_at"]


def test_ops_are_idempotent_upserts_keyed_on_event_id():
    op = market_ops([ROW])[0]
    assert op._filter == {"event_id": "mkt_abcdef12"} and op._upsert is True
    assert "$inc" in op._doc and "$setOnInsert" in op._doc


def test_dead_letter_keyed_on_kafka_position_and_truncates():
    now = datetime.now(timezone.utc)
    op = dead_letter_ops([dict(topic="t", partition=0, offset=7, error="x", raw="a" * 10000, kafka_timestamp=now)], now)[0]
    assert op._filter == {"topic": "t", "partition": 0, "offset": 7} and len(op._doc["$set"]["raw"]) == 4096


def test_status_update_counters():
    u = status_update(stats={"received": 3, "valid": 2, "invalid": 1}, now=datetime.now(timezone.utc), batch_id=4,
                      topic="market.raw", checkpoint="/c")
    assert u["$inc"] == {"totals.received": 3, "totals.valid": 2, "totals.invalid": 1} and u["$set"]["state"] == "RUNNING"
