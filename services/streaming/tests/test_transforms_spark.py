"""Needs pyspark + a JVM (run in the streaming container or locally with Java 17). Skipped otherwise."""
import shutil
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("java not found", allow_module_level=True)

from marketpulse_common.events import DataMode, MarketBarEvent
from mp_streaming.transforms import classify_market, valid_market

NOW = datetime(2025, 1, 3, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def bar_json(**kw):
    d = dict(event_id="mkt_abcdef12", source="yfinance", event_time=datetime(2025, 1, 2, tzinfo=timezone.utc), ingested_at=NOW,
             data_mode=DataMode.HISTORICAL, symbol="aapl", entity_id="aapl", market="US", currency="USD", interval="1d",
             open=1, high=2, low=1, close=1.5, volume=10, source_metadata={"k": "v"})
    d.update(kw)
    return MarketBarEvent(**d).model_dump_json()


def kafka_df(spark, values):
    rows = [("market.raw", 0, i, datetime(2025, 1, 3), v.encode()) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "topic string, partition int, offset long, timestamp timestamp, value binary")


def test_valid_event_parsed_with_contract_values(spark):
    parsed = classify_market(kafka_df(spark, [bar_json()]))
    assert parsed.first()["error"] is None
    r = valid_market(parsed).first().asDict()
    assert r["symbol"] == "AAPL" and r["close"] == 1.5 and r["volume"] == 10.0 and r["interval"] == "1d"
    assert r["event_time_us"] == 1735776000_000000 and r["source_metadata"] == {"k": "v"}
    assert r["processed_at_us"] >= r["event_time_us"] and r["kafka_offset"] == 0


def test_malformed_and_contract_violations_flagged_not_crashing(spark):
    vals = ["this is {not json", '{"event_id":"mkt_abcdef12","schema_version":"1.0","event_type":"market_bar",'
            '"symbol":"X","entity_id":"x","event_time":"2025-01-02T00:00:00Z","ingested_at":"2025-01-03T00:00:00Z",'
            '"close":-5,"data_mode":"historical"}', bar_json().replace('"historical"', '"live"'), bar_json()]
    errs = {r["offset"]: r["error"] for r in classify_market(kafka_df(spark, vals)).collect()}
    assert errs == {0: "unparseable_json", 1: "invalid_close", 2: "invalid_data_mode", 3: None}


def test_duplicates_in_batch_dropped(spark):
    out = valid_market(classify_market(kafka_df(spark, [bar_json(), bar_json()]))).collect()
    assert len(out) == 1
