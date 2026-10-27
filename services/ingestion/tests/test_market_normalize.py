import json
from datetime import datetime, timedelta, timezone

from marketpulse_common.config import Instrument
from marketpulse_common.ids import market_event_id
from mp_ingestion.market import normalize_rows

IST = timezone(timedelta(hours=5, minutes=30))
INST = Instrument(entity_id="tcs", symbol="TCS.NS", display_name="TCS", market="India", exchange="NSE",
                  instrument_type="equity", currency="INR", timezone="Asia/Kolkata")
NOW = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)


def row(day=2, **kw):
    r = dict(timestamp=datetime(2025, 1, day, tzinfo=IST), open=10.0, high=12.0, low=9.0, close=11.0,
             adj_close=10.9, volume=1000.0)
    r.update(kw)
    return r


def test_valid_row_utc_id_and_metadata():
    res = normalize_rows(INST, [row()], "1d", NOW)
    ev = res.events[0]
    assert ev.event_time == datetime(2025, 1, 1, 18, 30, tzinfo=timezone.utc)   # local midnight -> UTC
    assert ev.event_id == market_event_id("yfinance", "TCS.NS", "1d", datetime(2025, 1, 2, tzinfo=IST))
    assert ev.currency == "INR" and ev.entity_ids == ["tcs"] and ev.data_mode.value == "historical"
    assert ev.source_metadata["session_date_local"] == "2025-01-02"
    assert ev.source_metadata["bar_may_be_provisional"] == "false"
    assert all(isinstance(v, str) for v in ev.source_metadata.values())


def test_missing_values_preserved_not_invented():
    ev = normalize_rows(INST, [row(open=float("nan"), volume=None, adj_close=None)], "1d", NOW).events[0]
    assert ev.open is None and ev.volume is None and ev.adjusted_close is None


def test_rejections_counted_with_reason():
    rows = [row(close=float("nan")), row(day=3, close=-1), row(day=4, timestamp=datetime(2025, 1, 4)),
            row(day=5, high=1.0, low=9.0), row(day=6)]
    res = normalize_rows(INST, rows, "1d", NOW)
    reasons = [r for _, r in res.rejected]
    assert len(res.events) == 1 and len(res.rejected) == 4
    assert reasons[0] == "invalid_close" and reasons[2] == "bad_or_naive_timestamp" and reasons[3].startswith("validation")


def test_duplicate_bars_dropped():
    res = normalize_rows(INST, [row(), row(close=11.5)], "1d", NOW)
    assert len(res.events) == 1 and res.duplicates == 1


def test_today_bar_flagged_provisional():
    res = normalize_rows(INST, [row(day=10)], "1d", NOW)   # ingested 2025-01-10 17:30 IST -> same local date
    assert res.events[0].source_metadata["bar_may_be_provisional"] == "true"


def test_json_payload_shape():
    d = normalize_rows(INST, [row()], "1d", NOW).events[0].model_dump(mode="json")
    json.dumps(d)
    assert d["event_type"] == "market_bar" and d["event_time"].endswith("Z") and d["schema_version"] == "1.0"
    for k in ("event_id", "symbol", "entity_id", "market", "currency", "interval", "close", "ingested_at", "data_mode"):
        assert k in d
