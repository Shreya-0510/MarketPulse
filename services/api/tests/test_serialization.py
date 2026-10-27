from datetime import datetime, timedelta, timezone

from app.serialization import json_default


def test_aware_datetime_is_iso_utc_z():
    assert json_default(datetime(2026, 1, 1, 10, 0, 0, 123000, tzinfo=timezone.utc)) == "2026-01-01T10:00:00.123000Z"


def test_non_utc_offset_converted():
    assert json_default(datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))) == "2026-01-01T10:00:00Z"


def test_naive_treated_as_utc_and_other_types_stringified():
    assert json_default(datetime(2026, 1, 1, 10, 0)) == "2026-01-01T10:00:00Z"
    assert json_default(5) == "5"
