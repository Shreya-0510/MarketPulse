from marketpulse_common.mongo_schema import INDEXES

REQUIRED = {"market_prices", "news_articles", "social_posts", "entity_metrics",
            "attention_metrics", "anomaly_events", "pipeline_status", "dead_letters"}


def test_all_collections_have_a_unique_index():
    assert set(INDEXES) == REQUIRED
    for coll, specs in INDEXES.items():
        assert any(o.get("unique") for _, o in specs), coll


def test_ensure_indexes_idempotent_calls():
    from marketpulse_common.mongo_schema import ensure_indexes

    class Coll:
        def __init__(self): self.calls = []
        def create_index(self, keys, **o): self.calls.append((keys, o))

    class DB(dict):
        def __missing__(self, k): self[k] = Coll(); return self[k]

    db = DB()
    out = ensure_indexes(db)
    assert out["market_prices"] == 3 and len(db["social_posts"].calls) == 4


def test_ttl_retention_only_on_raw_text_collections(monkeypatch):
    from marketpulse_common.mongo_schema import ensure_indexes, ttl_seconds
    monkeypatch.setenv("SOCIAL_RETENTION_DAYS", "7")
    monkeypatch.setenv("NEWS_RETENTION_DAYS", "0")
    assert ttl_seconds("social_posts") == 7 * 86400 and ttl_seconds("news_articles") is None
    assert ttl_seconds("market_prices") is None

    class Coll:
        def __init__(self): self.calls = []
        def create_index(self, keys, **o): self.calls.append((keys, o))

    class DB(dict):
        def __missing__(self, k): self[k] = Coll(); return self[k]

    db = DB()
    ensure_indexes(db)
    assert any(o.get("expireAfterSeconds") == 7 * 86400 for _, o in db["social_posts"].calls)
    assert not any("expireAfterSeconds" in o for _, o in db["news_articles"].calls)
