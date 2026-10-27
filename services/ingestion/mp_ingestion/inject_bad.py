"""Publish deliberately malformed events to market.raw to exercise Spark's bad-record path.
python -m mp_ingestion.inject_bad   (run inside the ingestion container)"""
import json
import os

from marketpulse_common.config import load_config

from .kafka_out import KafkaPublisher


def main() -> int:
    cfg = load_config()
    topic = cfg.settings.kafka.topics.market_raw
    pub = KafkaPublisher(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    # 1) not JSON at all: sent raw (bypasses publish()'s json.dumps)
    pub._p.produce(topic, key=b"BAD|1d", value=b"this is {not json", on_delivery=pub._cb)
    # 2) valid JSON, contract violation (close <= 0, live mode)
    pub.publish(topic, "BAD|1d", {"event_id": "mkt_badbadbad", "schema_version": "1.0", "event_type": "market_bar",
                                  "symbol": "BAD", "entity_id": "bad", "close": -5, "data_mode": "live"})
    acked, failed = pub.flush()
    print(json.dumps({"acked": acked, "failed": failed}))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
