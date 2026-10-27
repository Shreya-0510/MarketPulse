"""Spark Structured Streaming job: market.raw (Kafka) -> validate/dedupe -> MongoDB.   python -m mp_streaming.market_job

Semantics: at-least-once from Kafka (checkpointed offsets) + idempotent upsert on event_id => effectively-once results
in `market_prices`. A failing batch is not committed; the container restarts and the batch is retried.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from marketpulse_common.config import load_config

from .sinks import dead_letter_ops, error_status_update, market_ops, status_update
from .transforms import classify_market, valid_market

log = logging.getLogger("market_job")
QUERY_NAME = "market_stream"


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config()
    topic = cfg.settings.kafka.topics.market_raw
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    checkpoint = os.path.join(os.environ.get("CHECKPOINT_ROOT", "/checkpoints"), "market_raw_v1")
    trigger = int(os.environ.get("SPARK_TRIGGER_SECONDS", "10"))
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongodb:27017"), tz_aware=True,
                     serverSelectionTimeoutMS=10000)[os.environ.get("MONGO_DB", "marketpulse")]
    status = db["pipeline_status"]
    sel = {"component": "spark", "name": QUERY_NAME}

    spark = (SparkSession.builder.appName("marketpulse-market-stream").master(os.environ.get("SPARK_MASTER", "local[2]"))
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g")).getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def process_batch(batch_df, batch_id: int) -> None:
        now = datetime.now(timezone.utc)
        try:
            parsed = classify_market(batch_df).persist()
            agg = parsed.agg(F.count("*").alias("n"), F.sum(F.when(F.col("error").isNotNull(), 1).otherwise(0)).alias("bad")).first()
            received, invalid = int(agg["n"]), int(agg["bad"] or 0)
            if received == 0:
                parsed.unpersist()
                return
            rows = [r.asDict(recursive=True) for r in valid_market(parsed).collect()]
            bad_rows = [r.asDict() for r in parsed.filter(F.col("error").isNotNull())
                        .select("topic", "partition", "offset", "kafka_timestamp", "raw", "error").limit(500).collect()]
            parsed.unpersist()
            valid = received - invalid
            for r in bad_rows[:5]:
                log.warning("bad market event %s[%s]@%s: %s", r["topic"], r["partition"], r["offset"], r["error"])
            upserted = matched = 0
            if rows:
                res = db["market_prices"].bulk_write(market_ops(rows), ordered=False)
                upserted, matched = res.upserted_count, res.matched_count
            if bad_rows:
                db["dead_letters"].bulk_write(dead_letter_ops(bad_rows, now), ordered=False)
            stats = {"received": received, "valid": valid, "invalid": invalid,
                     "duplicates_in_batch": valid - len(rows), "upserted_new": upserted, "already_present": matched}
            status.update_one(sel, status_update(stats=stats, now=now, batch_id=batch_id, topic=topic, checkpoint=checkpoint), upsert=True)
            log.info("batch %d: %s", batch_id, stats)
        except Exception as e:
            log.exception("batch %d failed; not committed, will be retried after restart", batch_id)
            try:
                status.update_one(sel, error_status_update(now, f"{type(e).__name__}: {e}"), upsert=True)
            except Exception:
                pass
            raise

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic)
              .option("startingOffsets", "earliest").option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", "5000").load())
    query = (stream.writeStream.queryName(QUERY_NAME).foreachBatch(process_batch)
             .option("checkpointLocation", checkpoint).trigger(processingTime=f"{trigger} seconds").start())
    log.info("streaming started: topic=%s checkpoint=%s trigger=%ss", topic, checkpoint, trigger)
    status.update_one(sel, {"$set": {"state": "RUNNING", "started_at": datetime.now(timezone.utc), "topic": topic,
                                     "checkpoint": checkpoint, "updated_at": datetime.now(timezone.utc)}}, upsert=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
