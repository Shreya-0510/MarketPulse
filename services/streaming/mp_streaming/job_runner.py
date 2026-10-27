"""Generic Kafka -> classify -> validate/dedupe -> Mongo upsert streaming query (used by social and news).
Mirrors market_job semantics: at-least-once + idempotent upsert; a failing batch is not committed and the process exits."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import functions as F

from .sinks import dead_letter_ops, error_status_update, status_update

log = logging.getLogger("job_runner")


def start_query(spark, db, *, name: str, topic: str, checkpoint_name: str, collection: str,
                classify: Callable, valid: Callable, ops: Callable, bootstrap: str, trigger_seconds: int):
    checkpoint = os.path.join(os.environ.get("CHECKPOINT_ROOT", "/checkpoints"), checkpoint_name)
    status, sel = db["pipeline_status"], {"component": "spark", "name": name}

    def process_batch(batch_df, batch_id: int) -> None:
        now = datetime.now(timezone.utc)
        try:
            parsed = classify(batch_df).persist()
            agg = parsed.agg(F.count("*").alias("n"), F.sum(F.when(F.col("error").isNotNull(), 1).otherwise(0)).alias("bad")).first()
            received, invalid = int(agg["n"]), int(agg["bad"] or 0)
            if received == 0:
                parsed.unpersist()
                return
            rows = [r.asDict(recursive=True) for r in valid(parsed).collect()]
            bad = [r.asDict() for r in parsed.filter(F.col("error").isNotNull())
                   .select("topic", "partition", "offset", "kafka_timestamp", "raw", "error").limit(500).collect()]
            parsed.unpersist()
            for r in bad[:5]:
                log.warning("bad %s event %s[%s]@%s: %s", name, r["topic"], r["partition"], r["offset"], r["error"])
            upserted = matched = 0
            if rows:
                res = db[collection].bulk_write(ops(rows), ordered=False)
                upserted, matched = res.upserted_count, res.matched_count
            if bad:
                db["dead_letters"].bulk_write(dead_letter_ops(bad, now), ordered=False)
            stats = {"received": received, "valid": received - invalid, "invalid": invalid,
                     "duplicates_in_batch": received - invalid - len(rows), "upserted_new": upserted, "already_present": matched}
            status.update_one(sel, status_update(stats=stats, now=now, batch_id=batch_id, topic=topic, checkpoint=checkpoint), upsert=True)
            log.info("%s batch %d: %s", name, batch_id, stats)
        except Exception as e:
            log.exception("%s batch %d failed; not committed", name, batch_id)
            try:
                status.update_one(sel, error_status_update(now, f"{type(e).__name__}: {e}"), upsert=True)
            except Exception:
                pass
            raise

    stream = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap).option("subscribe", topic)
              .option("startingOffsets", "earliest").option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", "5000").load())
    q = (stream.writeStream.queryName(name).foreachBatch(process_batch).option("checkpointLocation", checkpoint)
         .trigger(processingTime=f"{trigger_seconds} seconds").start())
    t = datetime.now(timezone.utc)
    status.update_one(sel, {"$set": {"state": "RUNNING", "started_at": t, "topic": topic, "checkpoint": checkpoint, "updated_at": t}}, upsert=True)
    log.info("started %s: topic=%s checkpoint=%s", name, topic, checkpoint)
    return q
