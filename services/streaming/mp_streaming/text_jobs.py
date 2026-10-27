"""One Spark session, two streaming queries: social.raw -> social_posts and news.raw -> news_articles.
python -m mp_streaming.text_jobs   (compose service `spark-text`; one JVM keeps laptop memory use down)

Slice 5: sentiment is now scored inside foreachBatch (driver-side, after collect()) using VADER.
data_mode=="test" records are stored but marked for exclusion by analytics_job.
If either query dies the process exits non-zero and Docker restarts it; checkpoints resume from committed offsets.
"""
from __future__ import annotations

import logging
import os

from pymongo import MongoClient
from pyspark.sql import SparkSession

from marketpulse_common.config import load_config

from .job_runner import start_query
from .sinks_text import news_ops, social_ops
from .transforms_text import classify_news, classify_social, valid_news, valid_social
from .sentiment import score_text


def _make_social_ops_with_sentiment(pos_thr: float, neg_thr: float):
    """Wrap social_ops to score sentiment after collect() (driver-side, no Spark UDF needed)."""
    def _ops(rows):
        for r in rows:
            if r.get("sentiment_label") is None and r.get("text"):
                label, score = score_text(r["text"], pos_thr, neg_thr)
                r["sentiment_label"] = label
                r["sentiment_score"] = score
        return social_ops(rows)
    return _ops


def _make_news_ops_with_sentiment(pos_thr: float, neg_thr: float):
    """Wrap news_ops to score sentiment on title (driver-side)."""
    def _ops(rows):
        for r in rows:
            if r.get("sentiment_label") is None and r.get("title"):
                label, score = score_text(r["title"], pos_thr, neg_thr)
                r["sentiment_label"] = label
                r["sentiment_score"] = score
        return news_ops(rows)
    return _ops


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    trigger = int(os.environ.get("SPARK_TRIGGER_SECONDS", "10"))
    db = MongoClient(os.environ.get("MONGO_URI", "mongodb://mongodb:27017"), tz_aware=True,
                     serverSelectionTimeoutMS=10000)[os.environ.get("MONGO_DB", "marketpulse")]
    spark = (SparkSession.builder.appName("marketpulse-text-streams").master(os.environ.get("SPARK_MASTER", "local[2]"))
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g")).getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t = cfg.settings.kafka.topics

    s_cfg = cfg.settings.sentiment
    pos_thr = float(s_cfg.get("positive_threshold", 0.05))
    neg_thr = float(s_cfg.get("negative_threshold", -0.05))

    social_ops_s5 = _make_social_ops_with_sentiment(pos_thr, neg_thr)
    news_ops_s5 = _make_news_ops_with_sentiment(pos_thr, neg_thr)

    start_query(spark, db, name="social_stream", topic=t.social_raw, checkpoint_name="social_raw_v1",
                collection="social_posts", classify=classify_social, valid=valid_social,
                ops=social_ops_s5, bootstrap=bootstrap, trigger_seconds=trigger)
    start_query(spark, db, name="news_stream", topic=t.news_raw, checkpoint_name="news_raw_v1",
                collection="news_articles", classify=classify_news, valid=valid_news,
                ops=news_ops_s5, bootstrap=bootstrap, trigger_seconds=trigger)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
