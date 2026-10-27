"""Spark transforms for social.raw and news.raw (same classify -> valid pattern as market). Pure DataFrame functions."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .transforms import SUPPORTED_SCHEMA_VERSION

SOCIAL_MODES = ["live", "replay", "test"]
NEWS_MODES = ["polled", "replay", "test"]
SENTIMENT_LABELS = ["positive", "neutral", "negative"]

_COMMON = [
    T.StructField("event_id", T.StringType()), T.StructField("schema_version", T.StringType()),
    T.StructField("event_type", T.StringType()), T.StructField("source", T.StringType()),
    T.StructField("event_time", T.StringType()), T.StructField("ingested_at", T.StringType()),
    T.StructField("data_mode", T.StringType()), T.StructField("entity_ids", T.ArrayType(T.StringType())),
    T.StructField("source_metadata", T.MapType(T.StringType(), T.StringType())),
    T.StructField("matched_entities", T.ArrayType(T.StringType())),
    T.StructField("sentiment_label", T.StringType()), T.StructField("sentiment_score", T.DoubleType()),
]
SOCIAL_SCHEMA = T.StructType(_COMMON + [
    T.StructField("platform_post_id", T.StringType()), T.StructField("author_id", T.StringType()),
    T.StructField("text", T.StringType()), T.StructField("created_at", T.StringType()),
    T.StructField("language", T.StringType()), T.StructField("relevance_score", T.DoubleType()),
    T.StructField("post_url", T.StringType()),
])
NEWS_SCHEMA = T.StructType(_COMMON + [
    T.StructField("publisher", T.StringType()), T.StructField("publisher_verified", T.BooleanType()),
    T.StructField("title", T.StringType()), T.StructField("summary", T.StringType()),
    T.StructField("url", T.StringType()), T.StructField("published_at", T.StringType()),
    T.StructField("timestamp_quality", T.StringType()),
])


def _parse(kafka_df: DataFrame, schema: T.StructType, extra_ts: dict[str, str]) -> DataFrame:
    raw = kafka_df.select("topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
                          F.col("value").cast("string").alias("raw"))
    p = raw.withColumn("e", F.from_json("raw", schema)).withColumn("ingested_ts", F.col("e.ingested_at").cast("timestamp"))
    for out, field in extra_ts.items():
        p = p.withColumn(out, F.col(f"e.{field}").cast("timestamp"))
    return p


def _sentiment_bad(e: Column) -> Column:
    return (e["sentiment_label"].isNotNull() & ~e["sentiment_label"].isin(SENTIMENT_LABELS)) | \
           (e["sentiment_score"].isNotNull() & ((e["sentiment_score"] < -1) | (e["sentiment_score"] > 1)))


def classify_social(kafka_df: DataFrame) -> DataFrame:
    p = _parse(kafka_df, SOCIAL_SCHEMA, {"event_ts": "event_time", "created_ts": "created_at"})
    e = F.col("e")
    err = (F.when(e.isNull(), "unparseable_json")
           .when(e["event_id"].isNull() | (F.length(e["event_id"]) < 8), "missing_event_id")
           .when(~e["event_type"].eqNullSafe("social_post"), "wrong_event_type")
           .when(~e["schema_version"].eqNullSafe(SUPPORTED_SCHEMA_VERSION), "unsupported_schema_version")
           .when(e["platform_post_id"].isNull() | e["author_id"].isNull(), "missing_post_or_author")
           .when(e["text"].isNull() | (F.length(F.trim(e["text"])) == 0), "empty_text")
           .when(F.col("created_ts").isNull(), "invalid_created_at")
           .when(F.col("ingested_ts").isNull(), "invalid_ingested_at")
           .when(e["data_mode"].isNull() | ~e["data_mode"].isin(SOCIAL_MODES), "invalid_data_mode")
           .when(_sentiment_bad(e), "invalid_sentiment")
           .otherwise(F.lit(None).cast("string")))
    return p.withColumn("error", err)


def classify_news(kafka_df: DataFrame) -> DataFrame:
    p = _parse(kafka_df, NEWS_SCHEMA, {"event_ts": "event_time", "published_ts": "published_at"})
    e = F.col("e")
    err = (F.when(e.isNull(), "unparseable_json")
           .when(e["event_id"].isNull() | (F.length(e["event_id"]) < 8), "missing_event_id")
           .when(~e["event_type"].eqNullSafe("news_article"), "wrong_event_type")
           .when(~e["schema_version"].eqNullSafe(SUPPORTED_SCHEMA_VERSION), "unsupported_schema_version")
           .when(e["title"].isNull() | (F.length(F.trim(e["title"])) == 0), "missing_title")
           .when(e["publisher"].isNull(), "missing_publisher")
           .when(e["published_at"].isNotNull() & F.col("published_ts").isNull(), "invalid_published_at")
           .when(F.col("ingested_ts").isNull(), "invalid_ingested_at")
           .when(e["data_mode"].isNull() | ~e["data_mode"].isin(NEWS_MODES), "invalid_data_mode")
           .when(_sentiment_bad(e), "invalid_sentiment")
           .otherwise(F.lit(None).cast("string")))
    return p.withColumn("error", err)


def _common_cols(e: Column) -> list[Column]:
    ents = F.coalesce(e["matched_entities"], e["entity_ids"], F.array().cast("array<string>"))
    return [e["event_id"].alias("event_id"), e["schema_version"].alias("schema_version"), e["event_type"].alias("event_type"),
            e["source"].alias("source"), ents.alias("entity_ids"), ents.alias("matched_entities"),
            e["sentiment_label"].alias("sentiment_label"), e["sentiment_score"].alias("sentiment_score"),
            e["data_mode"].alias("data_mode"), e["source_metadata"].alias("source_metadata")]


def _finish(df: DataFrame) -> DataFrame:
    out = df.withColumn("processing_delay_ms", ((F.col("processed_at_us") - F.col("ingested_at_us")) / 1000).cast("long"))
    return out.dropDuplicates(["event_id"])


def valid_social(parsed: DataFrame) -> DataFrame:
    e = F.col("e")
    v = parsed.filter(F.col("error").isNull()).withColumn("processed_ts", F.current_timestamp())
    out = v.select(*_common_cols(e),
                   e["platform_post_id"].alias("platform_post_id"), e["author_id"].alias("author_id"),
                   e["text"].alias("text"), e["language"].alias("language"), e["relevance_score"].alias("relevance_score"),
                   e["post_url"].alias("post_url"),
                   F.expr("unix_micros(event_ts)").alias("event_time_us"),
                   F.expr("unix_micros(created_ts)").alias("created_at_us"),
                   F.expr("unix_micros(ingested_ts)").alias("ingested_at_us"),
                   F.expr("unix_micros(processed_ts)").alias("processed_at_us"),
                   F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
    return _finish(out)


def valid_news(parsed: DataFrame) -> DataFrame:
    e = F.col("e")
    v = parsed.filter(F.col("error").isNull()).withColumn("processed_ts", F.current_timestamp())
    out = v.select(*_common_cols(e),
                   e["publisher"].alias("publisher"), F.coalesce(e["publisher_verified"], F.lit(False)).alias("publisher_verified"),
                   e["title"].alias("title"), e["summary"].alias("summary"), e["url"].alias("url"),
                   F.when(F.col("published_ts").isNull(), "missing").otherwise("source").alias("timestamp_quality"),
                   F.expr("unix_micros(published_ts)").alias("published_at_us"),
                   F.expr("unix_micros(event_ts)").alias("event_time_us"),
                   F.expr("unix_micros(ingested_ts)").alias("ingested_at_us"),
                   F.expr("unix_micros(processed_ts)").alias("processed_at_us"),
                   F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
    return _finish(out)
