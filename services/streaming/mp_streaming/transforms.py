"""Spark transforms for market.raw. Pure DataFrame functions (no I/O) so they can be tested with a local session."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SUPPORTED_SCHEMA_VERSION = "1.0"
ALLOWED_MODES = ["historical", "polled", "replay", "test"]   # 'live' is rejected for market bars (contract)

MARKET_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("schema_version", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("ingested_at", T.StringType()),
    T.StructField("data_mode", T.StringType()),
    T.StructField("entity_ids", T.ArrayType(T.StringType())),
    T.StructField("source_metadata", T.MapType(T.StringType(), T.StringType())),
    T.StructField("symbol", T.StringType()),
    T.StructField("entity_id", T.StringType()),
    T.StructField("market", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("interval", T.StringType()),
    T.StructField("open", T.DoubleType()),
    T.StructField("high", T.DoubleType()),
    T.StructField("low", T.DoubleType()),
    T.StructField("close", T.DoubleType()),
    T.StructField("adjusted_close", T.DoubleType()),
    T.StructField("volume", T.DoubleType()),
])


def classify_market(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows (topic, partition, offset, timestamp, value) -> parsed rows with `error` (null when valid)."""
    raw = kafka_df.select("topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
                          F.col("value").cast("string").alias("raw"))
    p = raw.withColumn("e", F.from_json("raw", MARKET_SCHEMA))
    p = (p.withColumn("event_ts", F.col("e.event_time").cast("timestamp"))
          .withColumn("ingested_ts", F.col("e.ingested_at").cast("timestamp")))
    e = F.col("e")
    error: Column = (
        F.when(e.isNull(), "unparseable_json")
        .when(e["event_id"].isNull() | (F.length(e["event_id"]) < 8), "missing_event_id")
        .when(~e["event_type"].eqNullSafe("market_bar"), "wrong_event_type")
        .when(~e["schema_version"].eqNullSafe(SUPPORTED_SCHEMA_VERSION), "unsupported_schema_version")
        .when(e["symbol"].isNull() | e["entity_id"].isNull(), "missing_symbol_or_entity")
        .when(F.col("event_ts").isNull(), "invalid_event_time")
        .when(F.col("ingested_ts").isNull(), "invalid_ingested_at")
        .when(e["close"].isNull() | (e["close"] <= 0), "invalid_close")
        .when(e["data_mode"].isNull() | ~e["data_mode"].isin(ALLOWED_MODES), "invalid_data_mode")
        .when(e["high"].isNotNull() & e["low"].isNotNull() & (e["high"] < e["low"]), "high_below_low")
        .when(e["volume"] < 0, "negative_volume")
        .otherwise(F.lit(None).cast("string"))
    )
    return p.withColumn("error", error)


def valid_market(parsed: DataFrame) -> DataFrame:
    """Valid rows -> flat processed records. Dedupes on event_id within the micro-batch; adds processed_at + delay.
    Timestamps are emitted as epoch microseconds (long) so the sink is independent of JVM/Python time zones."""
    v = parsed.filter(F.col("error").isNull())
    v = v.withColumn("processed_ts", F.current_timestamp())
    e = F.col("e")
    out = v.select(
        e["event_id"].alias("event_id"), e["schema_version"].alias("schema_version"),
        e["event_type"].alias("event_type"), e["source"].alias("source"),
        F.upper(e["symbol"]).alias("symbol"), e["entity_id"].alias("entity_id"),
        F.coalesce(e["entity_ids"], F.array(e["entity_id"])).alias("entity_ids"),
        e["market"].alias("market"), e["currency"].alias("currency"), e["interval"].alias("interval"),
        e["open"].alias("open"), e["high"].alias("high"), e["low"].alias("low"), e["close"].alias("close"),
        e["adjusted_close"].alias("adjusted_close"), e["volume"].alias("volume"),
        e["data_mode"].alias("data_mode"), e["source_metadata"].alias("source_metadata"),
        F.expr("unix_micros(event_ts)").alias("event_time_us"),
        F.expr("unix_micros(ingested_ts)").alias("ingested_at_us"),
        F.expr("unix_micros(processed_ts)").alias("processed_at_us"),
        F.col("topic").alias("kafka_topic"), F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    out = out.withColumn("processing_delay_ms",
                         ((F.col("processed_at_us") - F.col("ingested_at_us")) / 1000).cast("long"))
    return out.dropDuplicates(["event_id"])
