"""Structured Streaming over synthetic shipment events (JSON files in data/streaming).

Reads one file per micro-batch, de-duplicates by event_id, aggregates per day + event type and
upserts the running totals into the Delta table gold_event_metrics (foreachBatch + MERGE).
Uses availableNow, so the stream drains all files and stops (no long-running job).

Run:  python -m src.streaming
"""
import shutil

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from config.pipeline_config import CHECKPOINT_DIR, STREAM_WINDOW, STREAMING_DIR, get_spark, gold_path

METRICS_PATH = gold_path("gold_event_metrics")
CHECKPOINT = str(CHECKPOINT_DIR / "shipment_events")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("shipment_id", StringType()),
    StructField("event_type", StringType()), StructField("event_time", StringType()),
    StructField("location", StringType()), StructField("delay_hours", LongType()),
])


def read_event_stream(spark: SparkSession, path=STREAMING_DIR) -> DataFrame:
    return (spark.readStream.schema(EVENT_SCHEMA).option("maxFilesPerTrigger", 1).json(str(path)))


def transform_events(df: DataFrame) -> DataFrame:
    return (df.withColumn("event_time", F.to_timestamp("event_time"))
              .withColumn("event_type", F.upper(F.trim("event_type")))
              .withColumn("delay_hours", F.coalesce(F.col("delay_hours"), F.lit(0)))
              .filter(F.col("event_id").isNotNull() & F.col("event_time").isNotNull()))


def aggregate_events(df: DataFrame) -> DataFrame:
    return (df.groupBy(F.window("event_time", STREAM_WINDOW).alias("w"), "event_type")
              .agg(F.count("*").alias("event_count"), F.sum("delay_hours").alias("total_delay_hours"))
              .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                      "event_type", "event_count", "total_delay_hours"))


def upsert_metrics(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    agg = aggregate_events(batch_df)
    if not DeltaTable.isDeltaTable(spark, METRICS_PATH):
        agg.write.format("delta").mode("overwrite").save(METRICS_PATH)
        return
    (DeltaTable.forPath(spark, METRICS_PATH).alias("t")
        .merge(agg.alias("s"), "t.window_start = s.window_start AND t.event_type = s.event_type")
        .whenMatchedUpdate(set={"event_count": "t.event_count + s.event_count",
                                "total_delay_hours": "t.total_delay_hours + s.total_delay_hours"})
        .whenNotMatchedInsertAll()
        .execute())


def run_streaming(spark: SparkSession, fresh: bool = True) -> DataFrame:
    if fresh:  # re-runnable: drop old checkpoint + output
        shutil.rmtree(CHECKPOINT, ignore_errors=True)
        shutil.rmtree(METRICS_PATH, ignore_errors=True)
    stream = transform_events(read_event_stream(spark)).dropDuplicates(["event_id"])
    query = (stream.writeStream.foreachBatch(upsert_metrics)
                   .option("checkpointLocation", CHECKPOINT)
                   .trigger(availableNow=True).start())
    query.awaitTermination()
    out = spark.read.format("delta").load(METRICS_PATH)
    print(f"[streaming] gold_event_metrics rows: {out.count()}")
    out.orderBy("window_start", "event_type").show(10, False)
    return out


if __name__ == "__main__":
    run_streaming(get_spark("streaming"))
