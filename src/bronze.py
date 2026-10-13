"""Bronze layer: ingest raw + CDC files as-is into Delta, plus ingestion metadata.

Run:  python -m src.bronze
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from config.pipeline_config import CDC_SOURCES, RAW_SOURCES, bronze_path, get_spark


def read_source(spark: SparkSession, path, fmt: str) -> DataFrame:
    """CSV is read as all-strings (no inference) so bronze stays faithful to the source."""
    if fmt == "csv":
        return spark.read.option("header", True).csv(str(path))
    if fmt == "json_array":
        return spark.read.option("multiLine", True).json(str(path))
    if fmt == "json_lines":
        return spark.read.json(str(path))
    raise ValueError(f"Unsupported format: {fmt}")


def add_metadata(df: DataFrame, source: str) -> DataFrame:
    return df.withColumn("_source", F.lit(source)).withColumn("_ingested_at", F.current_timestamp())


def ingest_all(spark: SparkSession) -> dict:
    counts = {}
    for name, (path, fmt) in {**RAW_SOURCES, **CDC_SOURCES}.items():
        df = add_metadata(read_source(spark, path, fmt), name)
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .save(bronze_path(name)))
        counts[name] = df.count()
    print("[bronze]", counts)
    return counts


if __name__ == "__main__":
    ingest_all(get_spark("bronze"))
