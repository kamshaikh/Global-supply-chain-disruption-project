"""SCD Type 2 supplier history (dim_supplier_scd2) built with Delta MERGE.

Baseline = bronze supplier snapshot; then supplier_changes are applied in time order.
Each change closes the current row (effective_to, is_current=false) and inserts a new version.
Deletes close the current row without inserting a new one.

Run:  python -m src.scd2
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from config.pipeline_config import bronze_path, get_spark, silver_path
from src.cdc import SUPPLIER_ATTRS, prepare_supplier_changes
from src.silver import clean_suppliers

DIM_PATH = silver_path("dim_supplier_scd2")
DIM_COLS = ["supplier_id"] + SUPPLIER_ATTRS


def init_dim(baseline: DataFrame, dim_path: str = DIM_PATH) -> None:
    """Create version 1 of every supplier from a typed baseline snapshot."""
    (baseline.select(*DIM_COLS,
                     F.col("updated_at").alias("effective_from"),
                     F.lit(None).cast("timestamp").alias("effective_to"),
                     F.lit(True).alias("is_current"))
             .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dim_path))


def apply_change_batch(spark: SparkSession, batch: DataFrame, dim_path: str = DIM_PATH) -> None:
    """Apply a batch holding at most ONE change per supplier."""
    close_rows = batch.withColumn("merge_key", F.col("supplier_id"))
    new_rows = batch.filter("op != 'D'").withColumn("merge_key", F.lit(None).cast("string"))
    staged = close_rows.unionByName(new_rows)

    insert_values = {c: f"s.{c}" for c in DIM_COLS}
    insert_values.update({"effective_from": "s.changed_at",
                          "effective_to": F.lit(None).cast("timestamp"),
                          "is_current": F.lit(True)})
    (DeltaTable.forPath(spark, dim_path).alias("d")
        .merge(staged.alias("s"), "d.supplier_id = s.merge_key AND d.is_current = true")
        .whenMatchedUpdate(set={"is_current": F.lit(False), "effective_to": "s.changed_at"})
        .whenNotMatchedInsert(condition="s.merge_key IS NULL", values=insert_values)
        .execute())


def apply_changes(spark: SparkSession, changes: DataFrame, dim_path: str = DIM_PATH) -> None:
    ranked = changes.withColumn(
        "_rank", F.row_number().over(Window.partitionBy("supplier_id").orderBy("changed_at", "change_id")))
    max_rank = ranked.agg(F.max("_rank")).first()[0] or 0
    for r in range(1, max_rank + 1):
        apply_change_batch(spark, ranked.filter(F.col("_rank") == r).drop("_rank"), dim_path)


def build_scd2(spark: SparkSession, dim_path: str = DIM_PATH) -> int:
    baseline = clean_suppliers(spark.read.format("delta").load(bronze_path("suppliers")))
    changes = prepare_supplier_changes(spark.read.format("delta").load(bronze_path("supplier_changes")))
    init_dim(baseline, dim_path)
    apply_changes(spark, changes, dim_path)
    n = spark.read.format("delta").load(dim_path).count()
    print(f"[scd2] dim_supplier_scd2 rows: {n}")
    return n


def supplier_as_of(spark: SparkSession, ts: str, dim_path: str = DIM_PATH) -> DataFrame:
    """Point-in-time view of suppliers."""
    t = F.to_timestamp(F.lit(ts))
    return (spark.read.format("delta").load(dim_path)
            .filter((F.col("effective_from") <= t) & (F.col("effective_to").isNull() | (F.col("effective_to") > t))))


if __name__ == "__main__":
    build_scd2(get_spark("scd2"))
