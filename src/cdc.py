"""CDC: apply change records to silver tables with Delta MERGE.

supplier_changes  (I/U/D)  -> silver suppliers  (insert / update / delete)
shipment_changes  (U)      -> silver shipments  (status + actual arrival, recomputed delay)

Run:  python -m src.cdc
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from config.pipeline_config import bronze_path, get_spark, silver_path

SUPPLIER_ATTRS = ["supplier_name", "country", "tier", "reliability_score", "lead_time_days", "status"]


def prepare_supplier_changes(df: DataFrame) -> DataFrame:
    """Type the bronze change feed. Keeps full history (no de-duplication)."""
    return (df.select(F.trim("change_id").alias("change_id"), F.trim("supplier_id").alias("supplier_id"),
                      F.upper(F.trim("op")).alias("op"),
                      F.to_timestamp("changed_at").alias("changed_at"),
                      F.trim("supplier_name").alias("supplier_name"), F.trim("country").alias("country"),
                      F.col("tier").cast("int").alias("tier"),
                      F.col("reliability_score").cast("double").alias("reliability_score"),
                      F.col("lead_time_days").cast("int").alias("lead_time_days"),
                      F.upper(F.trim("status")).alias("status"))
              .filter(F.col("supplier_id").isNotNull() & F.col("op").isin("I", "U", "D")))


def prepare_shipment_changes(df: DataFrame) -> DataFrame:
    return (df.select(F.trim("change_id").alias("change_id"), F.trim("shipment_id").alias("shipment_id"),
                      F.upper(F.trim("op")).alias("op"),
                      F.to_timestamp("changed_at").alias("changed_at"),
                      F.upper(F.trim("status")).alias("status"),
                      F.to_date("actual_arrival").alias("actual_arrival"))
              .filter(F.col("shipment_id").isNotNull()))


def latest_per_key(df: DataFrame, key: str, order_col: str = "changed_at") -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc(), F.col("change_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_supplier_changes(spark: SparkSession, changes: DataFrame, target_path: str) -> None:
    latest = latest_per_key(changes, "supplier_id")
    values = {c: f"s.{c}" for c in ["supplier_id"] + SUPPLIER_ATTRS}
    values["updated_at"] = "s.changed_at"
    updates = {c: f"s.{c}" for c in SUPPLIER_ATTRS}
    updates["updated_at"] = "s.changed_at"
    (DeltaTable.forPath(spark, target_path).alias("t")
        .merge(latest.alias("s"), "t.supplier_id = s.supplier_id")
        .whenMatchedDelete(condition="s.op = 'D'")
        .whenMatchedUpdate(condition="s.op IN ('I','U') AND s.changed_at >= t.updated_at", set=updates)
        .whenNotMatchedInsert(condition="s.op IN ('I','U')", values=values)
        .execute())


def apply_shipment_changes(spark: SparkSession, changes: DataFrame, target_path: str) -> None:
    latest = latest_per_key(changes, "shipment_id")
    delay = "datediff(s.actual_arrival, t.planned_arrival)"
    (DeltaTable.forPath(spark, target_path).alias("t")
        .merge(latest.alias("s"), "t.shipment_id = s.shipment_id")
        .whenMatchedUpdate(condition="s.op = 'U'", set={
            "status": "s.status",
            "actual_arrival": "s.actual_arrival",
            "delay_days": delay,
            "is_delayed": f"coalesce({delay} > 0, false)"})
        .execute())


def run_cdc(spark: SparkSession) -> dict:
    sup = prepare_supplier_changes(spark.read.format("delta").load(bronze_path("supplier_changes")))
    shp = prepare_shipment_changes(spark.read.format("delta").load(bronze_path("shipment_changes")))
    apply_supplier_changes(spark, sup, silver_path("suppliers"))
    apply_shipment_changes(spark, shp, silver_path("shipments"))
    result = {"supplier_changes": sup.count(), "shipment_changes": shp.count()}
    print("[cdc] applied", result)
    return result


if __name__ == "__main__":
    run_cdc(get_spark("cdc"))
