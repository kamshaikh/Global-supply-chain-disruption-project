"""Silver layer: typed, trimmed, de-duplicated, validated Delta tables.

Each clean_* function is a pure DataFrame -> DataFrame transformation (unit-testable).
Run:  python -m src.silver
"""
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType

from config.pipeline_config import bronze_path, get_spark, silver_path


def _prep(df: DataFrame) -> DataFrame:
    """Drop bronze metadata and trim every string column."""
    df = df.drop("_source", "_ingested_at")
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            df = df.withColumn(f.name, F.trim(F.col(f.name)))
    return df


def clean_suppliers(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.filter(F.col("supplier_id").isNotNull())
              .select("supplier_id", "supplier_name", "country",
                      F.col("tier").cast("int").alias("tier"),
                      F.col("reliability_score").cast("double").alias("reliability_score"),
                      F.col("lead_time_days").cast("int").alias("lead_time_days"),
                      F.upper("status").alias("status"),
                      F.to_timestamp("updated_at").alias("updated_at"))
              .dropDuplicates(["supplier_id"]))


def clean_products(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.filter(F.col("product_id").isNotNull())
              .select("product_id", "product_name", "category", "supplier_id",
                      F.col("unit_cost").cast("double").alias("unit_cost"),
                      F.col("unit_price").cast("double").alias("unit_price"))
              .dropDuplicates(["product_id"]))


def clean_customers(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.filter(F.col("customer_id").isNotNull())
              .select("customer_id", "customer_name", "region", "segment")
              .dropDuplicates(["customer_id"]))


def clean_warehouses(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.filter(F.col("warehouse_id").isNotNull())
              .select("warehouse_id", "warehouse_name", "country", "region",
                      F.col("capacity_units").cast("int").alias("capacity_units"))
              .dropDuplicates(["warehouse_id"]))


def clean_orders(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.select("order_id", "customer_id", "product_id", "warehouse_id",
                      F.to_date("order_date").alias("order_date"),
                      F.col("quantity").cast("int").alias("quantity"),
                      F.upper("order_status").alias("order_status"))
              .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
                      & (F.col("quantity") > 0))
              .dropDuplicates(["order_id"]))


def clean_shipments(df: DataFrame) -> DataFrame:
    df = _prep(df)
    out = (df.filter(F.col("shipment_id").isNotNull())
             .select("shipment_id", "order_id", "supplier_id", "origin_country", "destination_country",
                     "carrier",
                     F.to_date("ship_date").alias("ship_date"),
                     F.to_date("planned_arrival").alias("planned_arrival"),
                     F.to_date("actual_arrival").alias("actual_arrival"),
                     F.upper("status").alias("status"))
             .dropDuplicates(["shipment_id"]))
    return (out.withColumn("delay_days", F.datediff("actual_arrival", "planned_arrival"))
               .withColumn("is_delayed", F.coalesce(F.col("delay_days") > 0, F.lit(False))))


def clean_inventory(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.select("warehouse_id", "product_id",
                      F.col("quantity_on_hand").cast("int").alias("quantity_on_hand"),
                      F.col("reorder_point").cast("int").alias("reorder_point"),
                      F.to_date("snapshot_date").alias("snapshot_date"))
              .filter(F.col("warehouse_id").isNotNull() & F.col("product_id").isNotNull())
              .dropDuplicates(["warehouse_id", "product_id"]))


def clean_disruptions(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.select("disruption_id", "disruption_type", "country", "region",
                      F.col("severity").cast("int").alias("severity"),
                      F.to_date("start_date").alias("start_date"),
                      F.to_date("end_date").alias("end_date"), "description")
              .filter(F.col("disruption_id").isNotNull())
              .dropDuplicates(["disruption_id"]))


def clean_shipment_events(df: DataFrame) -> DataFrame:
    df = _prep(df)
    return (df.select("event_id", "shipment_id", F.upper("event_type").alias("event_type"),
                      F.to_timestamp("event_time").alias("event_time"), "location",
                      F.coalesce(F.col("delay_hours").cast("int"), F.lit(0)).alias("delay_hours"))
              .filter(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())
              .dropDuplicates(["event_id"]))


CLEANERS = {
    "suppliers": clean_suppliers, "products": clean_products, "customers": clean_customers,
    "warehouses": clean_warehouses, "orders": clean_orders, "shipments": clean_shipments,
    "inventory": clean_inventory, "disruptions": clean_disruptions,
    "shipment_events": clean_shipment_events,
}


def build_silver(spark: SparkSession) -> dict:
    counts = {}
    for name, fn in CLEANERS.items():
        df = fn(spark.read.format("delta").load(bronze_path(name)))
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
           .save(silver_path(name)))
        counts[name] = df.count()
    print("[silver]", counts)
    return counts


if __name__ == "__main__":
    build_silver(get_spark("silver"))
