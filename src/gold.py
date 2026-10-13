"""Gold layer: analytical tables + supply-chain risk score.

Tables: gold_supplier_risk, gold_shipment_performance, gold_inventory_risk, gold_disruption_impact
Run:  python -m src.gold
"""
from pyspark.sql import DataFrame, SparkSession, functions as F

from config.pipeline_config import (
    DELAY_RATE_CAP, LEAD_TIME_CAP_DAYS, MAX_SEVERITY, REFERENCE_DATE, RISK_TIER_HIGH,
    RISK_TIER_MEDIUM, RISK_WEIGHTS, get_spark, gold_path, silver_path)


def _silver(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.format("delta").load(silver_path(name))


def _save(df: DataFrame, name: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path(name))
    return df.count()


def active_disruptions(disruptions: DataFrame, ref_date: str = REFERENCE_DATE) -> DataFrame:
    ref = F.to_date(F.lit(ref_date))
    return disruptions.filter((F.col("start_date") <= ref) & (F.col("end_date") >= ref))


def supplier_shipment_stats(shipments: DataFrame) -> DataFrame:
    return shipments.groupBy("supplier_id").agg(
        F.count("*").alias("total_shipments"),
        F.sum(F.when(F.col("actual_arrival").isNotNull(), 1).otherwise(0)).alias("delivered_shipments"),
        F.sum(F.col("is_delayed").cast("int")).alias("delayed_shipments"),
        F.avg("delay_days").alias("avg_delay_days"))


def supplier_disruption_exposure(suppliers: DataFrame, active: DataFrame) -> DataFrame:
    joined = suppliers.select("supplier_id", "country").join(
        active.select("country", "disruption_id", "severity"), "country", "left")
    return joined.groupBy("supplier_id").agg(
        F.count("disruption_id").alias("active_disruptions"),
        F.coalesce(F.max("severity"), F.lit(0)).alias("max_disruption_severity"))


def compute_risk_score(df: DataFrame) -> DataFrame:
    """Risk score 0-100 (higher = riskier) from delay rate, reliability, disruption severity, lead time.

    Required columns: delay_rate, reliability_score, max_disruption_severity, lead_time_days.
    Adds: risk_score, risk_tier (HIGH / MEDIUM / LOW).
    """
    w = RISK_WEIGHTS
    delay = F.least(F.coalesce(F.col("delay_rate"), F.lit(0.0)) / DELAY_RATE_CAP, F.lit(1.0)) * w["delay"]
    reliability = (F.lit(1.0) - F.coalesce(F.col("reliability_score"), F.lit(0.0))) * w["reliability"]
    disruption = (F.coalesce(F.col("max_disruption_severity"), F.lit(0)) / MAX_SEVERITY) * w["disruption"]
    lead = F.least(F.coalesce(F.col("lead_time_days"), F.lit(0)) / LEAD_TIME_CAP_DAYS, F.lit(1.0)) * w["lead_time"]
    score = F.round(F.greatest(F.lit(0.0), F.least(F.lit(100.0), delay + reliability + disruption + lead)), 1)
    return (df.withColumn("risk_score", score)
              .withColumn("risk_tier", F.when(F.col("risk_score") >= RISK_TIER_HIGH, "HIGH")
                                        .when(F.col("risk_score") >= RISK_TIER_MEDIUM, "MEDIUM")
                                        .otherwise("LOW")))


def build_supplier_risk(spark: SparkSession) -> DataFrame:
    suppliers, shipments = _silver(spark, "suppliers"), _silver(spark, "shipments")
    active = active_disruptions(_silver(spark, "disruptions"))
    history = (_silver(spark, "dim_supplier_scd2").groupBy("supplier_id")
               .agg((F.count("*") - 1).alias("profile_changes")))
    df = (suppliers.join(supplier_shipment_stats(shipments), "supplier_id", "left")
                   .join(supplier_disruption_exposure(suppliers, active), "supplier_id", "left")
                   .join(history, "supplier_id", "left")
                   .fillna({"total_shipments": 0, "delivered_shipments": 0, "delayed_shipments": 0,
                            "avg_delay_days": 0.0, "profile_changes": 0, "active_disruptions": 0,
                            "max_disruption_severity": 0})
                   .withColumn("delay_rate", F.round(F.col("delayed_shipments")
                                                     / F.greatest(F.col("delivered_shipments"), F.lit(1)), 3))
                   .withColumn("avg_delay_days", F.round("avg_delay_days", 2)))
    df = compute_risk_score(df).withColumn("as_of_date", F.to_date(F.lit(REFERENCE_DATE)))
    return df.select("supplier_id", "supplier_name", "country", "tier", "status", "reliability_score",
                     "lead_time_days", "total_shipments", "delivered_shipments", "delayed_shipments",
                     "delay_rate", "avg_delay_days", "active_disruptions", "max_disruption_severity",
                     "profile_changes", "risk_score", "risk_tier", "as_of_date")


def build_shipment_performance(spark: SparkSession) -> DataFrame:
    return (_silver(spark, "shipments")
            .withColumn("ship_month", F.date_format("ship_date", "yyyy-MM"))
            .groupBy("destination_country", "ship_month")
            .agg(F.count("*").alias("total_shipments"),
                 F.sum(F.when(F.col("actual_arrival").isNotNull(), 1).otherwise(0)).alias("delivered_shipments"),
                 F.sum(F.col("is_delayed").cast("int")).alias("delayed_shipments"),
                 F.round(F.avg("delay_days"), 2).alias("avg_delay_days"))
            .withColumn("on_time_rate", F.when(F.col("delivered_shipments") > 0,
                        F.round(1 - F.col("delayed_shipments") / F.col("delivered_shipments"), 3))))


def build_inventory_risk(spark: SparkSession, supplier_risk: DataFrame) -> DataFrame:
    products = _silver(spark, "products").select("product_id", "product_name", "supplier_id")
    risk = supplier_risk.select("supplier_id", "risk_score", "risk_tier")
    df = (_silver(spark, "inventory").join(products, "product_id", "left").join(risk, "supplier_id", "left")
          .withColumn("below_reorder", F.col("quantity_on_hand") <= F.col("reorder_point"))
          .withColumn("coverage_ratio", F.round(F.col("quantity_on_hand") / F.col("reorder_point"), 2)))
    return (df.withColumn("priority",
                          F.when(F.col("below_reorder") & (F.col("risk_tier") == "HIGH"), "CRITICAL")
                           .when(F.col("below_reorder") | (F.col("risk_tier") == "HIGH"), "WATCH")
                           .otherwise("OK"))
              .select("warehouse_id", "product_id", "product_name", "supplier_id", "quantity_on_hand",
                      "reorder_point", "below_reorder", "coverage_ratio", "risk_score", "risk_tier", "priority"))


def build_disruption_impact(spark: SparkSession) -> DataFrame:
    d, s = _silver(spark, "disruptions").alias("d"), _silver(spark, "shipments").alias("s")
    cond = (F.col("s.origin_country") == F.col("d.country")) & \
           F.col("s.ship_date").between(F.col("d.start_date"), F.col("d.end_date"))
    return (d.join(s, cond, "left")
             .groupBy("d.disruption_id", "d.disruption_type", "d.country", "d.severity",
                      "d.start_date", "d.end_date")
             .agg(F.count("s.shipment_id").alias("shipments_affected"),
                  F.coalesce(F.sum(F.col("s.is_delayed").cast("int")), F.lit(0)).alias("delayed_shipments"),
                  F.round(F.avg("s.delay_days"), 2).alias("avg_delay_days")))


def build_gold(spark: SparkSession) -> dict:
    risk = build_supplier_risk(spark)
    counts = {"gold_supplier_risk": _save(risk, "gold_supplier_risk")}
    risk = spark.read.format("delta").load(gold_path("gold_supplier_risk"))
    counts["gold_shipment_performance"] = _save(build_shipment_performance(spark), "gold_shipment_performance")
    counts["gold_inventory_risk"] = _save(build_inventory_risk(spark, risk), "gold_inventory_risk")
    counts["gold_disruption_impact"] = _save(build_disruption_impact(spark), "gold_disruption_impact")
    print("[gold]", counts)
    print("[gold] top supplier risks:")
    risk.orderBy(F.desc("risk_score")).select(
        "supplier_id", "country", "delay_rate", "max_disruption_severity", "risk_score", "risk_tier").show(5, False)
    return counts


if __name__ == "__main__":
    build_gold(get_spark("gold"))
