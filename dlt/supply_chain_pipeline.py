"""Delta Live Tables version of the pipeline (Databricks runtime only; `dlt` is not pip-installable).

Mirrors src/: bronze -> silver -> SCD2 (apply_changes) -> gold risk score. Not deployed anywhere.
Set pipeline config `supply_chain.data_path` to a folder containing raw/ and cdc/ (synthetic CSV/JSON).
"""
import dlt
from pyspark.sql import functions as F

DATA = spark.conf.get("supply_chain.data_path", "data")  # noqa: F821 (spark provided by DLT)
REF_DATE = "2025-06-30"


# ---------------- Bronze ----------------
def _csv(path):
    return spark.read.option("header", True).csv(f"{DATA}/{path}")  # noqa: F821


@dlt.table(name="bronze_suppliers")
def bronze_suppliers():
    return _csv("raw/suppliers.csv")


@dlt.table(name="bronze_shipments")
def bronze_shipments():
    return _csv("raw/shipments.csv")


@dlt.table(name="bronze_disruptions")
def bronze_disruptions():
    return spark.read.option("multiLine", True).json(f"{DATA}/raw/disruptions.json")  # noqa: F821


@dlt.table(name="bronze_supplier_changes")
def bronze_supplier_changes():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")  # noqa: F821
            .option("header", True).load(f"{DATA}/cdc/supplier_changes.csv"))


# ---------------- Silver ----------------
@dlt.table(name="silver_suppliers")
@dlt.expect_or_drop("supplier_id_not_null", "supplier_id IS NOT NULL")
@dlt.expect("reliability_in_range", "reliability_score BETWEEN 0 AND 1")
def silver_suppliers():
    return (dlt.read("bronze_suppliers")
            .select(F.trim("supplier_id").alias("supplier_id"), F.trim("supplier_name").alias("supplier_name"),
                    F.trim("country").alias("country"), F.col("tier").cast("int").alias("tier"),
                    F.col("reliability_score").cast("double").alias("reliability_score"),
                    F.col("lead_time_days").cast("int").alias("lead_time_days"),
                    F.upper(F.trim("status")).alias("status"),
                    F.to_timestamp("updated_at").alias("updated_at"))
            .dropDuplicates(["supplier_id"]))


@dlt.table(name="silver_shipments")
@dlt.expect_or_drop("shipment_id_not_null", "shipment_id IS NOT NULL")
def silver_shipments():
    df = (dlt.read("bronze_shipments")
          .select("shipment_id", "order_id", "supplier_id", "origin_country", "destination_country", "carrier",
                  F.to_date("ship_date").alias("ship_date"),
                  F.to_date("planned_arrival").alias("planned_arrival"),
                  F.to_date("actual_arrival").alias("actual_arrival"),
                  F.upper(F.trim("status")).alias("status"))
          .dropDuplicates(["shipment_id"]))
    return (df.withColumn("delay_days", F.datediff("actual_arrival", "planned_arrival"))
              .withColumn("is_delayed", F.coalesce(F.col("delay_days") > 0, F.lit(False))))


@dlt.table(name="silver_disruptions")
def silver_disruptions():
    return (dlt.read("bronze_disruptions")
            .select("disruption_id", "disruption_type", "country", "region",
                    F.col("severity").cast("int").alias("severity"),
                    F.to_date("start_date").alias("start_date"), F.to_date("end_date").alias("end_date")))


@dlt.view(name="silver_supplier_changes")
def silver_supplier_changes():
    return (dlt.read_stream("bronze_supplier_changes")
            .select("change_id", "supplier_id", F.upper(F.trim("op")).alias("op"),
                    F.to_timestamp("changed_at").alias("changed_at"), "supplier_name", "country",
                    F.col("tier").cast("int").alias("tier"),
                    F.col("reliability_score").cast("double").alias("reliability_score"),
                    F.col("lead_time_days").cast("int").alias("lead_time_days"),
                    F.upper(F.trim("status")).alias("status")))


# ---------------- SCD Type 2 supplier history ----------------
dlt.create_streaming_table("dim_supplier_scd2")
dlt.apply_changes(
    target="dim_supplier_scd2",
    source="silver_supplier_changes",
    keys=["supplier_id"],
    sequence_by="changed_at",
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["change_id", "op", "changed_at"],
    stored_as_scd_type=2,
)


# ---------------- Gold ----------------
@dlt.table(name="gold_supplier_risk")
@dlt.expect("risk_score_range", "risk_score BETWEEN 0 AND 100")
def gold_supplier_risk():
    ref = F.to_date(F.lit(REF_DATE))
    sup, shp = dlt.read("silver_suppliers"), dlt.read("silver_shipments")
    active = dlt.read("silver_disruptions").filter((F.col("start_date") <= ref) & (F.col("end_date") >= ref))

    stats = shp.groupBy("supplier_id").agg(
        F.sum(F.when(F.col("actual_arrival").isNotNull(), 1).otherwise(0)).alias("delivered_shipments"),
        F.sum(F.col("is_delayed").cast("int")).alias("delayed_shipments"))
    expo = (sup.select("supplier_id", "country").join(active.select("country", "severity"), "country", "left")
               .groupBy("supplier_id").agg(F.coalesce(F.max("severity"), F.lit(0)).alias("max_disruption_severity")))

    df = (sup.join(stats, "supplier_id", "left").join(expo, "supplier_id", "left")
             .fillna({"delivered_shipments": 0, "delayed_shipments": 0, "max_disruption_severity": 0})
             .withColumn("delay_rate", F.col("delayed_shipments") / F.greatest(F.col("delivered_shipments"), F.lit(1))))
    score = (F.least(F.col("delay_rate") / 0.5, F.lit(1.0)) * 40
             + (1 - F.col("reliability_score")) * 25
             + (F.col("max_disruption_severity") / 5) * 20
             + F.least(F.col("lead_time_days") / 60, F.lit(1.0)) * 15)
    df = df.withColumn("risk_score", F.round(F.greatest(F.lit(0.0), F.least(F.lit(100.0), score)), 1))
    return df.withColumn("risk_tier", F.when(F.col("risk_score") >= 50, "HIGH")
                                       .when(F.col("risk_score") >= 30, "MEDIUM").otherwise("LOW"))
