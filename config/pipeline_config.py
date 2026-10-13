"""Central configuration: paths, table names, thresholds and the Spark session factory.

Synthetic data only. Everything runs locally (no cloud, no credentials).
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CDC_DIR = DATA_DIR / "cdc"
STREAMING_DIR = DATA_DIR / "streaming"

LAKE_DIR = PROJECT_ROOT / "lakehouse"
BRONZE_DIR = LAKE_DIR / "bronze"
SILVER_DIR = LAKE_DIR / "silver"
GOLD_DIR = LAKE_DIR / "gold"
CHECKPOINT_DIR = LAKE_DIR / "_checkpoints"

SEED = 42
REFERENCE_DATE = "2025-06-30"  # fixed "today" so results are deterministic

# name -> (path, format)   format: csv | json_array | json_lines
RAW_SOURCES = {
    "suppliers": (RAW_DIR / "suppliers.csv", "csv"),
    "products": (RAW_DIR / "products.csv", "csv"),
    "customers": (RAW_DIR / "customers.csv", "csv"),
    "warehouses": (RAW_DIR / "warehouses.csv", "csv"),
    "orders": (RAW_DIR / "orders.csv", "csv"),
    "shipments": (RAW_DIR / "shipments.csv", "csv"),
    "inventory": (RAW_DIR / "inventory.csv", "csv"),
    "disruptions": (RAW_DIR / "disruptions.json", "json_array"),
    "shipment_events": (RAW_DIR / "shipment_events.json", "json_lines"),
}
CDC_SOURCES = {
    "supplier_changes": (CDC_DIR / "supplier_changes.csv", "csv"),
    "shipment_changes": (CDC_DIR / "shipment_changes.csv", "csv"),
}


def bronze_path(name: str) -> str:
    return str(BRONZE_DIR / name)


def silver_path(name: str) -> str:
    return str(SILVER_DIR / name)


def gold_path(name: str) -> str:
    return str(GOLD_DIR / name)


# Supply-chain risk score (0-100). Weights sum to 100.
RISK_WEIGHTS = {"delay": 40.0, "reliability": 25.0, "disruption": 20.0, "lead_time": 15.0}
DELAY_RATE_CAP = 0.5      # a 50%+ late-delivery rate gets the full delay weight
LEAD_TIME_CAP_DAYS = 60   # 60+ day lead time gets the full lead-time weight
MAX_SEVERITY = 5
RISK_TIER_HIGH = 50.0
RISK_TIER_MEDIUM = 30.0

STREAM_WINDOW = "1 day"


def get_spark(app_name: str = "global-supply-chain-disruption", with_delta: bool = True):
    """Local SparkSession, Delta-enabled by default."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if with_delta:
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        builder = configure_spark_with_delta_pip(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
