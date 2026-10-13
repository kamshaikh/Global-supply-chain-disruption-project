"""Data-quality checks. Each check returns a dict; results are logged to Delta (gold dq_results).

severity "error" fails the run when strict=True; "warn" is reported only.
Run:  python -m src.quality
"""
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F

from config.pipeline_config import get_spark, gold_path, silver_path


class DataQualityError(Exception):
    pass


def _result(table: str, check: str, failed: int, severity: str) -> dict:
    return {"table": table, "check": check, "severity": severity,
            "failed_rows": int(failed), "passed": failed == 0}


def check_not_null(df: DataFrame, table: str, cols: list, severity: str = "error") -> dict:
    cond = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols])
    return _result(table, f"not_null({','.join(cols)})", df.filter(cond).count(), severity)


def check_unique(df: DataFrame, table: str, cols: list, severity: str = "error") -> dict:
    return _result(table, f"unique({','.join(cols)})", df.count() - df.dropDuplicates(cols).count(), severity)


def check_range(df: DataFrame, table: str, col: str, min_v, max_v, severity: str = "error") -> dict:
    failed = df.filter((F.col(col) < min_v) | (F.col(col) > max_v)).count()
    return _result(table, f"range({col},{min_v},{max_v})", failed, severity)


def check_accepted_values(df: DataFrame, table: str, col: str, values: list, severity: str = "error") -> dict:
    failed = df.filter(F.col(col).isNull() | ~F.col(col).isin(values)).count()
    return _result(table, f"accepted_values({col})", failed, severity)


def check_referential(df: DataFrame, ref_df: DataFrame, table: str, col: str, ref_col: str,
                      ref_table: str, severity: str = "error") -> dict:
    ref = ref_df.select(F.col(ref_col).alias("_ref")).distinct()
    failed = df.join(ref, df[col] == F.col("_ref"), "left_anti").count()
    return _result(table, f"foreign_key({col}->{ref_table}.{ref_col})", failed, severity)


def silver_checks(spark: SparkSession) -> list:
    r = lambda n: spark.read.format("delta").load(silver_path(n))
    sup, prod, cust, wh = r("suppliers"), r("products"), r("customers"), r("warehouses")
    orders, shp, inv, ev = r("orders"), r("shipments"), r("inventory"), r("shipment_events")
    return [
        check_unique(sup, "suppliers", ["supplier_id"]),
        check_not_null(sup, "suppliers", ["supplier_id", "supplier_name", "country"]),
        check_range(sup, "suppliers", "reliability_score", 0.0, 1.0),
        check_range(sup, "suppliers", "tier", 1, 3),
        check_accepted_values(sup, "suppliers", "status", ["ACTIVE", "SUSPENDED", "INACTIVE"]),
        check_unique(prod, "products", ["product_id"]),
        check_referential(prod, sup, "products", "supplier_id", "supplier_id", "suppliers"),
        check_unique(orders, "orders", ["order_id"]),
        check_not_null(orders, "orders", ["customer_id", "product_id", "warehouse_id"]),
        check_range(orders, "orders", "quantity", 1, 10000),
        check_referential(orders, cust, "orders", "customer_id", "customer_id", "customers"),
        check_referential(orders, prod, "orders", "product_id", "product_id", "products"),
        check_referential(orders, wh, "orders", "warehouse_id", "warehouse_id", "warehouses"),
        check_unique(shp, "shipments", ["shipment_id"]),
        check_referential(shp, orders, "shipments", "order_id", "order_id", "orders"),
        check_referential(shp, sup, "shipments", "supplier_id", "supplier_id", "suppliers"),
        check_accepted_values(shp, "shipments", "status", ["DELIVERED", "DELAYED", "IN_TRANSIT"]),
        check_range(shp, "shipments", "delay_days", -10, 60, severity="warn"),
        check_unique(inv, "inventory", ["warehouse_id", "product_id"]),
        check_range(inv, "inventory", "quantity_on_hand", 0, 1000000),
        check_unique(ev, "shipment_events", ["event_id"]),
        check_accepted_values(ev, "shipment_events", "event_type",
                              ["DEPARTED", "PORT_ARRIVAL", "CUSTOMS_HOLD", "DELAYED", "DELIVERED"]),
    ]


def gold_checks(spark: SparkSession) -> list:
    risk = spark.read.format("delta").load(gold_path("gold_supplier_risk"))
    inv = spark.read.format("delta").load(gold_path("gold_inventory_risk"))
    return [
        check_unique(risk, "gold_supplier_risk", ["supplier_id"]),
        check_range(risk, "gold_supplier_risk", "risk_score", 0.0, 100.0),
        check_range(risk, "gold_supplier_risk", "delay_rate", 0.0, 1.0),
        check_accepted_values(risk, "gold_supplier_risk", "risk_tier", ["HIGH", "MEDIUM", "LOW"]),
        check_unique(inv, "gold_inventory_risk", ["warehouse_id", "product_id"]),
        check_accepted_values(inv, "gold_inventory_risk", "priority", ["CRITICAL", "WATCH", "OK"]),
    ]


def _persist(spark: SparkSession, layer: str, results: list) -> None:
    ts = datetime.now(timezone.utc)
    rows = [(ts, layer, r["table"], r["check"], r["severity"], r["failed_rows"], r["passed"]) for r in results]
    schema = ("run_ts timestamp, layer string, table_name string, check_name string, "
              "severity string, failed_rows int, passed boolean")
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").save(gold_path("dq_results"))


def _run(spark: SparkSession, layer: str, results: list, strict: bool) -> list:
    _persist(spark, layer, results)
    failed = [r for r in results if not r["passed"]]
    print(f"[quality:{layer}] {len(results) - len(failed)}/{len(results)} checks passed")
    for r in failed:
        print(f"  FAIL [{r['severity']}] {r['table']}.{r['check']}: {r['failed_rows']} rows")
    errors = [r for r in failed if r["severity"] == "error"]
    if strict and errors:
        raise DataQualityError(f"{len(errors)} data-quality error(s) in {layer} layer")
    return results


def run_silver_checks(spark: SparkSession, strict: bool = True) -> list:
    return _run(spark, "silver", silver_checks(spark), strict)


def run_gold_checks(spark: SparkSession, strict: bool = True) -> list:
    return _run(spark, "gold", gold_checks(spark), strict)


if __name__ == "__main__":
    s = get_spark("quality")
    run_silver_checks(s)
    run_gold_checks(s)
