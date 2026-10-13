"""Unit tests. Run from project root:  pytest -q

Pure-transformation tests need only PySpark; CDC / SCD2 tests need Delta Lake (skipped if unavailable).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from src import cdc, gold, quality, scd2, silver
from src.generate_data import build_datasets

SUP_SCHEMA = ("supplier_id string, supplier_name string, country string, tier string, "
              "reliability_score string, lead_time_days string, status string, updated_at string")
CHG_SCHEMA = ("change_id string, supplier_id string, op string, changed_at string, supplier_name string, "
              "country string, tier string, reliability_score string, lead_time_days string, status string")


@pytest.fixture(scope="session")
def spark():
    from config.pipeline_config import get_spark
    try:
        s = get_spark("tests", with_delta=True)
    except Exception:
        s = get_spark("tests", with_delta=False)
    yield s
    s.stop()


@pytest.fixture
def delta_spark(spark):
    if "DeltaSparkSessionExtension" not in spark.conf.get("spark.sql.extensions", ""):
        pytest.skip("Delta Lake not available in this environment")
    return spark


def _base_suppliers(spark):
    return silver.clean_suppliers(spark.createDataFrame([
        ("S001", "A", "India", "1", "0.9", "10", "ACTIVE", "2025-01-01 00:00:00"),
        ("S002", "B", "China", "2", "0.8", "15", "ACTIVE", "2025-01-01 00:00:00")], SUP_SCHEMA))


# ---------------- data generator ----------------
def test_generator_is_deterministic_and_complete():
    a, b = build_datasets(), build_datasets()
    assert a == b
    assert len(a["suppliers"]) == 20 and len(a["shipments"]) == 200 and len(a["orders"]) == 205
    assert {c["op"] for c in a["supplier_changes"]} == {"I", "U", "D"}


# ---------------- silver ----------------
def test_clean_suppliers_trims_types_and_dedupes(spark):
    row = (" S001 ", "Acme ", " India", "1", "0.9", "10", "active", "2025-01-01 00:00:00")
    df = spark.createDataFrame([row, row, (None,) + row[1:]], SUP_SCHEMA)
    out = silver.clean_suppliers(df)
    assert out.count() == 1
    r = out.first()
    assert (r.supplier_id, r.country, r.status, r.tier) == ("S001", "India", "ACTIVE", 1)


def test_clean_orders_drops_invalid_and_duplicates(spark):
    schema = ("order_id string, customer_id string, product_id string, warehouse_id string, "
              "order_date string, quantity string, order_status string")
    ok = ("O1", "C1", "P1", "W1", "2025-01-02", "10", " shipped")
    df = spark.createDataFrame([ok, ok, ("O2", None, "P1", "W1", "2025-01-02", "5", "PENDING"),
                                ("O3", "C1", "P1", "W1", "2025-01-02", "-5", "PENDING"),
                                ("O4", "C2", "P2", "W2", "2025-01-03", "7", "Completed")], schema)
    out = {r.order_id: r for r in silver.clean_orders(df).collect()}
    assert set(out) == {"O1", "O4"}
    assert out["O1"].order_status == "SHIPPED"


def test_clean_shipments_delay_days(spark):
    schema = ("shipment_id string, order_id string, supplier_id string, origin_country string, "
              "destination_country string, carrier string, ship_date string, planned_arrival string, "
              "actual_arrival string, status string")
    base = ("O1", "S001", "India", "USA", "OceanLine", "2025-05-20", "2025-06-01")
    df = spark.createDataFrame([("SH1",) + base + ("2025-06-04", "delayed"),
                                ("SH2",) + base + (None, "in_transit"),
                                ("SH3",) + base + ("2025-05-31", "delivered")], schema)
    out = {r.shipment_id: r for r in silver.clean_shipments(df).collect()}
    assert out["SH1"].delay_days == 3 and out["SH1"].is_delayed is True
    assert out["SH2"].delay_days is None and out["SH2"].is_delayed is False
    assert out["SH3"].delay_days == -1 and out["SH3"].is_delayed is False


# ---------------- gold ----------------
def test_risk_score_bounds_and_ordering(spark):
    df = spark.createDataFrame(
        [("worst", 1.0, 0.0, 5, 60), ("best", 0.0, 1.0, 0, 0), ("mid", 0.25, 0.8, 3, 30)],
        "supplier_id string, delay_rate double, reliability_score double, "
        "max_disruption_severity int, lead_time_days int")
    r = {x.supplier_id: x for x in gold.compute_risk_score(df).collect()}
    assert r["worst"].risk_score == 100.0 and r["worst"].risk_tier == "HIGH"
    assert r["best"].risk_score == 0.0 and r["best"].risk_tier == "LOW"
    assert 0 < r["mid"].risk_score < 100
    assert r["worst"].risk_score > r["mid"].risk_score > r["best"].risk_score


# ---------------- quality ----------------
def test_quality_checks(spark):
    df = spark.createDataFrame([("a", 1), ("a", 2), (None, 500)], "k string, v int")
    ref = spark.createDataFrame([("a",)], "k string")
    assert quality.check_not_null(df, "t", ["k"])["failed_rows"] == 1
    assert quality.check_unique(df, "t", ["k"])["failed_rows"] == 1
    assert quality.check_range(df, "t", "v", 0, 100)["failed_rows"] == 1
    assert quality.check_accepted_values(df, "t", "k", ["a"])["failed_rows"] == 1
    assert quality.check_referential(df, ref, "t", "k", "k", "ref")["failed_rows"] == 1
    assert quality.check_range(df, "t", "v", 0, 1000)["passed"] is True


# ---------------- CDC / SCD2 (Delta) ----------------
def test_cdc_merge_insert_update_delete(delta_spark, tmp_path):
    path = str(tmp_path / "suppliers")
    _base_suppliers(delta_spark).write.format("delta").save(path)
    changes = cdc.prepare_supplier_changes(delta_spark.createDataFrame([
        ("C1", "S001", "U", "2025-02-01 00:00:00", "A", "India", "1", "0.5", "10", "ACTIVE"),
        ("C2", "S002", "D", "2025-02-01 00:00:00", "B", "China", "2", "0.8", "15", "ACTIVE"),
        ("C3", "S003", "I", "2025-02-01 00:00:00", "C", "Turkey", "2", "0.8", "15", "ACTIVE")], CHG_SCHEMA))
    cdc.apply_supplier_changes(delta_spark, changes, path)
    rows = {r.supplier_id: r for r in delta_spark.read.format("delta").load(path).collect()}
    assert set(rows) == {"S001", "S003"}
    assert rows["S001"].reliability_score == 0.5


def test_scd2_history(delta_spark, tmp_path):
    path = str(tmp_path / "dim")
    scd2.init_dim(_base_suppliers(delta_spark), path)
    changes = cdc.prepare_supplier_changes(delta_spark.createDataFrame([
        ("C1", "S001", "U", "2025-02-01 00:00:00", "A", "India", "1", "0.7", "10", "ACTIVE"),
        ("C2", "S001", "U", "2025-03-01 00:00:00", "A", "India", "1", "0.5", "10", "ACTIVE"),
        ("C3", "S002", "D", "2025-02-15 00:00:00", "B", "China", "2", "0.8", "15", "ACTIVE"),
        ("C4", "S003", "I", "2025-02-20 00:00:00", "C", "Turkey", "2", "0.8", "15", "ACTIVE")], CHG_SCHEMA))
    scd2.apply_changes(delta_spark, changes, path)
    dim = delta_spark.read.format("delta").load(path)
    s1 = dim.filter("supplier_id = 'S001'").collect()
    assert len(s1) == 3 and sum(r.is_current for r in s1) == 1
    assert [r for r in s1 if r.is_current][0].reliability_score == 0.5
    s2 = dim.filter("supplier_id = 'S002'").collect()
    assert len(s2) == 1 and s2[0].is_current is False and s2[0].effective_to is not None
    assert dim.filter("supplier_id = 'S003' AND is_current").count() == 1
    past = {r.supplier_id: r for r in scd2.supplier_as_of(delta_spark, "2025-02-10 00:00:00", path).collect()}
    assert past["S001"].reliability_score == 0.7 and "S003" not in past
