"""Synthetic data generator (stdlib only, deterministic).

Writes raw CSV/JSON, CDC change files and streaming event batches.
Run:  python -m src.generate_data
"""
import csv
import json
import random
from datetime import date, datetime, timedelta

from config.pipeline_config import CDC_DIR, RAW_DIR, REFERENCE_DATE, SEED, STREAMING_DIR

REF = date.fromisoformat(REFERENCE_DATE)
BASE_TS = "2025-01-01 00:00:00"
COUNTRIES = ["China", "Vietnam", "India", "Germany", "Mexico",
             "USA", "Brazil", "Taiwan", "Turkey", "Indonesia"]
SUPPLIER_COLS = ["supplier_id", "supplier_name", "country", "tier",
                 "reliability_score", "lead_time_days", "status"]


def build_datasets(seed: int = SEED) -> dict:
    """Return every dataset as lists of dicts (no I/O)."""
    rnd = random.Random(seed)

    # ---- suppliers (S001..S020) ----
    suppliers = []
    for i in range(1, 21):
        suppliers.append({
            "supplier_id": f"S{i:03d}", "supplier_name": f"Supplier {i:02d} Industries",
            "country": COUNTRIES[(i - 1) % len(COUNTRIES)], "tier": (i % 3) + 1,
            "reliability_score": round(rnd.uniform(0.60, 0.98), 2),
            "lead_time_days": rnd.randint(7, 45), "status": "ACTIVE", "updated_at": BASE_TS,
        })
    sup = {s["supplier_id"]: s for s in suppliers}
    sup["S003"].update(reliability_score=0.95, lead_time_days=20)

    # ---- products (use S001..S019 only; S020 stays an orphan supplier) ----
    cats = ["Electronics", "Apparel", "Machinery", "Chemicals", "Food", "Furniture"]
    products = []
    for i in range(1, 31):
        cost = round(rnd.uniform(5, 200), 2)
        products.append({
            "product_id": f"P{i:03d}", "product_name": f"Product {i:03d}",
            "category": cats[i % len(cats)], "supplier_id": f"S{((i - 1) % 19) + 1:03d}",
            "unit_cost": cost, "unit_price": round(cost * rnd.uniform(1.2, 1.8), 2),
        })
    prod = {p["product_id"]: p for p in products}

    # ---- customers ----
    regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
    segments = ["Retail", "Enterprise", "Wholesale"]
    customers = [{"customer_id": f"C{i:03d}", "customer_name": f"Customer {i:03d}",
                  "region": regions[i % 4], "segment": segments[i % 3]} for i in range(1, 26)]

    # ---- warehouses ----
    wh_countries = [("USA", "North America"), ("Germany", "Europe"), ("Singapore", "Asia Pacific"),
                    ("Brazil", "Latin America"), ("India", "Asia Pacific"), ("Netherlands", "Europe")]
    warehouses = [{"warehouse_id": f"W{i:02d}", "warehouse_name": f"Warehouse {c}", "country": c,
                   "region": r, "capacity_units": rnd.randint(5000, 20000)}
                  for i, (c, r) in enumerate(wh_countries, 1)]

    # ---- orders (200 clean + injected dirty rows) ----
    orders = []
    for i in range(1, 201):
        orders.append({
            "order_id": f"O{i:04d}", "customer_id": rnd.choice(customers)["customer_id"],
            "product_id": rnd.choice(products)["product_id"],
            "warehouse_id": rnd.choice(warehouses)["warehouse_id"],
            "order_date": (date(2025, 1, 1) + timedelta(days=rnd.randint(0, 165))).isoformat(),
            "quantity": rnd.randint(5, 500),
            "order_status": rnd.choice(["COMPLETED", "shipped", " PENDING", "Completed"]),
        })
    clean_orders = list(orders)
    orders += [dict(orders[0]), dict(orders[5]), dict(orders[9])]                       # duplicates
    orders.append({**orders[1], "order_id": "O0201", "customer_id": ""})                # null key
    orders.append({**orders[2], "order_id": "O0202", "quantity": -5})                   # bad quantity

    # ---- shipments (one per clean order) ----
    wh = {w["warehouse_id"]: w for w in warehouses}
    carriers = ["OceanLine", "AirExpress", "RailFreight", "TruckCo"]
    shipments = []
    for i, o in enumerate(clean_orders, 1):
        s = sup[prod[o["product_id"]]["supplier_id"]]
        ship = date.fromisoformat(o["order_date"]) + timedelta(days=rnd.randint(1, 5))
        planned = ship + timedelta(days=rnd.randint(5, 25))
        row = {"shipment_id": f"SH{i:04d}", "order_id": o["order_id"], "supplier_id": s["supplier_id"],
               "origin_country": s["country"], "destination_country": wh[o["warehouse_id"]]["country"],
               "carrier": rnd.choice(carriers), "ship_date": ship.isoformat(),
               "planned_arrival": planned.isoformat(), "actual_arrival": "", "status": "IN_TRANSIT"}
        if planned <= REF:
            late = rnd.random() < (1 - s["reliability_score"]) + 0.05
            delay = rnd.randint(1, 12) if late else rnd.choice([-1, 0, 0])
            row["actual_arrival"] = (planned + timedelta(days=delay)).isoformat()
            row["status"] = "DELAYED" if delay > 0 else "DELIVERED"
        shipments.append(row)

    # ---- inventory ----
    inventory = [{"warehouse_id": w["warehouse_id"], "product_id": p["product_id"],
                  "quantity_on_hand": rnd.randint(0, 800), "reorder_point": rnd.randint(50, 250),
                  "snapshot_date": REFERENCE_DATE} for w in warehouses for p in products]

    # ---- disruptions ----
    d = [("Port Congestion", "China", "Asia Pacific", 4, "2025-06-10", "2025-07-20"),
         ("Typhoon", "Taiwan", "Asia Pacific", 5, "2025-06-25", "2025-07-05"),
         ("Labor Strike", "Germany", "Europe", 3, "2025-05-01", "2025-05-20"),
         ("Flooding", "India", "Asia Pacific", 4, "2025-06-20", "2025-07-15"),
         ("Export Restriction", "Vietnam", "Asia Pacific", 3, "2025-03-01", "2025-04-15"),
         ("Cyberattack", "Mexico", "North America", 2, "2025-06-28", "2025-07-10"),
         ("Rail Strike", "USA", "North America", 2, "2025-02-10", "2025-02-20"),
         ("Drought", "Brazil", "Latin America", 3, "2025-06-01", "2025-08-31")]
    disruptions = [{"disruption_id": f"D{i:03d}", "disruption_type": t, "country": c, "region": r,
                    "severity": sv, "start_date": s, "end_date": e,
                    "description": f"{t} affecting logistics in {c}"}
                   for i, (t, c, r, sv, s, e) in enumerate(d, 1)]

    # ---- shipment events (recent shipments only) ----
    events = []
    recent = [s for s in shipments if s["ship_date"] >= "2025-06-01"][:40]
    for s in recent:
        t0 = datetime.fromisoformat(s["ship_date"]) + timedelta(hours=8 + rnd.randint(0, 6))
        seq = [("DEPARTED", t0, s["origin_country"], 0),
               ("PORT_ARRIVAL", t0 + timedelta(days=2, hours=rnd.randint(0, 8)), s["origin_country"], 0)]
        if rnd.random() < 0.3:
            seq.append(("CUSTOMS_HOLD", t0 + timedelta(days=3), s["destination_country"], rnd.randint(24, 72)))
        if rnd.random() < 0.2:
            seq.append(("DELAYED", t0 + timedelta(days=4), s["destination_country"], rnd.randint(12, 96)))
        if s["actual_arrival"]:
            seq.append(("DELIVERED", datetime.fromisoformat(s["actual_arrival"]) + timedelta(hours=14),
                        s["destination_country"], 0))
        for etype, ts, loc, dh in seq:
            events.append({"shipment_id": s["shipment_id"], "event_type": etype,
                           "event_time": ts.isoformat(timespec="seconds"), "location": loc, "delay_hours": dh})
    events.sort(key=lambda e: (e["event_time"], e["shipment_id"]))
    for i, e in enumerate(events, 1):
        e["event_id"] = f"E{i:05d}"
    events = [{k: e[k] for k in ["event_id", "shipment_id", "event_type", "event_time", "location", "delay_hours"]}
              for e in events]

    # ---- CDC: supplier changes (full row + op + changed_at) ----
    cdc_suppliers, cur, n = [], {k: dict(v) for k, v in sup.items()}, 0

    def change(sid, op, at, **upd):
        nonlocal n
        n += 1
        base = {**cur.get(sid, {"supplier_id": sid}), **upd}
        cur[sid] = base
        row = {"change_id": f"CH{n:03d}", "supplier_id": sid, "op": op, "changed_at": at}
        row.update({c: base[c] for c in SUPPLIER_COLS[1:]})
        cdc_suppliers.append(row)

    change("S003", "U", "2025-03-15 09:00:00", reliability_score=0.80)
    change("S003", "U", "2025-05-10 09:00:00", reliability_score=0.62, lead_time_days=30)
    change("S007", "U", "2025-04-02 12:00:00", status="SUSPENDED")
    change("S011", "U", "2025-04-20 08:30:00", lead_time_days=40, tier=3)
    change("S021", "I", "2025-05-05 10:00:00", supplier_name="Supplier 21 Industries", country="Turkey",
           tier=2, reliability_score=0.88, lead_time_days=18, status="ACTIVE")
    change("S020", "D", "2025-06-01 00:00:00")
    change("S015", "U", "2025-06-12 14:00:00", reliability_score=0.55)

    # ---- CDC: shipment status changes for in-transit shipments ----
    cdc_shipments = []
    transit = [s for s in shipments if s["status"] == "IN_TRANSIT"][:8]
    for i, s in enumerate(transit, 1):
        planned = date.fromisoformat(s["planned_arrival"])
        arrival = planned + timedelta(days=rnd.randint(0, 6))
        cdc_shipments.append({"change_id": f"SC{i:03d}", "shipment_id": s["shipment_id"], "op": "U",
                              "changed_at": f"{arrival.isoformat()} 12:00:00",
                              "status": "DELAYED" if arrival > planned else "DELIVERED",
                              "actual_arrival": arrival.isoformat()})

    return {"suppliers": suppliers, "products": products, "customers": customers,
            "warehouses": warehouses, "orders": orders, "shipments": shipments,
            "inventory": inventory, "disruptions": disruptions, "shipment_events": events,
            "supplier_changes": cdc_suppliers, "shipment_changes": cdc_shipments}


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)


def main() -> dict:
    data = build_datasets()
    for name in ["suppliers", "products", "customers", "warehouses", "orders", "shipments", "inventory"]:
        _write_csv(RAW_DIR / f"{name}.csv", data[name])
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_DIR / "disruptions.json").write_text(json.dumps(data["disruptions"], indent=2))
    _write_jsonl(RAW_DIR / "shipment_events.json", data["shipment_events"])
    _write_csv(CDC_DIR / "supplier_changes.csv", data["supplier_changes"])
    _write_csv(CDC_DIR / "shipment_changes.csv", data["shipment_changes"])

    STREAMING_DIR.mkdir(parents=True, exist_ok=True)
    for old in STREAMING_DIR.glob("shipment_events_batch_*.json"):
        old.unlink()
    ev, k = data["shipment_events"], 3
    size = -(-len(ev) // k)
    for b in range(k):
        _write_jsonl(STREAMING_DIR / f"shipment_events_batch_{b + 1}.json", ev[b * size:(b + 1) * size])

    print({name: len(rows) for name, rows in data.items()})
    return data


if __name__ == "__main__":
    main()
