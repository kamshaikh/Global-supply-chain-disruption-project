# Architecture

Synthetic data only, local Spark + Delta Lake, no cloud and no credentials.

```
data/raw (CSV/JSON) ─┐
data/cdc (CSV)       ├─► Bronze ─► Silver ─┬─► SCD2 dim_supplier_scd2 ─┐
data/streaming (JSON)┘   (as-is)  (typed)  └─► CDC MERGE into silver ──┴─► Gold ─► DQ checks
                                                                         ▲
data/streaming ─► Structured Streaming ─► gold_event_metrics (MERGE) ────┘
```

## Layers (`lakehouse/`, created at run time)

| Layer | Module | Output |
|---|---|---|
| Bronze | `src/bronze.py` | One Delta table per source (raw strings + `_source`, `_ingested_at`), including CDC feeds |
| Silver | `src/silver.py` | Typed, trimmed, de-duplicated, validated tables; shipments get `delay_days`, `is_delayed` |
| SCD2 | `src/scd2.py` | `dim_supplier_scd2`: baseline + ordered change feed, `effective_from/to`, `is_current` |
| CDC | `src/cdc.py` | Delta `MERGE` of supplier (I/U/D) and shipment (U) changes into silver |
| Quality | `src/quality.py` | not-null, unique, range, accepted-values, FK checks; results in `gold/dq_results` |
| Gold | `src/gold.py` | `gold_supplier_risk`, `gold_shipment_performance`, `gold_inventory_risk`, `gold_disruption_impact` |
| Streaming | `src/streaming.py` | `gold_event_metrics` (per day + event type), `availableNow` trigger, `foreachBatch` + `MERGE` |

Orchestration: `src/run_pipeline.py`. Config, paths and risk weights: `config/pipeline_config.py`.

## Run order and why
1. Bronze keeps the unchanged baseline, so SCD2 can rebuild history deterministically from it.
2. Silver is rebuilt from bronze; CDC then brings silver to current state.
3. Quality gates silver (errors stop the run) before gold is built.
4. Gold reads post-CDC silver and the SCD2 dimension (`profile_changes`).

## Risk score (0-100)
`40·min(delay_rate/0.5,1) + 25·(1−reliability) + 20·(max active disruption severity/5) + 15·min(lead_time/60,1)`
Tiers: HIGH ≥ 50, MEDIUM ≥ 30, else LOW. Active disruptions are evaluated at `REFERENCE_DATE` (2025-06-30).

## Other artifacts
- `dlt/supply_chain_pipeline.py`: Delta Live Tables equivalent (Databricks runtime; not deployed).
- `sql/`: Spark SQL queries over the local Delta tables.
- `notebooks/`: walkthrough and gold analytics.
