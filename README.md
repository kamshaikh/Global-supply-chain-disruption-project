# global-supply-chain-disruption

PySpark + Delta Lake medallion pipeline on **synthetic** supply-chain data:
Raw → Bronze → Silver → SCD2 / CDC → Gold, with data-quality checks, a supplier risk score and Structured Streaming.
Runs locally. No credentials, no cloud deployment.

## Quick start
Requires Python 3.9+ and Java 17 or 21. First run needs internet (Delta jars are downloaded).

```bash
pip install -r requirements.txt
python -m src.run_pipeline      # generates data, runs every stage, writes ./lakehouse
pytest -q                       # unit tests (Delta tests skip if Delta is unavailable)
```

## Layout
```
config/pipeline_config.py   paths, thresholds, risk weights, Spark factory
data/{raw,cdc,streaming}    synthetic CSV/JSON (regenerate: python -m src.generate_data)
src/                        generate_data, bronze, silver, cdc, scd2, gold, streaming, quality, run_pipeline
tests/                      test_transformations.py
sql/                        gold, SCD2 and data-quality queries
notebooks/                  pipeline walkthrough, gold analytics
dlt/                        Delta Live Tables version (Databricks runtime, not deployed)
docs/                       architecture.md, interview_notes.md
```

## Outputs (`lakehouse/`)
- `bronze/*`, `silver/*` (incl. `dim_supplier_scd2`)
- `gold/gold_supplier_risk`, `gold_shipment_performance`, `gold_inventory_risk`, `gold_disruption_impact`, `gold_event_metrics`, `dq_results`

## Risk score
0–100 from delay rate (40), supplier reliability (25), active disruption severity (20) and lead time (15).
HIGH ≥ 50, MEDIUM ≥ 30. Weights live in `config/pipeline_config.py`.

See `docs/architecture.md` for details.
