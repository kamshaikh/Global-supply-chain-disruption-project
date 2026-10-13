# Interview notes

## 30-second pitch
A medallion lakehouse on PySpark + Delta that turns synthetic orders, shipments, supplier changes and
shipment events into a supplier risk score and inventory-risk view, with CDC, SCD2 history, data-quality gates and streaming.

## Design decisions to talk through
- **Bronze as untouched strings:** no schema surprises at ingest; typing happens in silver.
- **Pure transformation functions** (`clean_*`, `compute_risk_score`) make logic unit-testable without Delta.
- **CDC via Delta `MERGE`:** keep only the latest change per key; guard updates with `changed_at >= updated_at` so replays are idempotent.
- **SCD2 via staged MERGE:** each change appears twice in the source, once keyed (closes the current row) and once with a null key (inserts the new version). Changes are applied in per-supplier rank order so several changes for one supplier are handled correctly. Deletes only close the row.
- **Rebuild from bronze baseline:** the SCD2 table is reproducible, unaffected by CDC mutating silver.
- **Quality severities:** `error` blocks the pipeline, `warn` only reports; results stored in a Delta table for trend analysis.
- **Streaming:** file source, `maxFilesPerTrigger=1`, dedupe by `event_id`, `foreachBatch` MERGE to accumulate counts; `availableNow` makes it testable and batch-like.
- **Deterministic synthetic data:** fixed seed and fixed reference date, so tests and demos are repeatable.

## Likely questions
- *Why MERGE for streaming aggregates?* Delta sinks don't support update mode; MERGE gives upserts with exactly-once per batch under checkpointing.
- *How would you handle late events?* Add a watermark and windowed state, or reprocess affected windows in the MERGE.
- *How would this scale?* Partition by date, Z-ORDER on join keys, Auto Loader for ingest, DLT (see `dlt/`) for orchestration and expectations.
- *Risk score limitations?* Hand-tuned weights; next step is calibrating against actual delay outcomes.
- *Out-of-order CDC?* Latest-by-`changed_at` selection plus the update guard; SCD2 orders by `changed_at`.

## Known simplifications
Single-node local Spark; one disruption-severity driver (max) in the score; no schema evolution handling; DLT file is illustrative and untested here.
