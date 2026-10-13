"""End-to-end pipeline: Raw -> Bronze -> Silver -> SCD2 -> CDC -> Quality -> Gold -> Quality -> Streaming.

Run from the project root:  python -m src.run_pipeline
"""
import argparse
import shutil

from config.pipeline_config import LAKE_DIR, RAW_DIR, get_spark
from src import bronze, cdc, generate_data, gold, quality, scd2, silver, streaming


def main(regenerate: bool = True) -> None:
    if regenerate or not (RAW_DIR / "suppliers.csv").exists():
        generate_data.main()
    shutil.rmtree(LAKE_DIR, ignore_errors=True)  # clean, reproducible run
    spark = get_spark()

    bronze.ingest_all(spark)                 # Raw  -> Bronze
    silver.build_silver(spark)               # Bronze -> Silver
    scd2.build_scd2(spark)                   # supplier history from bronze baseline + change feed
    cdc.run_cdc(spark)                       # Delta MERGE of change feed into silver
    quality.run_silver_checks(spark)         # gate before gold
    gold.build_gold(spark)                   # Silver -> Gold + risk score
    quality.run_gold_checks(spark)
    streaming.run_streaming(spark)           # Structured Streaming over event batches
    print("Pipeline finished. Lakehouse:", LAKE_DIR)
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-regenerate", action="store_true", help="reuse existing synthetic files")
    main(regenerate=not ap.parse_args().no_regenerate)
