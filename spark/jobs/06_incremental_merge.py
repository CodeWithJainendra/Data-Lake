#!/usr/bin/env python3
"""
Stage 6 — Incremental MERGE (CDC pattern)

Demonstrates the production pattern for incremental loads: instead of
overwriting `fact_claims` every night, MERGE in only changed rows from the
last 24 hours.

We use a simple "left-anti + union" trick that works on plain Parquet without
needing Iceberg/Delta Lake. In production you'd swap this for an Iceberg
`MERGE INTO ... USING ... WHEN MATCHED THEN UPDATE` statement (one extra config
line, identical job code).

Run this with a `--since YYYY-MM-DD` argument or it defaults to 24 hours ago.
"""
import argparse
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from _common import get_spark, write_parquet, PROCESSED_BASE, CURATED_BASE


def merge_claims(spark, since_date):
    print(f"\n[CDC] Merging claims changed since {since_date}")

    target_path = f"{CURATED_BASE}/tables/fact_claims"

    # Read existing curated fact_claims
    target = spark.read.parquet(target_path)
    print(f"  target rows (before): {target.count():,}")

    # Source: only claims with submitted_date >= since_date
    source = (spark.read.parquet(f"{PROCESSED_BASE}/tables/claims")
              .filter(F.col("submitted_date") >= F.lit(since_date)))
    print(f"  changed rows in source: {source.count():,}")

    if source.count() == 0:
        print("  no changes — done.")
        return

    # Anti-join: keep target rows whose claim_id is NOT in source
    target_kept = target.join(
        source.select("claim_id"), on="claim_id", how="left_anti"
    )

    # Re-join source with reference dims for the curated schema
    enc      = spark.read.parquet(f"{PROCESSED_BASE}/tables/encounters")\
                  .select("encounter_id", "department", "encounter_type", "facility", "provider_id")
    patients = spark.read.parquet(f"{PROCESSED_BASE}/tables/patients")\
                  .select("patient_id", "age", "sex", "state")
    payers   = spark.read.parquet(f"{PROCESSED_BASE}/tables/payers")\
                  .select(F.col("payer_id"), "payer_name", "payer_type")

    source_curated = (source
        .join(enc,      "encounter_id", "left")
        .join(patients, "patient_id",   "left")
        .join(payers,   "payer_id",     "left")
        .withColumn("is_denied",   F.col("claim_status") == "denied")
        .withColumn("is_paid",     F.col("claim_status") == "paid")
        .withColumn("net_payment",
                    F.coalesce(F.col("paid_amount"), F.lit(0.0)) - F.coalesce(F.col("adjustment_amount"), F.lit(0.0)))
        .withColumn("_source_file",      F.lit(f"merge:{since_date}"))
        .withColumn("_ingested_at",      F.current_timestamp())
        .withColumn("_pipeline_version", F.lit("1.1.0"))
    )

    # Union: kept target + newly inserted/updated rows
    merged = target_kept.unionByName(source_curated, allowMissingColumns=True)
    print(f"  target rows (after):  {merged.count():,}")

    # Atomic-ish write: write to staging path then swap
    staging = f"{target_path}_staging"
    write_parquet(merged, staging, partition_by=["submitted_year", "submitted_month"])

    # In real Iceberg/Delta this is one atomic commit. For plain Parquet,
    # we'd swap the LOCATION on the Hive table to point at staging — for the
    # demo we just keep both around.
    print(f"  ✓ wrote merged set to {staging}")
    print(f"  (Next: swap LOCATION on curated.fact_claims to staging, then delete old.)")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default=(datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d"))
    args = p.parse_args()

    spark = get_spark("06_incremental_merge")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 6 — INCREMENTAL MERGE (CDC)")
    print("=" * 70)
    merge_claims(spark, args.since)
    print("\n" + "=" * 70)
    print("STAGE 6 COMPLETE")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
