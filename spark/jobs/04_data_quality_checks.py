#!/usr/bin/env python3
"""
Stage 4 — Data Quality framework

Per-table checks (row_count, null %, duplicate %, freshness) written to
s3://curated/tables/dq_metrics/ — powers the "Data Health" Superset dashboard.

Null check is now a SINGLE aggregation per table (was N-pass; see _common.null_pct_per_column).
"""
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.utils import AnalysisException

from _common import (
    get_spark, add_lineage_columns, null_pct_per_column,
    PROCESSED_BASE, CURATED_BASE,
)

# Retain only the last N days of DQ history. Older rows are discarded each run.
# Production tip: in real Iceberg / Delta this is a TIME TRAVEL retention policy.
DQ_RETENTION_DAYS = 30


DQ_SCHEMA = StructType([
    StructField("run_id",            StringType(), True),
    StructField("run_timestamp",     StringType(), True),
    StructField("zone",              StringType(), True),
    StructField("table_name",        StringType(), True),
    StructField("row_count",         LongType(),   True),
    StructField("duplicate_count",   LongType(),   True),
    StructField("duplicate_pct",     DoubleType(), True),
    StructField("worst_null_column", StringType(), True),
    StructField("worst_null_pct",    DoubleType(), True),
    StructField("freshness",         StringType(), True),
    StructField("status",            StringType(), True),
    StructField("notes",             StringType(), True),
])


CHECKS = {
    "patients":   {"path": f"{PROCESSED_BASE}/tables/patients",   "pk": "patient_id",   "zone": "processed"},
    "providers":  {"path": f"{PROCESSED_BASE}/tables/providers",  "pk": "provider_id",  "zone": "processed"},
    "encounters": {"path": f"{PROCESSED_BASE}/tables/encounters", "pk": "encounter_id", "zone": "processed"},
    "claims":     {"path": f"{PROCESSED_BASE}/tables/claims",     "pk": "claim_id",     "zone": "processed"},
    "diagnoses":  {"path": f"{PROCESSED_BASE}/tables/diagnoses",  "pk": "diagnosis_id", "zone": "processed"},
    "procedures": {"path": f"{PROCESSED_BASE}/tables/procedures", "pk": "procedure_id", "zone": "processed"},
    "fact_encounters": {"path": f"{CURATED_BASE}/tables/fact_encounters", "pk": "encounter_id", "zone": "curated"},
    "fact_claims":     {"path": f"{CURATED_BASE}/tables/fact_claims",     "pk": "claim_id",     "zone": "curated"},
}


def run_checks(spark):
    rows = []
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    for table_name, cfg in CHECKS.items():
        print(f"\n[DQ] {table_name} ──────────────────────────")
        try:
            df = spark.read.parquet(cfg["path"])
        except Exception as e:
            print(f"  SKIP (not found): {e}")
            continue

        row_count = df.count()
        print(f"  row_count = {row_count:,}")

        # Duplicate check on PK
        pk_col = cfg["pk"]
        dup_count = (df.groupBy(pk_col).count().filter(F.col("count") > 1).count())
        dup_pct = (dup_count / row_count * 100) if row_count else 0.0
        print(f"  duplicate_pct = {dup_pct:.2f}%")

        # Null % per column — SINGLE-PASS aggregation
        null_results = null_pct_per_column(df) if row_count else {}
        for col, pct in null_results.items():
            if pct > 50:
                print(f"    ⚠ {col}: {pct:.1f}% null")

        # Freshness
        freshness = None
        if "_ingested_at" in df.columns:
            freshness = df.agg(F.max("_ingested_at").alias("m")).collect()[0]["m"]

        # Status determination
        status = "PASS"
        notes = []
        if row_count == 0:
            status = "FAIL"; notes.append("Empty table")
        if dup_pct > 0.1:
            status = "WARN"; notes.append(f"{dup_count} duplicates on {pk_col}")
        worst_null_col = max(null_results, key=null_results.get) if null_results else None
        worst_null_pct = null_results.get(worst_null_col, 0.0) if worst_null_col else 0.0
        if worst_null_pct > 90:
            status = "WARN"; notes.append(f"{worst_null_col} is {worst_null_pct:.0f}% null")

        rows.append({
            "run_id":            run_id,
            "run_timestamp":     datetime.utcnow().isoformat(),
            "zone":              cfg["zone"],
            "table_name":        table_name,
            "row_count":         row_count,
            "duplicate_count":   dup_count,
            "duplicate_pct":     round(dup_pct, 4),
            "worst_null_column": worst_null_col,
            "worst_null_pct":    round(worst_null_pct, 2),
            "freshness":         str(freshness) if freshness else None,
            "status":            status,
            "notes":             "; ".join(notes) if notes else None,
        })
        print(f"  status = {status}")

    new_df = spark.createDataFrame(rows, schema=DQ_SCHEMA)
    new_df = add_lineage_columns(new_df, "dq_checks")

    dq_path = f"{CURATED_BASE}/tables/dq_metrics"
    cutoff_iso = (datetime.utcnow() - timedelta(days=DQ_RETENTION_DAYS)).isoformat()

    # Retention: read existing rows (if any), keep only those newer than cutoff,
    # union with this run's rows, then OVERWRITE. .cache() materializes the
    # combined DF in memory so we can safely blow away the source files.
    try:
        existing = spark.read.parquet(dq_path)
        retained = existing.filter(F.col("run_timestamp") >= F.lit(cutoff_iso))
        combined = retained.unionByName(new_df, allowMissingColumns=True)
        kept_before = retained.count()
        print(f"\n[DQ] Retention: kept {kept_before} rows from prior runs (within {DQ_RETENTION_DAYS} days)")
    except (AnalysisException, Exception) as e:
        # First run — table doesn't exist yet
        print(f"\n[DQ] First run (no prior dq_metrics): {type(e).__name__}")
        combined = new_df

    combined.cache().count()   # materialize before overwrite
    combined.write.mode("overwrite").format("parquet").save(dq_path)

    spark.sql("CREATE SCHEMA IF NOT EXISTS curated LOCATION 's3://curated/'")
    spark.sql("DROP TABLE IF EXISTS curated.dq_metrics")
    spark.sql(f"""
        CREATE TABLE curated.dq_metrics
        USING PARQUET
        LOCATION '{dq_path}/'
    """)

    print(f"[DQ] Total rows in curated.dq_metrics after retention: {combined.count()}")
    print(f"[DQ] Wrote {len(rows)} new check results to curated.dq_metrics")


def main():
    spark = get_spark("04_data_quality_checks")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 4 — DATA QUALITY CHECKS")
    print("=" * 70)
    run_checks(spark)
    print("\n" + "=" * 70)
    print("STAGE 4 COMPLETE")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
