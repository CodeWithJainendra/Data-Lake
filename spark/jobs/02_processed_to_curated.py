#!/usr/bin/env python3
"""
Stage 2 — PROCESSED → CURATED

Builds business-level analytics-ready tables (the "Gold" layer).
All paths use s3:// (Trino-compatible).
"""
from pyspark.sql import functions as F
from _common import (
    get_spark, add_lineage_columns, write_parquet,
    PROCESSED_BASE, CURATED_BASE,
)


PROCESSED = f"{PROCESSED_BASE}/tables"
CURATED   = f"{CURATED_BASE}/tables"


def build_dimensions(spark):
    print("\n[DIM] Building dimensions ───────────────────────────────")
    patients   = spark.read.parquet(f"{PROCESSED}/patients")
    providers  = spark.read.parquet(f"{PROCESSED}/providers")
    payers     = spark.read.parquet(f"{PROCESSED}/payers")

    dim_patient = (patients
        .select(
            "patient_id", "mrn", "first_name", "last_name", "date_of_birth", "age",
            "sex", "race", "ethnicity", "marital_status", "language",
            "city", "state", "zip", "primary_payer_id", "secondary_payer_id", "deceased"
        )
        .withColumn("age_band", F.when(F.col("age") < 18, "0-17")
                                .when(F.col("age") < 35, "18-34")
                                .when(F.col("age") < 50, "35-49")
                                .when(F.col("age") < 65, "50-64")
                                .when(F.col("age") < 80, "65-79")
                                .otherwise("80+"))
    )
    write_parquet(add_lineage_columns(dim_patient, "patients"), f"{CURATED}/dim_patient")

    dim_provider = (providers
        .select("provider_id", "npi", "first_name", "last_name", "credentials",
                "department", "specialty", "facility", "active", "hire_date")
        .withColumn("provider_name",
                    F.concat_ws(", ", F.col("last_name"), F.col("first_name")))
    )
    write_parquet(add_lineage_columns(dim_provider, "providers"), f"{CURATED}/dim_provider")

    dim_payer = payers.select("payer_id", "payer_name", "payer_type", "avg_collection_rate")
    write_parquet(add_lineage_columns(dim_payer, "payers"), f"{CURATED}/dim_payer")

    dim_date = (spark.range(0, 2557)
        .withColumn("date", F.date_add(F.lit("2020-01-01"), F.col("id").cast("int")))
        .withColumn("year",  F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("week",  F.weekofyear("date"))
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_of_week",  F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
        .drop("id")
    )
    write_parquet(dim_date, f"{CURATED}/dim_date")


def build_fact_encounters(spark):
    print("\n[FACT] fact_encounters")
    enc   = spark.read.parquet(f"{PROCESSED}/encounters")
    dx    = spark.read.parquet(f"{PROCESSED}/diagnoses")
    proc  = spark.read.parquet(f"{PROCESSED}/procedures")

    dx_rollup = (dx.groupBy("encounter_id")
        .agg(
            F.count("*").alias("diagnosis_count"),
            F.max(F.when(F.col("is_primary"), F.col("icd10_code"))).alias("primary_diagnosis_code"),
            F.max(F.when(F.col("is_primary"), F.col("description"))).alias("primary_diagnosis_desc"),
            F.sum("raf_weight").alias("total_raf_score"),
            F.max(F.when(F.col("severity") == "severe", 1).otherwise(0)).alias("has_severe_dx"),
            F.collect_set("hcc_category").alias("hcc_categories"),
        )
    )

    proc_rollup = (proc.groupBy("encounter_id")
        .agg(
            F.count("*").alias("procedure_count"),
            F.sum("charge_amount").alias("total_charge_amount"),
            F.collect_set("category").alias("procedure_categories"),
        )
    )

    fact = (enc
        .join(dx_rollup,   "encounter_id", "left")
        .join(proc_rollup, "encounter_id", "left")
        .fillna({"diagnosis_count": 0, "procedure_count": 0, "total_charge_amount": 0.0, "has_severe_dx": 0})
    )
    write_parquet(add_lineage_columns(fact, "encounters+diagnoses+procedures"),
                  f"{CURATED}/fact_encounters",
                  partition_by=["admit_year", "admit_month"])


def build_fact_claims(spark):
    print("\n[FACT] fact_claims")
    claims    = spark.read.parquet(f"{PROCESSED}/claims")
    enc       = spark.read.parquet(f"{PROCESSED}/encounters")\
                     .select("encounter_id", "department", "encounter_type", "facility", "provider_id")
    patients  = spark.read.parquet(f"{PROCESSED}/patients")\
                     .select("patient_id", "age", "sex", "state")
    payers    = spark.read.parquet(f"{PROCESSED}/payers")\
                     .select(F.col("payer_id"), "payer_name", "payer_type")

    fact = (claims
        .join(enc,      "encounter_id", "left")
        .join(patients, "patient_id",   "left")
        .join(payers,   "payer_id",     "left")
        .withColumn("is_denied", F.col("claim_status") == "denied")
        .withColumn("is_paid",   F.col("claim_status") == "paid")
        .withColumn("net_payment",
                    F.coalesce(F.col("paid_amount"), F.lit(0.0)) - F.coalesce(F.col("adjustment_amount"), F.lit(0.0)))
    )
    write_parquet(add_lineage_columns(fact, "claims+encounters+patients+payers"),
                  f"{CURATED}/fact_claims",
                  partition_by=["submitted_year", "submitted_month"])


def build_fact_diagnoses_enriched(spark):
    print("\n[FACT] fact_diagnoses (HCC-enriched)")
    dx  = spark.read.parquet(f"{PROCESSED}/diagnoses")
    ref = spark.read.parquet(f"{PROCESSED}/icd10_reference")\
              .select(F.col("icd10_code"),
                      F.col("description").alias("ref_description"),
                      F.col("severity").alias("ref_severity"),
                      F.col("raf_weight").alias("ref_raf"))
    enriched = dx.join(ref, "icd10_code", "left")
    write_parquet(add_lineage_columns(enriched, "diagnoses+icd10_reference"),
                  f"{CURATED}/fact_diagnoses")


def build_aggregates(spark):
    print("\n[AGG] Pre-aggregates ────────────────────────────────────")
    fact_claims = spark.read.parquet(f"{CURATED}/fact_claims")

    agg_monthly = (fact_claims
        .groupBy("submitted_year", "submitted_month", "department", "payer_name", "payer_type")
        .agg(
            F.count("*").alias("claim_count"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum("paid_amount").alias("total_paid"),
            F.sum("adjustment_amount").alias("total_adjustments"),
            F.sum(F.when(F.col("is_denied"), 1).otherwise(0)).alias("denied_count"),
            F.avg("collection_rate").alias("avg_collection_rate"),
            F.avg("days_to_adjudicate").alias("avg_days_to_adjudicate"),
        )
        .withColumn("denial_rate", F.col("denied_count") / F.col("claim_count"))
    )
    write_parquet(agg_monthly, f"{CURATED}/agg_monthly_revenue")

    agg_denial = (fact_claims
        .filter(F.col("is_denied"))
        .groupBy("denial_reason_code", "payer_name", "department")
        .agg(
            F.count("*").alias("denial_count"),
            F.sum("billed_amount").alias("billed_at_risk"),
            F.avg("billed_amount").alias("avg_claim_value"),
        )
        .orderBy(F.col("denial_count").desc())
    )
    write_parquet(agg_denial, f"{CURATED}/agg_denial_summary")

    fact_enc = spark.read.parquet(f"{CURATED}/fact_encounters")
    agg_provider = (fact_enc
        .groupBy("provider_id", "department")
        .agg(
            F.count("*").alias("encounter_count"),
            F.sum("total_charge_amount").alias("total_charges"),
            F.avg("diagnosis_count").alias("avg_dx_per_encounter"),
            F.avg("procedure_count").alias("avg_proc_per_encounter"),
            F.avg("total_raf_score").alias("avg_raf_score"),
            F.sum("has_severe_dx").alias("severe_dx_encounters"),
            F.avg("length_of_stay_hours").alias("avg_los_hours"),
        )
    )
    write_parquet(agg_provider, f"{CURATED}/agg_provider_kpi")


def register_hive_tables(spark):
    """
    Register curated tables in Hive Metastore.

    Partitioned tables (fact_encounters, fact_claims) need MSCK REPAIR after
    CREATE TABLE — without it, Spark detects partition columns from the
    directory layout but the metastore has no partition entries, so Trino
    returns 0 rows on SELECT.
    """
    print("\n[CAT] Registering tables in Hive Metastore ─────────────")
    spark.sql("CREATE SCHEMA IF NOT EXISTS curated LOCATION 's3://curated/'")

    # Tables that have Hive-style directory partitions
    PARTITIONED_TABLES = {"fact_encounters", "fact_claims"}

    tables = [
        "dim_patient", "dim_provider", "dim_payer", "dim_date",
        "fact_encounters", "fact_claims", "fact_diagnoses",
        "agg_monthly_revenue", "agg_denial_summary", "agg_provider_kpi",
    ]
    for t in tables:
        spark.sql(f"DROP TABLE IF EXISTS curated.{t}")
        spark.sql(f"""
            CREATE TABLE curated.{t}
            USING PARQUET
            LOCATION 's3://curated/tables/{t}/'
        """)
        if t in PARTITIONED_TABLES:
            # Discover partitions from S3 directory layout and write to metastore
            spark.sql(f"MSCK REPAIR TABLE curated.{t}")
            n_parts = spark.sql(f"SHOW PARTITIONS curated.{t}").count()
            print(f"  ✓ curated.{t}  (partitioned, {n_parts} partitions registered)")
        else:
            print(f"  ✓ curated.{t}")


def main():
    spark = get_spark("02_processed_to_curated")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 2 — PROCESSED → CURATED")
    print("=" * 70)
    build_dimensions(spark)
    build_fact_encounters(spark)
    build_fact_claims(spark)
    build_fact_diagnoses_enriched(spark)
    build_aggregates(spark)
    register_hive_tables(spark)
    print("\n" + "=" * 70)
    print("STAGE 2 COMPLETE")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
