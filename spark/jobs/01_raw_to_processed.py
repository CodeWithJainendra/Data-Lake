#!/usr/bin/env python3
"""
Stage 1 — RAW → PROCESSED

Reads CSVs from s3://raw/tables/, cleans them (typed, deduped, null-checked,
standardized), and writes Parquet to s3://processed/tables/.

This is the "Bronze → Silver" step in the medallion architecture.

For non-CSV sources (JSON / Parquet / Excel / HL7 / FHIR / SQL dumps), see
00_universal_ingestion.py which lands those formats into the same processed
zone alongside the CSVs.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from _common import get_spark, add_lineage_columns, write_parquet, RAW_BASE, PROCESSED_BASE


# ──────────────────────────────────────────────────────────────────────
# Resilient CSV reader — permissive mode handles new/missing columns
# ──────────────────────────────────────────────────────────────────────
def read_csv(spark, name):
    return (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt")
            .csv(f"{RAW_BASE}/tables/{name}.csv"))


def process_patients(spark):
    print("\n[1/7] Patients ──────────────────────────────────────────")
    df = read_csv(spark, "patients")

    # Deduplicate: keep latest row per patient_id (ordered by created_at)
    dedupe_window = Window.partitionBy("patient_id").orderBy(F.col("created_at").desc())

    df = (df
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
        .withColumn("city",       F.initcap(F.trim(F.col("city"))))
        .withColumn("state",      F.upper(F.trim(F.col("state"))))
        .withColumn("zip",        F.substring(F.regexp_replace(F.col("zip").cast("string"), "[^0-9]", ""), 1, 5))
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth")))
        .withColumn("created_at",    F.to_timestamp(F.col("created_at")))
        .withColumn("_rn", F.row_number().over(dedupe_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # age sanity (negative or >120 → null)
        .withColumn("age", F.when((F.col("age") < 0) | (F.col("age") > 120), None).otherwise(F.col("age")))
        # partition column — ingestion year-month, NOT state (avoids small files)
        .withColumn("ingest_year",  F.year(F.current_timestamp()))
        .withColumn("ingest_month", F.month(F.current_timestamp()))
    )
    df = add_lineage_columns(df, "patients.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/patients", partition_by=["ingest_year", "ingest_month"])


def process_providers(spark):
    print("\n[2/7] Providers ─────────────────────────────────────────")
    df = read_csv(spark, "providers")
    df = (df
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
        .withColumn("hire_date",  F.to_date(F.col("hire_date")))
        .dropDuplicates(["provider_id"])
    )
    df = add_lineage_columns(df, "providers.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/providers")


def process_encounters(spark):
    print("\n[3/7] Encounters ────────────────────────────────────────")
    df = read_csv(spark, "encounters")
    df = (df
        .withColumn("admit_datetime",     F.to_timestamp(F.col("admit_datetime")))
        .withColumn("discharge_datetime", F.to_timestamp(F.col("discharge_datetime")))
        .withColumn("admit_date",         F.to_date(F.col("admit_datetime")))
        .withColumn("admit_year",  F.year(F.col("admit_datetime")))
        .withColumn("admit_month", F.month(F.col("admit_datetime")))
        .dropDuplicates(["encounter_id"])
        .filter(F.col("length_of_stay_hours") >= 0)
    )
    df = add_lineage_columns(df, "encounters.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/encounters", partition_by=["admit_year", "admit_month"])


def process_diagnoses(spark):
    print("\n[4/7] Diagnoses ─────────────────────────────────────────")
    df = read_csv(spark, "diagnoses")
    df = (df
        .withColumn("icd10_code", F.upper(F.trim(F.col("icd10_code"))))
        .filter(F.col("icd10_code").rlike(r"^[A-Z][0-9]{2}"))
        .dropDuplicates(["diagnosis_id"])
    )
    df = add_lineage_columns(df, "diagnoses.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/diagnoses")


def process_procedures(spark):
    print("\n[5/7] Procedures ────────────────────────────────────────")
    df = read_csv(spark, "procedures")
    df = (df
        .withColumn("cpt_code", F.trim(F.col("cpt_code")))
        .filter(F.col("cpt_code").rlike(r"^[0-9]{5}$"))
        .filter(F.col("charge_amount") >= 0)
        .dropDuplicates(["procedure_id"])
    )
    df = add_lineage_columns(df, "procedures.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/procedures")


def process_claims(spark):
    print("\n[6/7] Claims ────────────────────────────────────────────")
    df = read_csv(spark, "claims")
    df = (df
        .withColumn("submitted_date",    F.to_date(F.col("submitted_date")))
        .withColumn("adjudicated_date",  F.to_date(F.col("adjudicated_date")))
        .withColumn("days_to_adjudicate",
                    F.datediff(F.col("adjudicated_date"), F.col("submitted_date")))
        .withColumn("submitted_year",  F.year(F.col("submitted_date")))
        .withColumn("submitted_month", F.month(F.col("submitted_date")))
        .withColumn("collection_rate",
                    F.when(F.col("billed_amount") > 0,
                           F.col("paid_amount") / F.col("billed_amount")).otherwise(0))
        .dropDuplicates(["claim_id"])
    )
    df = add_lineage_columns(df, "claims.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/claims", partition_by=["submitted_year", "submitted_month"])


def process_payments(spark):
    print("\n[7/7] Payments ──────────────────────────────────────────")
    df = read_csv(spark, "payments")
    df = (df
        .withColumn("payment_date", F.to_date(F.col("payment_date")))
        .filter(F.col("payment_amount") > 0)
        .dropDuplicates(["payment_id"])
    )
    df = add_lineage_columns(df, "payments.csv")
    write_parquet(df, f"{PROCESSED_BASE}/tables/payments")


def process_reference_tables(spark):
    print("\n[Ref] Reference tables (icd10, cpt, denial_reasons, payers)")
    for name in ["icd10_reference", "cpt_reference", "denial_reasons", "payers"]:
        df = read_csv(spark, name)
        df = add_lineage_columns(df, f"{name}.csv")
        write_parquet(df, f"{PROCESSED_BASE}/tables/{name}")


def main():
    spark = get_spark("01_raw_to_processed")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 1 — RAW → PROCESSED")
    print("=" * 70)
    process_patients(spark)
    process_providers(spark)
    process_encounters(spark)
    process_diagnoses(spark)
    process_procedures(spark)
    process_claims(spark)
    process_payments(spark)
    process_reference_tables(spark)
    print("\n" + "=" * 70)
    print("STAGE 1 COMPLETE")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
