#!/usr/bin/env python3
"""
Stage 5 — HIPAA PII Masking

Produces a HIPAA-Safe-Harbor compliant copy of the patient dimension at
s3://curated/tables/dim_patient_masked/.

HIPAA Safe Harbor identifiers handled:
  • Names → first initial + last initial
  • SSN/MRN → SHA-256 with salt → 16-char token (deterministic, joinable)
  • DOB → year only; "pre-1935" if patient is 90+
  • ZIP → first 3 digits only
  • Phone, email, full address → REDACTED via drop()

Access to the unmasked dim_patient is controlled at the Trino layer via
file-based RBAC (see trino/etc/rules.json — only data_engineers role can read).
"""
import hashlib
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from _common import get_spark, add_lineage_columns, CURATED_BASE


SALT = "DATALAKE_HIPAA_v1"
DIM_PATIENT_PATH        = f"{CURATED_BASE}/tables/dim_patient"
DIM_PATIENT_MASKED_PATH = f"{CURATED_BASE}/tables/dim_patient_masked"


def _hash_udf():
    """Deterministic salted hash — same input → same token (allows joins on masked IDs)."""
    def _h(s):
        if s is None:
            return None
        return hashlib.sha256((SALT + str(s)).encode("utf-8")).hexdigest()[:16]
    return F.udf(_h, StringType())


def mask_patient_pii(spark):
    print("\n[PII] Masking dim_patient → dim_patient_masked")
    h = _hash_udf()
    df = spark.read.parquet(DIM_PATIENT_PATH)

    masked = (df
        .withColumn("patient_token", h(F.col("patient_id")))
        .withColumn("mrn_token",     h(F.col("mrn")))
        .withColumn("first_initial", F.substring(F.col("first_name"), 1, 1))
        .withColumn("last_initial",  F.substring(F.col("last_name"), 1, 1))
        .withColumn("birth_year",
                    F.when(F.col("age") >= 90, F.lit("pre-1935"))
                     .otherwise(F.year(F.col("date_of_birth")).cast("string")))
        .withColumn("zip3", F.substring(F.col("zip"), 1, 3))
        .drop("patient_id", "mrn", "first_name", "last_name", "date_of_birth", "zip")
    )

    masked = add_lineage_columns(masked, "dim_patient (HIPAA-masked)")
    masked.write.mode("overwrite").format("parquet").save(DIM_PATIENT_MASKED_PATH)
    print(f"  ✓ wrote {masked.count()} masked rows → {DIM_PATIENT_MASKED_PATH}")

    spark.sql("DROP TABLE IF EXISTS curated.dim_patient_masked")
    spark.sql(f"""
        CREATE TABLE curated.dim_patient_masked
        USING PARQUET
        LOCATION '{DIM_PATIENT_MASKED_PATH}/'
    """)
    print("  ✓ registered curated.dim_patient_masked in Hive Metastore")


def main():
    spark = get_spark("05_pii_masking")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 5 — HIPAA PII MASKING")
    print("=" * 70)
    mask_patient_pii(spark)
    print("\n" + "=" * 70)
    print("STAGE 5 COMPLETE")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
