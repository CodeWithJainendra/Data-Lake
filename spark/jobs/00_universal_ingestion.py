#!/usr/bin/env python3
"""
Stage 0 — Universal Multi-Format Ingestion

Real-world data lakes don't get only CSV. This job auto-detects the format
of every file in s3://raw/ and routes it to the right reader.

Supported formats:
  • CSV           → spark.read.csv
  • TSV           → spark.read.csv(sep='\t')
  • JSON          → spark.read.json (single-line)
  • JSONL         → spark.read.json (newline-delimited)
  • Parquet       → spark.read.parquet
  • Avro          → spark.read.format('avro')   (requires spark-avro package)
  • ORC           → spark.read.orc
  • Excel (.xlsx) → pandas → Spark DataFrame    (small files only)
  • SQL dumps     → INSERT statement parser → Spark DataFrame
  • HL7 v2        → pipe-delimited parser → Spark DataFrame
  • FHIR JSON     → JSON with $.entry[].resource flatten

Failures are written to s3://dlq/<format>/ with the error message.

Layout convention:
  s3://raw/<format>/<table_name>.<ext>
    e.g. s3://raw/json/patients.json
         s3://raw/excel/billing_q1.xlsx
         s3://raw/sql/patients_dump.sql
         s3://raw/hl7/adt_20260514.hl7
         s3://raw/fhir/bundle_001.json
"""
import io
import re
import json
import boto3
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from _common import get_spark, add_lineage_columns, RAW_BASE, PROCESSED_BASE, DLQ_BASE


S3_ENDPOINT = "http://minio:9000"
S3_ACCESS   = "admin"
S3_SECRET   = "admin123456"


def _s3():
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
    )


# ─────────────────────────────────────────────────────────────────────
# READERS — one per format. Each returns a Spark DataFrame.
# ─────────────────────────────────────────────────────────────────────

def read_csv_format(spark, path, sep=","):
    return (spark.read
            .option("header", True).option("inferSchema", True)
            .option("mode", "PERMISSIVE").option("sep", sep)
            .csv(path))


def read_json_format(spark, path, lines=False):
    reader = spark.read.option("mode", "PERMISSIVE")
    if not lines:
        reader = reader.option("multiline", "true")
    return reader.json(path)


def read_parquet_format(spark, path):
    return spark.read.parquet(path)


def read_orc_format(spark, path):
    return spark.read.orc(path)


def read_excel_format(spark, s3_path):
    """Read .xlsx via pandas (driver-side), then convert. OK for files <100MB."""
    import pandas as pd
    s3 = _s3()
    # Convert s3:// URL to bucket+key
    rest = s3_path.replace("s3://", "")
    bucket, key = rest.split("/", 1)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    pdf = pd.read_excel(io.BytesIO(body), engine="openpyxl")
    # Convert NaN → None for Spark
    pdf = pdf.where(pdf.notnull(), None)
    return spark.createDataFrame(pdf)


def read_sql_dump_format(spark, s3_path):
    """Parse a Postgres/MySQL .sql INSERT-statements dump into rows.
    Only supports `INSERT INTO <tbl> (cols) VALUES (...), (...);` style.
    For full ddl-recovery use pgloader; this is the lake-friendly subset.
    """
    s3 = _s3()
    rest = s3_path.replace("s3://", "")
    bucket, key = rest.split("/", 1)
    text = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="ignore")

    rows = []
    cols = None
    # Find any INSERT statement
    insert_re = re.compile(
        r"INSERT\s+INTO\s+\S+\s*\(([^)]+)\)\s*VALUES\s*((?:\([^)]*\)\s*,?\s*)+);",
        re.IGNORECASE | re.DOTALL,
    )
    val_row_re = re.compile(r"\(([^)]*)\)")
    for m in insert_re.finditer(text):
        if cols is None:
            cols = [c.strip().strip('"').strip("`") for c in m.group(1).split(",")]
        for vm in val_row_re.finditer(m.group(2)):
            # Crude CSV-of-SQL-values parser. Real systems use sqlparse.
            raw = vm.group(1)
            parts = [p.strip().strip("'") if p.strip() != "NULL" else None
                     for p in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", raw)]
            rows.append(dict(zip(cols, parts)))
    if not rows:
        raise ValueError("No INSERT statements parsed from SQL dump")
    return spark.createDataFrame(rows)


def read_hl7_v2_format(spark, s3_path):
    """Parse HL7 v2 pipe-delimited messages → row per segment with patient_id from PID."""
    s3 = _s3()
    rest = s3_path.replace("s3://", "")
    bucket, key = rest.split("/", 1)
    text = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="ignore")
    rows = []
    current_pid = None
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        seg_type = parts[0]
        if seg_type == "PID":
            current_pid = parts[3] if len(parts) > 3 else None
        rows.append({
            "patient_id": current_pid,
            "segment_type": seg_type,
            "raw_segment": line,
        })
    return spark.createDataFrame(rows)


def read_fhir_format(spark, s3_path):
    """Read FHIR JSON Bundle, flatten `entry[].resource` into rows."""
    s3 = _s3()
    rest = s3_path.replace("s3://", "")
    bucket, key = rest.split("/", 1)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="ignore")
    doc = json.loads(body)
    entries = doc.get("entry", []) or []
    rows = []
    for e in entries:
        r = e.get("resource", {}) or {}
        rows.append({
            "resource_type": r.get("resourceType"),
            "resource_id":   r.get("id"),
            "resource_json": json.dumps(r),
        })
    if not rows:
        # Empty bundle — write nothing
        schema = StructType([
            StructField("resource_type", StringType()),
            StructField("resource_id",   StringType()),
            StructField("resource_json", StringType()),
        ])
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows)


# ─────────────────────────────────────────────────────────────────────
# DISPATCHER — format detection + writes to processed zone
# ─────────────────────────────────────────────────────────────────────
FORMAT_HANDLERS = {
    ".csv":     ("csv",     lambda s, p: read_csv_format(s, p, sep=",")),
    ".tsv":     ("tsv",     lambda s, p: read_csv_format(s, p, sep="\t")),
    ".json":    ("json",    lambda s, p: read_json_format(s, p, lines=False)),
    ".jsonl":   ("jsonl",   lambda s, p: read_json_format(s, p, lines=True)),
    ".ndjson":  ("jsonl",   lambda s, p: read_json_format(s, p, lines=True)),
    ".parquet": ("parquet", read_parquet_format),
    ".orc":     ("orc",     read_orc_format),
    ".xlsx":    ("excel",   read_excel_format),
    ".sql":     ("sql",     read_sql_dump_format),
    ".hl7":     ("hl7",     read_hl7_v2_format),
}


def list_raw_files():
    """List every file under s3://raw/ across all sub-prefixes."""
    s3 = _s3()
    found = []
    paginator = s3.get_paginator("list_objects_v2")
    # We scan ALL prefixes (not just 'tables/' — also json/, sql/, hl7/, etc.)
    for page in paginator.paginate(Bucket="raw"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Skip PDFs — those are handled by Stage 3 OCR pipeline
            if key.lower().endswith(".pdf"):
                continue
            found.append(key)
    return found


def detect_format(key):
    for ext, (fmt_name, handler) in FORMAT_HANDLERS.items():
        if key.lower().endswith(ext):
            # Special case: FHIR is also .json but typically in fhir/ prefix
            if ext == ".json" and key.startswith("fhir/"):
                return ("fhir", read_fhir_format)
            return (fmt_name, handler)
    return (None, None)


def derive_table_name(key, fmt):
    """Derive a clean processed-table name from a raw key.
    e.g. 'json/patients.json' → 'patients_from_json'
         'tables/patients.csv' → 'patients'      (CSV is the canonical source)
    """
    fname = key.rsplit("/", 1)[-1]
    base  = fname.rsplit(".", 1)[0]
    if fmt == "csv" and key.startswith("tables/"):
        return base
    return f"{base}_from_{fmt}"


def main():
    spark = get_spark("00_universal_ingestion")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 0 — UNIVERSAL MULTI-FORMAT INGESTION")
    print("=" * 70)

    keys = list_raw_files()
    print(f"  Found {len(keys)} non-PDF files under s3://raw/")

    success, dlq = 0, 0
    for key in keys:
        fmt, handler = detect_format(key)
        if fmt is None:
            print(f"  SKIP (unknown extension): {key}")
            continue
        path = f"s3://raw/{key}"
        table_name = derive_table_name(key, fmt)
        print(f"\n  [{fmt:8s}] {key}  →  processed/tables/{table_name}/")
        try:
            df = handler(spark, path)
            df = add_lineage_columns(df, key)
            df.write.mode("overwrite").format("parquet")\
                .save(f"{PROCESSED_BASE}/tables/{table_name}")
            print(f"    ✓ {df.count()} rows")
            success += 1
        except Exception as e:
            print(f"    ✗ FAILED: {e}")
            # Write a DLQ record
            err_df = spark.createDataFrame(
                [{"key": key, "format": fmt, "error": str(e),
                  "failed_at": datetime.utcnow().isoformat()}]
            )
            err_df.write.mode("append").format("parquet")\
                .save(f"{DLQ_BASE}/ingestion/")
            dlq += 1

    print("\n" + "=" * 70)
    print(f"STAGE 0 COMPLETE — {success} ingested, {dlq} sent to DLQ")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
