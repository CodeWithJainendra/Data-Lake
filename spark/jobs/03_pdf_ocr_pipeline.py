#!/usr/bin/env python3
"""
Stage 3 — PDF OCR Pipeline (distributed)

Reads all PDFs from s3://raw/pdfs/ using Spark's binaryFile reader (so they're
processed in parallel across workers), extracts text via PyPDF2 (digital) or
Tesseract OCR (scanned), classifies each document, extracts clinical entities,
and writes structured rows to s3://curated/tables/dim_clinical_documents/.

Bad PDFs go to s3://dlq/pdfs/ with the error reason — no silent failures.
"""
import io
import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType
)

from _common import get_spark, add_lineage_columns


RAW_PATH    = "s3://raw/pdfs/"
CURATED_OUT = "s3://curated/tables/dim_clinical_documents"
DLQ_OUT     = "s3://dlq/pdfs/"


# Regex extractors for clinical entities
MRN_RE      = re.compile(r"MRN[:\s]+(\d{6,10})")
DOB_RE      = re.compile(r"DOB[:\s]+(\d{1,2}/\d{1,2}/\d{4})")
ICD10_RE    = re.compile(r"\b([A-Z]\d{2}(?:\.\d+)?)\b")
MED_RE      = re.compile(r"(?:Rx[:\s]+|•\s+)([A-Z][a-z]+(?:\s+[A-Za-z]+)?\s+\d+\s?mg(?:/\w+)?)")
PROVIDER_RE = re.compile(r"Dr\.\s+([A-Z][a-z]+)")


# ──────────────────────────────────────────────────────────────────────
# OUTPUT SCHEMA — the return type of the parsing UDF
# ──────────────────────────────────────────────────────────────────────
PARSE_SCHEMA = StructType([
    StructField("document_type", StringType(), True),
    StructField("mrn",           StringType(), True),
    StructField("dob",           StringType(), True),
    StructField("icd10_codes",   ArrayType(StringType()), True),
    StructField("medications",   ArrayType(StringType()), True),
    StructField("providers",     ArrayType(StringType()), True),
    StructField("char_count",    IntegerType(), True),
    StructField("raw_text",      StringType(), True),
    StructField("error",         StringType(), True),
])


# ──────────────────────────────────────────────────────────────────────
# PDF parsing UDF — runs in parallel on each worker
# ──────────────────────────────────────────────────────────────────────
def parse_pdf_bytes(content):
    """Extract structured info from a PDF as bytes. Returns a dict matching PARSE_SCHEMA."""
    try:
        # 1. Try PyPDF2 for digital PDFs first
        text = ""
        try:
            from PyPDF2 import PdfReader
            reader = PdfReader(io.BytesIO(bytes(content)))
            text = "\n".join((p.extract_text() or "") for p in reader.pages)
        except Exception:
            text = ""

        # 2. Fall back to Tesseract OCR if PyPDF2 got nothing
        if len(text.strip()) < 100:
            try:
                import pytesseract
                from pdf2image import convert_from_bytes
                images = convert_from_bytes(bytes(content), dpi=200)
                text = "\n".join(pytesseract.image_to_string(img) for img in images)
            except Exception as ocr_e:
                # If OCR fails too, return what we have with error
                return {
                    "document_type": "unknown",
                    "mrn": None, "dob": None,
                    "icd10_codes": [], "medications": [], "providers": [],
                    "char_count": len(text),
                    "raw_text": text[:5000],
                    "error": f"ocr_failed: {ocr_e}",
                }

        # 3. Classify
        tl = text.lower()
        if "discharge summary" in tl or "hospital course" in tl:
            doc_type = "discharge_summary"
        elif "laboratory report" in tl or "reference range" in tl:
            doc_type = "lab_report"
        elif "prescription" in tl and ("sig:" in tl or "rx:" in tl):
            doc_type = "prescription"
        else:
            doc_type = "unknown"

        # 4. Entity extraction via regex
        mrn = MRN_RE.search(text)
        dob = DOB_RE.search(text)

        return {
            "document_type": doc_type,
            "mrn":         mrn.group(1) if mrn else None,
            "dob":         dob.group(1) if dob else None,
            "icd10_codes": list(set(ICD10_RE.findall(text))),
            "medications": list(set(MED_RE.findall(text))),
            "providers":   list(set(PROVIDER_RE.findall(text))),
            "char_count":  len(text),
            "raw_text":    text[:5000],
            "error":       None,
        }
    except Exception as e:
        return {
            "document_type": "unknown",
            "mrn": None, "dob": None,
            "icd10_codes": [], "medications": [], "providers": [],
            "char_count": 0, "raw_text": "",
            "error": f"parse_failed: {type(e).__name__}: {e}",
        }


def main():
    spark = get_spark("03_pdf_ocr_pipeline")
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 70)
    print("STAGE 3 — DISTRIBUTED PDF OCR PIPELINE")
    print("=" * 70)

    # Read all PDFs as a DataFrame: (path, modificationTime, length, content)
    pdf_df = (spark.read.format("binaryFile")
              .option("pathGlobFilter", "*.pdf")
              .option("recursiveFileLookup", "true")
              .load(RAW_PATH))

    n = pdf_df.count()
    print(f"  Found {n} PDFs to process")

    if n == 0:
        print("  No PDFs found — skipping stage.")
        spark.stop()
        return

    # Run the parser in parallel via UDF
    parse_udf = F.udf(parse_pdf_bytes, PARSE_SCHEMA)
    parsed = (pdf_df
        .withColumn("parsed", parse_udf(F.col("content")))
        .select(
            F.col("path").alias("source_key"),
            F.monotonically_increasing_id().alias("_row_id"),
            "parsed.*",
        )
        .withColumn("document_id",  F.concat(F.lit("DOC"), F.lpad(F.col("_row_id").cast("string"), 10, "0")))
        .withColumn("processed_at", F.current_timestamp())
        .drop("_row_id")
    )

    # Split into good rows and DLQ (rows with errors)
    good = parsed.filter(F.col("error").isNull()).drop("error")
    bad  = parsed.filter(F.col("error").isNotNull())

    good = add_lineage_columns(good, "raw/pdfs/")
    good.write.mode("overwrite").format("parquet").save(CURATED_OUT)
    print(f"  ✓ wrote {good.count()} parsed documents to {CURATED_OUT}")

    # Idempotent DLQ: always drop the metastore table and clear the S3 path
    # FIRST, then write fresh. This avoids stale rows persisting from a prior
    # run that had failures when the current run is clean.
    spark.sql("CREATE SCHEMA IF NOT EXISTS dlq LOCATION 's3://dlq/'")
    spark.sql("DROP TABLE IF EXISTS dlq.pdfs")

    bad_count = bad.count()
    if bad_count > 0:
        # Overwrite (not append) so the DLQ reflects ONLY this run's failures
        bad.write.mode("overwrite").format("parquet").save(DLQ_OUT)
        spark.sql(f"CREATE TABLE dlq.pdfs USING PARQUET LOCATION '{DLQ_OUT}'")
        print(f"  ⚠ wrote {bad_count} failed PDFs to {DLQ_OUT} → dlq.pdfs")
    else:
        # Best-effort wipe of any stale parquet files from previous runs.
        # Uses Hadoop FileSystem API so it works on the s3:// backend.
        try:
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            URI = spark._jvm.java.net.URI
            Path = spark._jvm.org.apache.hadoop.fs.Path
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(URI(DLQ_OUT), hadoop_conf)
            if fs.exists(Path(DLQ_OUT)):
                fs.delete(Path(DLQ_OUT), True)
                print(f"  ✓ cleared stale DLQ files at {DLQ_OUT}")
        except Exception as e:
            print(f"  (could not clear DLQ path: {e})")
        print("  ✓ no DLQ entries this run")

    # Register the curated documents table
    spark.sql("CREATE SCHEMA IF NOT EXISTS curated LOCATION 's3://curated/'")
    spark.sql("DROP TABLE IF EXISTS curated.dim_clinical_documents")
    spark.sql(f"""
        CREATE TABLE curated.dim_clinical_documents
        USING PARQUET
        LOCATION '{CURATED_OUT}/'
    """)

    print("\n" + "=" * 70)
    print(f"STAGE 3 COMPLETE — {good.count()} good + {bad_count} DLQ")
    print("=" * 70)
    spark.stop()


if __name__ == "__main__":
    main()
