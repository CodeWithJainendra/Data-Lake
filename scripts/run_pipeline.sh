#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Run the full Spark ETL pipeline with basic retry on each stage.
#   Stage 0: universal ingestion (CSV, JSON, Parquet, Excel, SQL, HL7, FHIR)
#   Stage 1: raw → processed (CSV-specific cleanup)
#   Stage 2: processed → curated
#   Stage 3: distributed PDF OCR
#   Stage 4: data quality checks
#   Stage 5: HIPAA PII masking
# ─────────────────────────────────────────────────────────────────────────
set -e
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# spark-defaults.conf is mounted into the container, so we don't need --packages
SPARK_SUBMIT="docker exec -e HOME=/tmp -e HADOOP_USER_NAME=spark -e USER=spark \
    -e PYSPARK_PYTHON=python3 \
    dl-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.sql.adaptive.enabled=true"

MAX_RETRIES=2

run_stage_with_retry() {
    local stage=$1
    local file=$2
    local attempt=0

    while [ $attempt -le $MAX_RETRIES ]; do
        attempt=$((attempt + 1))
        echo ""
        echo "============================================================"
        echo "  STAGE $stage: $file  (attempt $attempt/$((MAX_RETRIES + 1)))"
        echo "============================================================"
        if $SPARK_SUBMIT /opt/spark-jobs/$file; then
            echo "  ✓ Stage $stage SUCCEEDED"
            return 0
        else
            echo "  ✗ Stage $stage FAILED on attempt $attempt"
            if [ $attempt -le $MAX_RETRIES ]; then
                echo "    Retrying in 10s..."
                sleep 10
            fi
        fi
    done
    echo "  ✗✗✗ Stage $stage FAILED after $((MAX_RETRIES + 1)) attempts"
    return 1
}

# Stage 0 always runs — the job itself prints "Found 0 files" and exits cleanly
# if no non-CSV/PDF files are present. (The previous bash detection logic had
# an off-by-slash bug that always evaluated false, so this stage never ran.)
run_stage_with_retry 0 00_universal_ingestion.py

run_stage_with_retry 1 01_raw_to_processed.py
run_stage_with_retry 2 02_processed_to_curated.py
run_stage_with_retry 3 03_pdf_ocr_pipeline.py
run_stage_with_retry 4 04_data_quality_checks.py
run_stage_with_retry 5 05_pii_masking.py

echo ""
echo "============================================================"
echo "  ✓ FULL PIPELINE COMPLETE"
echo "============================================================"
echo ""
echo "  Quick verification:"
echo "  docker exec -it dl-trino trino --catalog hive --schema curated"
echo "  > SHOW TABLES;"
echo "  > SELECT COUNT(*) FROM fact_claims;"
echo ""
echo "  Or open Superset → http://localhost:8088 (admin / admin)"
echo ""
echo "  To run incremental merge later (CDC pattern):"
echo "  docker exec dl-spark-master spark-submit \\"
echo "    --master spark://spark-master:7077 \\"
echo "    /opt/spark-jobs/06_incremental_merge.py --since 2026-05-01"
