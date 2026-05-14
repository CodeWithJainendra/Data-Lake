#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Generate synthetic medical data in multiple formats and upload to MinIO.
# ─────────────────────────────────────────────────────────────────────────
set -e
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "============================================================"
echo "  Step 1: Installing Python dependencies"
echo "============================================================"
docker exec dl-workbench pip install -q -r /home/jovyan/work/data-generator/requirements.txt
docker exec dl-workbench pip install -q openpyxl pyarrow

echo ""
echo "============================================================"
echo "  Step 2: Generating CSV tables (canonical source)"
echo "============================================================"
docker exec -e OUT_DIR=/home/jovyan/work/data/raw/tables dl-workbench \
    python /home/jovyan/work/data-generator/generate_medical_data.py \
    --patients 10000 --providers 500 --encounters 50000

echo ""
echo "============================================================"
echo "  Step 3: Generating medical PDFs"
echo "============================================================"
docker exec -e PDF_OUT_DIR=/home/jovyan/work/data/raw/pdfs dl-workbench \
    python /home/jovyan/work/data-generator/generate_pdfs.py --n 100

echo ""
echo "============================================================"
echo "  Step 4: Generating same data in alternate formats"
echo "          (JSON, JSONL, Parquet, Excel, SQL dump, HL7 v2, FHIR R4)"
echo "============================================================"
docker exec -e BASE_DIR=/home/jovyan/work/data/raw dl-workbench \
    python /home/jovyan/work/data-generator/generate_diverse_formats.py

echo ""
echo "============================================================"
echo "  Step 5: Uploading ALL formats to MinIO raw zone"
echo "============================================================"
docker run --rm --network medical_datalake \
    -v "$PROJECT_DIR/data:/data" \
    --entrypoint /bin/sh \
    minio/mc:latest \
    -c "
        mc alias set local http://minio:9000 admin admin123456;
        mc cp --recursive /data/raw/tables/  local/raw/tables/;
        mc cp --recursive /data/raw/pdfs/    local/raw/pdfs/;
        mc cp --recursive /data/raw/json/    local/raw/json/    2>/dev/null || true;
        mc cp --recursive /data/raw/jsonl/   local/raw/jsonl/   2>/dev/null || true;
        mc cp --recursive /data/raw/parquet/ local/raw/parquet/ 2>/dev/null || true;
        mc cp --recursive /data/raw/excel/   local/raw/excel/   2>/dev/null || true;
        mc cp --recursive /data/raw/sql/     local/raw/sql/     2>/dev/null || true;
        mc cp --recursive /data/raw/hl7/     local/raw/hl7/     2>/dev/null || true;
        mc cp --recursive /data/raw/fhir/    local/raw/fhir/    2>/dev/null || true;
        echo '─── MinIO raw bucket contents ───';
        mc ls --recursive local/raw/ | head -40;
    "

echo ""
echo "============================================================"
echo "  ✓ DATA LOADED — 7 formats in MinIO raw zone"
echo "============================================================"
echo "  Next: ./scripts/run_pipeline.sh"
