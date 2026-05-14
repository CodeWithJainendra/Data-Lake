#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Bring up the Data Lake stack.
#   - All required JARs are now baked into the Hive + Spark Dockerfiles
#     (no runtime curl needed — works offline after first build).
#   - First run takes ~5 min to build custom images; subsequent runs ~30s.
# ─────────────────────────────────────────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Ensure data directories exist on host (volumes mount them)
mkdir -p data/raw/tables data/raw/pdfs data/raw/json data/raw/jsonl \
         data/raw/parquet data/raw/excel data/raw/sql data/raw/hl7 data/raw/fhir \
         data/processed data/curated data/dlq

echo ""
echo "============================================================"
echo "  Step 1: Build custom images (Spark+OCR + Hive)"
echo "          (first run ~5 min; cached afterwards)"
echo "============================================================"
docker compose build spark-master spark-worker hive-metastore

echo ""
echo "============================================================"
echo "  Step 2: Bring up the stack..."
echo "============================================================"
docker compose up -d

echo ""
echo "Waiting for services to become healthy (~60s)..."
sleep 25
docker compose ps

echo ""
echo "============================================================"
echo "  STACK IS RUNNING. Access the UIs below:"
echo "============================================================"
echo "  MinIO Console      → http://localhost:9001     (admin / admin123456)"
echo "  Spark Master UI    → http://localhost:8080"
echo "  Trino UI           → http://localhost:8081"
echo "  Superset           → http://localhost:8088     (admin / admin)"
echo "  NiFi               → https://localhost:8443/nifi   (admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB)"
echo "  Jupyter Workbench  → http://localhost:8888     (token: datalake)"
echo "  Dashboard          → http://localhost:5050     (after ./scripts/start_dashboard.sh)"
echo "============================================================"
echo ""
echo "Next: ./scripts/load_sample_data.sh"
