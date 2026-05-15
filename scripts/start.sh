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
# spark-master and spark-worker share the same image (datalake/spark-ocr:3.5.6).
# Building both in parallel causes a race condition (image already exists error).
# Build hive-metastore and spark-master only; spark-worker will reuse the image.
docker compose build hive-metastore spark-master
# Build spark-worker separately (same image, no-op if already built)
docker compose build spark-worker || true

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
echo "  Step 3: Auto-configure NiFi ingestion flow"
echo "============================================================"
# Wait for NiFi API to be responsive (it can take 60-120s after container start)
echo "  Waiting for NiFi API to be ready..."
NIFI_READY=0
for i in $(seq 1 24); do
  STATUS=$(curl -sk -o /dev/null -w "%{http_code}" \
    https://localhost:8443/nifi-api/system-diagnostics 2>/dev/null || echo "000")
  if [ "$STATUS" = "200" ] || [ "$STATUS" = "401" ]; then
    NIFI_READY=1
    echo "  NiFi API is up (attempt $i)."
    break
  fi
  echo "  ...still waiting ($i/24, status=$STATUS)"
  sleep 10
done

if [ "$NIFI_READY" = "1" ]; then
  # Check if flow already exists (idempotent — skip if processors already present)
  TOKEN=$(curl -sk -X POST https://localhost:8443/nifi-api/access/token \
    -d 'username=admin&password=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB' \
    -H 'Content-Type: application/x-www-form-urlencoded' 2>/dev/null)
  ROOT_PG="2a71a802-019e-1000-3a9a-0584a76ca790"
  PROC_COUNT=$(curl -sk \
    -H "Authorization: Bearer $TOKEN" \
    "https://localhost:8443/nifi-api/process-groups/$ROOT_PG/processors" 2>/dev/null \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("processors",[])))' 2>/dev/null || echo "0")

  if [ "$PROC_COUNT" -gt "0" ] 2>/dev/null; then
    echo "  NiFi flow already exists ($PROC_COUNT processors). Skipping creation."
  else
    echo "  Creating NiFi ingestion flow (Tables + PDFs → MinIO)..."
    python3 "$PROJECT_DIR/scripts/create_nifi_flow.py" && \
      echo "  ✅ NiFi flow created successfully." || \
      echo "  ⚠️  NiFi flow creation failed — run 'python3 scripts/create_nifi_flow.py' manually."
  fi
else
  echo "  ⚠️  NiFi did not become ready in time. Run manually after NiFi starts:"
  echo "      python3 scripts/create_nifi_flow.py"
fi

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
