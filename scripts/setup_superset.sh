#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Register Trino as a Superset database AND auto-build the operations
# dashboard via the Superset REST API.
#
# Idempotent — safe to re-run; existing DB connection is skipped, prior
# dashboard is deleted and recreated.
#
# Prereqs: ./scripts/start.sh + ./scripts/run_pipeline.sh complete.
# ─────────────────────────────────────────────────────────────────────────
set -e
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "============================================================"
echo "  Waiting for Superset to be reachable…"
echo "============================================================"
for i in {1..60}; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8088/health 2>/dev/null | grep -q 200; then
        echo "  ✓ Superset is up."
        break
    fi
    sleep 3
    echo -n "."
done

# ─────────────────────────────────────────────────────────────────────────
# Step 1 — login + acquire JWT for the admin user
# ─────────────────────────────────────────────────────────────────────────
LOGIN_JSON=$(curl -s -X POST http://localhost:8088/api/v1/security/login \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin","provider":"db","refresh":true}')
TOKEN=$(echo "$LOGIN_JSON" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

# ─────────────────────────────────────────────────────────────────────────
# Step 2 — register Trino as a database (skip if already present)
# ─────────────────────────────────────────────────────────────────────────
EXISTING=$(curl -s http://localhost:8088/api/v1/database/ \
    -H "Authorization: Bearer $TOKEN" \
    | python3 -c "import sys,json;print(sum(1 for d in json.load(sys.stdin).get('result',[]) if d['database_name']=='Trino (Data Lake)'))")

if [ "$EXISTING" -gt 0 ]; then
    echo "  • Trino DB already registered in Superset — skipping."
else
    echo "  Adding Trino as a Superset database…"
    curl -s -X POST http://localhost:8088/api/v1/database/ \
        -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d '{
            "database_name": "Trino (Data Lake)",
            "sqlalchemy_uri": "trino://admin@trino:8080/hive/curated",
            "expose_in_sqllab": true,
            "allow_run_async": true,
            "allow_dml": false,
            "extra": "{\"engine_params\": {\"connect_args\": {\"http_scheme\": \"http\"}}}"
        }' > /dev/null
    echo "  ✓ Trino registered."
fi

# ─────────────────────────────────────────────────────────────────────────
# Step 3 — build the operational dashboard (datasets + charts + layout)
# ─────────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Building Superset dashboard via API…"
echo "============================================================"
python3 "$PROJECT_DIR/scripts/build_superset_dashboard.py"

echo ""
echo "============================================================"
echo "  ✓ Superset ready."
echo "============================================================"
echo "  • Login: admin / admin"
echo "  • Dashboard: http://localhost:8088/superset/dashboard/medical-data-lake-ops/"
echo "  • SQL Lab:   http://localhost:8088/sqllab/"
echo "============================================================"
