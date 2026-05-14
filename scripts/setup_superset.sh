#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Add Trino as a Superset database. Run AFTER scripts/run_pipeline.sh.
# ─────────────────────────────────────────────────────────────────────────
set -e
echo "Waiting for Superset to be reachable..."
for i in {1..60}; do
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:8088/health | grep -q 200; then
        echo " ready."
        break
    fi
    sleep 3
    echo -n "."
done

COOKIES=$(mktemp)
curl -s -c "$COOKIES" \
    -X POST http://localhost:8088/api/v1/security/login \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin","provider":"db","refresh":true}' \
    > /tmp/login.json

TOKEN=$(cat /tmp/login.json | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

echo "Adding Trino as a Superset database..."
# Note: URI uses /hive/curated to default to the curated schema (so users see
# fact_claims, dim_patient, etc. immediately in SQL Lab without typing the schema).
curl -s -b "$COOKIES" \
    -X POST http://localhost:8088/api/v1/database/ \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
        "database_name": "Trino (Data Lake)",
        "sqlalchemy_uri": "trino://admin@trino:8080/hive/curated",
        "expose_in_sqllab": true,
        "allow_run_async": true,
        "allow_dml": false,
        "extra": "{\"engine_params\": {\"connect_args\": {\"http_scheme\": \"http\"}}}"
    }'

echo ""
echo ""
echo "============================================================"
echo "  ✓ Superset connected to Trino"
echo "============================================================"
echo "  1. Open http://localhost:8088 (admin / admin)"
echo "  2. SQL Lab → New query → Database: 'Trino (Data Lake)'"
echo "  3. Run any query from sql/analytics/*.sql"
echo "  4. Save chart → add to dashboard"
echo "============================================================"
