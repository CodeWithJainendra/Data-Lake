#!/usr/bin/env bash
# Open a Trino CLI inside the container — for ad-hoc SQL exploration.
echo "Connecting to Trino..."
echo "Try: SHOW SCHEMAS IN hive;"
echo "     SHOW TABLES IN hive.curated;"
echo "     SELECT * FROM hive.curated.fact_claims LIMIT 10;"
echo ""
docker exec -it dl-trino trino --catalog hive --schema curated
