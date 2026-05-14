-- =====================================================================
-- DATA QUALITY DASHBOARD
-- Live health metrics across all tables in the data lake
-- =====================================================================

-- 1. Latest DQ snapshot per table
WITH latest AS (
    SELECT
        zone, table_name,
        MAX(run_timestamp) AS latest_run
    FROM hive.curated.dq_metrics
    GROUP BY zone, table_name
)
SELECT
    dq.zone,
    dq.table_name,
    dq.row_count,
    dq.duplicate_pct,
    dq.worst_null_column,
    dq.worst_null_pct,
    dq.status,
    dq.notes,
    dq.run_timestamp
FROM hive.curated.dq_metrics dq
INNER JOIN latest l
  ON dq.zone = l.zone
 AND dq.table_name = l.table_name
 AND dq.run_timestamp = l.latest_run
ORDER BY dq.status DESC, dq.zone, dq.table_name;


-- 2. DQ trend over time (row counts)
SELECT
    table_name,
    DATE_TRUNC('day', CAST(run_timestamp AS TIMESTAMP)) AS day,
    MAX(row_count) AS row_count
FROM hive.curated.dq_metrics
GROUP BY table_name, DATE_TRUNC('day', CAST(run_timestamp AS TIMESTAMP))
ORDER BY day DESC, table_name;


-- 3. Tables currently in FAIL or WARN state
SELECT *
FROM hive.curated.dq_metrics
WHERE status IN ('FAIL', 'WARN')
  AND run_timestamp = (SELECT MAX(run_timestamp) FROM hive.curated.dq_metrics);
