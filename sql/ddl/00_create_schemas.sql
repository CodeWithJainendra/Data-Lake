-- =====================================================================
-- Schema creation — runs in Hive Metastore via Spark or beeline
-- Trino sees these schemas automatically through the 'hive' catalog
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS raw       LOCATION 's3a://raw/';
CREATE SCHEMA IF NOT EXISTS processed LOCATION 's3a://processed/';
CREATE SCHEMA IF NOT EXISTS curated   LOCATION 's3a://curated/';

-- After Spark jobs run, tables are auto-registered. To inspect manually:
--   SHOW SCHEMAS IN hive;
--   SHOW TABLES IN hive.curated;
--   DESCRIBE hive.curated.fact_claims;
