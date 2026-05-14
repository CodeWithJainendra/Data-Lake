# Fixes Applied — Round 2 Audit

This document records all bugs / design gaps that were identified in the audit
and how they were fixed.

## 🔴 Critical bugs — all fixed

| # | Issue | Fix |
|---|-------|-----|
| 1 | `s3a://` vs `s3://` scheme mismatch between Spark and Trino — Trino native S3 fs can't read tables Spark registered with `s3a://` LOCATION | Unified on `s3://` everywhere. Spark configured with `fs.s3.impl=S3AFileSystem` so the S3A driver handles `s3://` URLs. Hive Metastore now stores `s3://` paths; Trino reads them natively. See `spark/conf/spark-defaults.conf` + `hive/conf/hive-site.xml`. |
| 2 | MinIO healthcheck used `curl` which isn't in the image — stack would stall forever | Replaced with pure-bash TCP probe: `timeout 2 bash -c '</dev/tcp/localhost/9000'`. No external binary needed. |
| 3 | PDF OCR crashed: `tesseract` + `poppler` system packages missing from bitnami Spark image (non-root, can't apt-get) | Created custom `spark/Dockerfile` that installs `tesseract-ocr` + `poppler-utils` + Python deps at build time. `docker-compose.yml` now uses `build:` for spark-master + spark-worker. |
| 4 | `__import__("pyspark.sql.window", ...)` hack inside chained transformations — fragile | Replaced with proper `from pyspark.sql.window import Window` at top of file. |
| 5 | Spark conf mount path `/opt/bitnami/spark/conf-custom` — bitnami reads from `/opt/bitnami/spark/conf/` | Mount now goes to the correct path. `hive-site.xml` also mounted alongside. |
| 6 | `start.sh` only downloaded Postgres JDBC jar — Hive Metastore needs `hadoop-aws` + `aws-sdk-bundle` too | Updated `start.sh` to download all three JARs into `hive/lib/`. |

## 🟡 Medium issues — all fixed

| # | Issue | Fix |
|---|-------|-----|
| 7 | Only CSV inputs supported — user's main concern | Created `spark/jobs/00_universal_ingestion.py` that auto-detects format (CSV/TSV/JSON/JSONL/Parquet/ORC/Excel/SQL dump/HL7 v2/FHIR JSON) and routes to the appropriate Spark reader. Plus `data-generator/generate_diverse_formats.py` creates sample files in all 7 alternate formats from the canonical CSV source. |
| 8 | NiFi templates were prose-only, no actual importable XML | Created two real importable templates: `nifi/templates/tables_ingestion_flow.xml` and `pdf_ingestion_flow.xml`. Both include the critical MinIO settings (path-style access, V4 signer, endpoint override) that 99% of NiFi+MinIO setups get wrong. |
| 9 | `MSCK REPAIR TABLE ... if False else None` dead code in 04 | Removed. |
| 10 | Null-pct loop did N table scans for N columns (O(N×M)) | Replaced with single-pass aggregation in `_common.null_pct_per_column()`. One scan, returns dict. |
| 11 | PDF OCR was driver-side single-threaded | Re-written using `spark.read.format("binaryFile")` + UDF — now distributes across workers. Bad PDFs go to `s3://dlq/pdfs/` with the error reason (no silent failures). |
| 12 | `mc anonymous set download local/raw` made raw bucket world-readable | Removed. Bucket is now authenticated-only. |
| 13 | Spark `enableHiveSupport()` without `hive-site.xml` in classpath | `hive-site.xml` now mounted into both spark-master and spark-worker containers at `/opt/bitnami/spark/conf/hive-site.xml`. |
| 15 | `partitionBy("state")` for patients = small files problem | Changed to `partitionBy("ingest_year", "ingest_month")` — ingestion-time partitions, larger files. |
| 16 | Superset URI didn't specify schema | Changed to `trino://admin@trino:8080/hive/curated` so SQL Lab opens directly in the curated schema. |

## 🟢 Design gaps — addressed

| # | Issue | Fix |
|---|-------|-----|
| 17 | No CDC / incremental ingestion | Added `spark/jobs/06_incremental_merge.py` — demonstrates the left-anti + union pattern for incremental claim updates. Includes a note on how this becomes a one-liner `MERGE INTO` with Iceberg/Delta. |
| 18 | No schema evolution handling | All CSV reads now use `mode=PERMISSIVE` + `columnNameOfCorruptRecord=_corrupt` — new columns in source don't break the job. |
| 19 | Failed PDFs silently swallowed | Stage 3 splits parsed rows into good + DLQ DataFrames. DLQ goes to `s3://dlq/pdfs/` and is registered as `dlq.pdfs` in Hive Metastore for monitoring. |
| 20 | No orchestration | `run_pipeline.sh` now retries each stage up to 2 times with 10-sec backoff. Documented as "step toward Airflow/Dagster" with a clear migration path. |
| 21 | No Trino RBAC for HIPAA PII | Added `trino/etc/access-control.properties` + `rules.json`. `curated.dim_patient` (unmasked) only readable by `data_engineer`/`admin` roles; everyone else gets `dim_patient_masked`. DLQ readable only by platform engineers. |
| 22 | NiFi state didn't persist | Added 6 dedicated Docker volumes (`nifi_data`, `nifi_conf`, `nifi_state`, `nifi_flowfile`, `nifi_content`, `nifi_provenance`). Flow state survives `docker compose down`. |

## Bonus additions

- **`spark/jobs/00_universal_ingestion.py`** — handles CSV, TSV, JSON, JSONL, Parquet, ORC, Excel (.xlsx), SQL INSERT dumps, HL7 v2 messages, and FHIR R4 JSON bundles. Bad files → DLQ.
- **`data-generator/generate_diverse_formats.py`** — emits the same medical data in 7 formats so the universal ingestion demo has real input to chew through.
- **Demo talking points** — at the demo, drop a JSON file + an HL7 file + an Excel file into MinIO and re-run Stage 0 live. Watching the same `patients` data flow in from 3 different formats simultaneously is the slam-dunk moment.
- **`spark/jobs/06_incremental_merge.py`** — the CDC pattern, runnable on its own with `--since YYYY-MM-DD`.

## What still needs your manual setup

1. **Build the Spark image** (one-time, ~3-5 min):
   ```bash
   ./scripts/start.sh    # this now triggers the docker compose build automatically
   ```
2. **Verify after first run**:
   ```bash
   docker exec -it dl-trino trino --catalog hive --schema curated
   trino> SHOW TABLES;
   trino> SELECT format, COUNT(*) FROM (
            SELECT _source_file AS format FROM curated.fact_claims
          ) GROUP BY format;
   ```

## Demo flow (updated for the multi-format story)

1. Show MinIO buckets — `raw/tables/`, `raw/json/`, `raw/excel/`, `raw/hl7/`, etc.
2. Run Stage 0 live — `spark-submit 00_universal_ingestion.py` — show the same data being ingested from 7 different formats
3. Run Stage 3 live — show PDF OCR distributed across workers, DLQ for failures
4. Open Trino — `SELECT * FROM hive.curated.fact_claims LIMIT 5;` works (no s3a:// error)
5. Show RBAC — try `SELECT * FROM curated.dim_patient` as a non-privileged user → ACCESS DENIED. Then `SELECT * FROM curated.dim_patient_masked` → works.
6. Open Superset, point at a dashboard. Done.
