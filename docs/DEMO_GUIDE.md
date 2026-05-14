# Demo Guide — 5-Minute Walkthrough

This is the script for demonstrating the data lake to your team / interviewer. Memorize the flow; run the commands live.

## Prerequisites Before Demo

```bash
./scripts/start.sh           # ~2 min to come up
./scripts/load_sample_data.sh   # ~3 min to generate + upload
./scripts/run_pipeline.sh    # ~5 min to run all 5 Spark stages
./scripts/setup_superset.sh  # connects Superset → Trino
```

Once done, all data is in place. The demo runs against pre-populated state.

## The Demo (5 Minutes)

### Minute 1 — The Problem & The Architecture (talk track)

> "We have two streams of medical data — structured claims and encounters in 300+ tables, and unstructured clinical PDFs like lab reports and discharge summaries. Until now they've lived in separate silos. This data lake unifies them so the CDI team can answer cross-cutting questions like *'which denied claims have a discharge summary that suggests a more severe diagnosis was missed?'*"

Show `docs/ARCHITECTURE.md` diagram or whiteboard the 7 layers.

### Minute 2 — Storage layer (MinIO)

Open http://localhost:9001 (admin / admin123456).

Show the three buckets — `raw/`, `processed/`, `curated/`.
- Click into `raw/tables/` — show the CSVs as they arrived.
- Click into `raw/pdfs/` — show the actual PDFs.
- Click into `curated/tables/fact_claims/` — show partitioned Parquet files.

> "Same data, three forms. Bronze is exactly what we received. Silver is cleaned. Gold is analytics-ready."

### Minute 3 — Processing layer (Spark)

Open http://localhost:8080 — show the Spark Master UI with the worker.

In a terminal, kick off a quick re-run to show it live:

```bash
docker exec dl-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/04_data_quality_checks.py
```

Show the live progress.

> "Five Spark jobs handle everything — cleansing, joins, OCR for the PDFs, data-quality checks, and HIPAA PII masking. The code scales unchanged from one worker to a thousand."

### Minute 4 — Query layer (Trino)

```bash
./scripts/connect_trino.sh
```

In Trino CLI:

```sql
-- show the catalog
SHOW SCHEMAS IN hive;
SHOW TABLES IN hive.curated;

-- the headline number
SELECT
    COUNT(*) as total_claims,
    SUM(billed_amount) as total_billed,
    SUM(paid_amount)   as total_collected,
    ROUND(SUM(CASE WHEN is_denied THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as denial_rate_pct
FROM hive.curated.fact_claims;

-- the killer query — denial reasons by payer
SELECT payer_name, denial_reason_code, COUNT(*) as denials, ROUND(SUM(billed_amount), 2) as dollars_at_risk
FROM hive.curated.fact_claims
WHERE is_denied = TRUE
GROUP BY payer_name, denial_reason_code
ORDER BY dollars_at_risk DESC
LIMIT 10;
```

> "Trino is reading Parquet files directly out of MinIO — no warehouse, no ETL into a database. Sub-second over the entire dataset."

### Minute 5 — Visualization (Superset)

Open http://localhost:8088 (admin / admin).

- Show the Trino database connection.
- Open SQL Lab → run any query from `sql/analytics/01_claim_denial_analysis.sql`.
- Click "Save Chart" → pick a chart type (bar/pie).
- Show a pre-built dashboard if you've made one.

> "Every chart here is live SQL hitting Trino, which hits the Parquet files. Refresh and the numbers update."

## Killer Talking Points (memorize these)

When asked questions, hit these:

**"How does this handle HIPAA?"**
> "We have a dedicated PII masking job (`05_pii_masking.py`) that produces a HIPAA Safe Harbor compliant dataset — SHA-256 hashed identifiers, year-only DOB, ZIP3 only, name initials only. The unmasked version is access-controlled at the Trino layer via RBAC."

**"How do you ensure data quality?"**
> "Every Spark job emits row counts, null percentages, duplicates, and freshness to a `dq_metrics` table. The Data Health dashboard in Superset shows table-level status — pass/warn/fail — and the trend over time. Failures alert via Superset alerts."

**"How would this scale?"**
> "MinIO → S3 in production. Spark workers scale horizontally — same code, 50 workers. The Hive Metastore is the only stateful piece; we'd move it to AWS Glue. Trino has a separate worker pool that scales independently of ingestion. The architecture is identical at 1B rows."

**"What about lineage?"**
> "Every row has `_source_file`, `_ingested_at`, `_pipeline_version`. We can trace any number on any dashboard back to the exact CSV or PDF it came from."

**"How long would a real implementation take?"**
> "The hard part isn't the technology — it's the source-system integrations and the domain modeling. The plumbing here (Docker Compose, Spark jobs, Trino config) is identical to what would run in production; only the source connectors and credentials change. I'd estimate 6-12 weeks for a first production rollout per source system."

## Recovery if Something Breaks Live

If a query is slow or a service is down:
1. Don't panic. Say *"This is one place we'd add caching in production — let me show you the saved result."*
2. Show the static SQL file (`sql/analytics/01_claim_denial_analysis.sql`) and the README architecture diagram.
3. Pivot to talking through the design rather than the live demo.

## After the Demo

Leave the audience with:
- `README.md` link
- `docs/ARCHITECTURE.md` link
- This `docs/DEMO_GUIDE.md`
- Offer to walk through any specific Spark job in detail
