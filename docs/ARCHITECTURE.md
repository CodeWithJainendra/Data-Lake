# Architecture — Medical & Billing Data Lake

## Design Goals

1. **Handle both structured and unstructured data** — claims, encounters, and patient records live in tables; clinical notes, lab reports, and discharge summaries arrive as PDFs.
2. **Decouple storage from compute** — store everything cheaply in object storage (MinIO/S3), query it with whatever engine fits the workload (Spark for ETL, Trino for interactive SQL).
3. **Production-style discipline at demo scale** — lineage, PII masking, data quality, and a catalog from day one. The same architecture scales from 10k rows to 10B with no rewrite.
4. **Open-source only** — every component is the same one used by Netflix, Airbnb, and JP Morgan in production.

## The 7 Layers

### 1. Data Sources

In production these are:
- **Structured**: ~300 tables in hospital EHRs (Epic, Cerner), billing systems (Athenahealth, eClinicalWorks), and claim clearinghouses.
- **Unstructured**: scanned lab reports, discharge summaries, prescriptions — typically arriving via fax, scanner, or HL7/FHIR.

For this demo we **synthesize** equivalent data using `Faker` + curated ICD-10/CPT reference tables. The schemas mirror the ones found in real healthcare warehouses (HCC categories, RAF weights, denial reason codes, CARC codes).

### 2. Ingestion (Apache NiFi)

NiFi is the "data conveyor belt" — it watches the source systems and moves files into the data lake.

Two flows:
- **Tables ingestion**: NiFi `ListFile → FetchFile → PutS3Object` reads CSVs nightly and writes them to `s3://raw/tables/<table_name>/yyyy=YYYY/mm=MM/dd=DD/`.
- **PDF ingestion**: a `ListenHTTP` processor accepts uploaded PDFs in near-real-time, computes a hash, and writes them to `s3://raw/pdfs/`.

**Why NiFi vs Airflow/Fivetran?** NiFi excels at file movement and binary data (PDFs). Airflow is better for orchestrating SQL transformations downstream. We use both philosophically (NiFi for ingest, Spark jobs are the "Airflow tasks" in this demo).

### 3. Storage (MinIO)

MinIO is an S3-compatible object store running locally. In AWS this would be S3, in Azure ADLS, in GCP GCS. The Spark code is identical — only credentials change.

Three zones, following the **medallion architecture**:

| Zone | Purpose | Mutability | Format |
|------|---------|------------|--------|
| `raw/` | Bronze — exactly as received from source | Immutable | CSV, JSON, PDF |
| `processed/` | Silver — cleansed, typed, deduped | Append-only | Parquet (snappy) |
| `curated/` | Gold — analytics-ready facts & dims | Overwritten on each run | Parquet, partitioned |

**Why Parquet?** Columnar, compressed, schema-aware. Trino reads only the columns it needs. A `SELECT denial_reason FROM fact_claims` scans <5% of the bytes a CSV would.

### 4. Processing (Apache Spark)

Five PySpark jobs run in sequence:

1. **`01_raw_to_processed.py`** — type coercion, null handling, deduplication, regex validation (ICD-10, CPT, NPI formats), and lineage column injection (`_source_file`, `_ingested_at`, `_pipeline_version`).
2. **`02_processed_to_curated.py`** — joins, rollups, and dimensional modeling. Builds star schema with `dim_patient`, `dim_provider`, `dim_payer`, `dim_date`, and fact tables (`fact_encounters`, `fact_claims`, `fact_diagnoses`). Pre-computes aggregates for dashboards.
3. **`03_pdf_ocr_pipeline.py`** — PyPDF2 for digital PDFs, Tesseract fallback for scanned ones. Regex-extracts MRN, DOB, ICD-10 codes, medications. Indexes results to a queryable `dim_clinical_documents` table.
4. **`04_data_quality_checks.py`** — per-table row counts, null %, duplicate %, freshness, and outliers. Writes results to `dq_metrics` (powers the Data Health dashboard).
5. **`05_pii_masking.py`** — HIPAA Safe Harbor transformation. Salted SHA-256 hashing for IDs, year-only DOB, ZIP3 only, name initials only. Produces `dim_patient_masked` for downstream analytics/research.

**Why Spark?** Distributes naturally. The exact code runs on 1 worker (here) or 1,000 workers (Databricks) with no changes. Adaptive Query Execution (`spark.sql.adaptive.enabled=true`) auto-tunes partitions.

### 5. Catalog (Hive Metastore + Postgres)

The Metastore is the "phone book" — it remembers that `s3://curated/tables/fact_claims/` is actually a Parquet table with columns `claim_id, patient_id, billed_amount, ...` partitioned by `(submitted_year, submitted_month)`.

It's backed by Postgres for persistence. Both Spark and Trino talk to it via Thrift (port 9083).

**Why this and not Glue/Unity Catalog?** Hive Metastore is the open-source de facto standard. AWS Glue, Databricks Unity, and Snowflake Polaris are all wire-compatible with it. Code written here works against any of them in production.

### 6. Query (Trino)

Trino (formerly PrestoSQL) is the interactive query engine — sub-second responses over terabytes of Parquet.

It reads from the Hive catalog, applies pushdown predicates (`WHERE submitted_year = 2026` reads only that partition), and supports standard ANSI SQL with window functions, CTEs, lateral joins, and `UNNEST`.

The Superset dashboard sends every query to Trino, which scans the relevant Parquet files in MinIO.

### 7. Visualization (Apache Superset)

Open-source BI tool — connects to Trino via SQLAlchemy and renders dashboards.

Suggested dashboards (queries provided in `sql/analytics/`):
- **Claim Denial Analysis** — top reasons, by payer, trended monthly
- **Revenue Cycle** — billed → allowed → paid funnel, days in AR
- **Clinical Documentation Improvement** — RAF capture, severity coding gaps
- **Data Quality Health** — live status per table from `dq_metrics`
- **Provider KPIs** — denial rates by physician, case-mix index

## Data Flow (End-to-End)

```
[Source CSVs]──NiFi──▶[s3://raw/tables/*.csv]
                           │
                           │ ┌────────────────────────────┐
                           ▼ │                            ▼
[Source PDFs]──NiFi──▶[s3://raw/pdfs/*.pdf]      [Spark Job 01]
                                                          │
                                                          ▼
                                                 [s3://processed/*]
                                                          │
                                                          ▼
                                                 [Spark Job 02 + 03 + 05]
                                                          │
                                                          ▼
                                                 [s3://curated/*]
                                                          │
                                  ┌───────────────────────┼───────────────┐
                                  ▼                       ▼               ▼
                          [Hive Metastore]          [Spark Job 04]   [Trino SQL]
                          (catalog)                 (DQ metrics)          │
                                                                          ▼
                                                                    [Superset]
```

## Why This Beats a Traditional Data Warehouse

| Concern | Traditional DW (e.g. Oracle) | This Data Lake |
|---------|------------------------------|----------------|
| Storage cost | $$$ (proprietary block storage) | $ (commodity object storage) |
| Schema flexibility | Schema-on-write (rigid) | Schema-on-read (PDFs, JSON, etc. live alongside tables) |
| Compute scaling | Vertical, license-locked | Horizontal, elastic |
| Tool lock-in | Vendor-specific SQL dialect | ANSI SQL via Trino; same code on Athena, BigQuery, Databricks |
| Unstructured data | Out of scope | Native — PDFs sit next to tables in the same bucket |
| Cost of failed experiment | High (need DBA, schema migration) | Near-zero (just write new files) |

## Production Scaling Considerations

If this needed to support 10M patients, 100M claims/yr:
- **MinIO → AWS S3** (or Azure ADLS Gen2)
- **Spark worker count** scale to 20-50, use EMR / Databricks
- **Hive Metastore** → AWS Glue (managed) or Databricks Unity Catalog
- **Trino → Athena** (serverless) or **Starburst** (managed Trino)
- **Superset → Tableau / PowerBI** (or stay on Superset Cloud)
- Add **Apache Iceberg** or **Delta Lake** for ACID transactions, time travel, and schema evolution
- Add **Great Expectations** for declarative DQ rules
- Add **OpenLineage** integration so every Spark job emits lineage events to a Marquez/DataHub catalog
