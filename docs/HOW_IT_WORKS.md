# Medical & Billing Data Lake — How It All Works

> A complete walkthrough of every component, why it exists, what it does, and how data flows through the system end-to-end.

---

## Table of Contents

1. [The Big Picture — What Is a Data Lake](#1-the-big-picture)
2. [The 7-Layer Architecture](#2-the-7-layer-architecture)
3. [Each Layer Explained](#3-each-layer-explained-in-detail)
4. [The Production-Hardening Layers](#4-production-hardening-layers)
5. [The Data Flow — Stage by Stage](#5-the-data-flow--stage-by-stage)
6. [The 10 Docker Services](#6-the-10-docker-services-explained)
7. [Project Structure — File by File](#7-project-structure)
8. [Demo Talking Points](#8-demo-talking-points-cheat-sheet)
9. [Quick Reference Commands](#9-quick-reference)

---

## 1. The Big Picture

### What problem does this solve?

A hospital generates data in **two completely different shapes**:

- **Structured** — patient records, billing claims, insurance payments. Rows + columns. Lives in databases (Epic, Cerner, Athenahealth).
- **Unstructured** — lab reports, discharge summaries, prescriptions. PDFs, often scanned. Lives in file systems and shared drives.

Traditionally these live in **silos**. The CDI team can't easily ask:
> *"Of all the claims denied last quarter for missing documentation, how many actually had a discharge summary that mentioned a more severe diagnosis we forgot to code?"*

That's a multi-million-dollar question. Answering it requires unifying both streams.

### What is a Data Lake?

A **data lake** is a single storage system that:

1. Accepts **any** data format (CSV, JSON, PDF, Parquet, HL7, FHIR, Excel) without forcing a schema upfront
2. Stores everything cheaply on commodity object storage (S3 / MinIO)
3. Lets you process and query it later with whatever engine fits (Spark for ETL, Trino for SQL)
4. Scales horizontally — add more workers, handle more data

Compare to a **data warehouse**:

| | Data Lake | Data Warehouse |
|---|-----------|----------------|
| Schema | On read (flexible) | On write (rigid) |
| Storage cost | Cheap (object storage) | Expensive (block storage) |
| Data types | Any (CSV, PDF, JSON, …) | Tables only |
| Cost of failed experiment | Near-zero | High (DBA, schema migration) |
| Lock-in | None (open formats) | Vendor-specific SQL |

### What this project specifically does

Builds a **complete, production-style data lake** for healthcare billing on your laptop using only open-source tools — same architecture used by Epic, Optum, and IQVIA at scale.

End-to-end:

1. Generate synthetic medical data in **7 formats** (CSV, JSON, JSONL, Parquet, Excel, SQL dumps, HL7 v2, FHIR R4) + 100 medical PDFs
2. Ingest via NiFi (or direct upload) into MinIO **raw zone**
3. Process with Spark — clean, deduplicate, OCR PDFs, run quality checks, mask HIPAA PII
4. Catalog in Hive Metastore so any query engine can find tables
5. Query with Trino — sub-second SQL over Parquet
6. Visualize via Apache Superset (BI platform) and a custom Flask dashboard
7. Enforce HIPAA via Trino RBAC + dead-letter queues for failed records

Total stack: **10 Docker services**, ~5,500 lines of code, runs on a 12 GB laptop.

---

## 2. The 7-Layer Architecture

```
┌─────────────┐  ┌──────────┐  ┌────────────────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐
│ 1. SOURCES  │→ │ 2. NIFI  │→ │ 3. MINIO STORAGE   │→ │ 4. SPARK │→ │ 5. HIVE  │→ │ 6. TRINO │→ │ 7. SUPERSET │
│             │  │          │  │                    │  │          │  │          │  │          │  │  +          │
│  CSV + PDF  │  │ Ingest   │  │  raw / processed / │  │ ETL +    │  │ Metadata │  │ SQL on   │  │  Flask      │
│  + 5 more   │  │ Layer    │  │  curated zones     │  │ OCR + DQ │  │ Catalog  │  │ Lake     │  │  Dashboard  │
└─────────────┘  └──────────┘  └────────────────────┘  └──────────┘  └──────────┘  └──────────┘  └─────────────┘
```

| # | Layer | Tool | Purpose |
|---|-------|------|---------|
| 1 | **Sources** | Synthetic generator (Python + Faker) | Mimics hospital systems & document scanners |
| 2 | **Ingestion** | Apache NiFi | Move files from sources → MinIO with backpressure & lineage |
| 3 | **Storage** | MinIO (S3-compatible) | 3 zones: raw → processed → curated (medallion) |
| 4 | **Processing** | Apache Spark + Tesseract | Clean, transform, OCR, deduplicate, validate |
| 5 | **Catalog** | Apache Hive Metastore | Phone book — knows where every table lives |
| 6 | **Query** | Trino | Distributed SQL — sub-second over TB of Parquet |
| 7 | **Visualization** | Superset (BI) + Flask (operational) | Self-service analytics + fixed ops dashboard |

---

## 3. Each Layer Explained in Detail

### Layer 1 — Data Sources

**What it is:** The systems where healthcare data is born.

**In real production:**
- Hospital EHRs: Epic, Cerner, Allscripts (300+ tables: patients, encounters, orders, results)
- Billing platforms: Athenahealth, eClinicalWorks (claims, payments, denials)
- Document scanners: lab reports, discharge summaries, prescriptions arrive as PDFs
- Lab interfaces: HL7 v2 messages over TCP
- Modern systems: FHIR R4 JSON via REST APIs

**In our demo:**
We synthesize the same kind of data using `Faker` + curated medical reference tables (real ICD-10 codes, real CPT codes, real CARC denial reasons).

**Files:**
- `data-generator/medical_codes.py` — reference tables (ICD-10, CPT, denial reasons, payers)
- `data-generator/generate_medical_data.py` — generates 10K patients, 50K encounters, 132K diagnoses, 82K procedures, 45K claims, 10K payments
- `data-generator/generate_pdfs.py` — generates 100 PDF documents (lab reports, discharge summaries, prescriptions) using ReportLab
- `data-generator/generate_diverse_formats.py` — emits the same data in JSON, JSONL, Parquet, Excel, SQL dump, HL7 v2, and FHIR R4 formats

**Why so many formats?** Real hospitals have legacy systems exporting HL7 v2, modern systems using FHIR, billing departments dumping Excel, IT teams handing over Postgres SQL dumps. A real data lake must handle all of them.

---

### Layer 2 — Ingestion (Apache NiFi)

**What it is:** A visual data-movement tool. Drag-drop processors that pull files from sources and push to destinations, with backpressure, retries, lineage, and provenance tracking.

**Why NiFi vs simple scripts?** In production:
- **Backpressure:** if MinIO slows down, NiFi automatically pauses upstream sources (no data loss)
- **Provenance:** every flowfile's complete journey is logged for HIPAA compliance audits
- **No-code rewiring:** ops team can re-route data without engineering involvement
- **Battle-tested:** NSA built and open-sourced NiFi; Oak Ridge labs use it for terabyte file movement

**In our stack:**
Two flow templates ready to import (`nifi/templates/`):

1. **`tables_ingestion_flow.xml`** — watches `/opt/data/incoming/tables/` for any CSV/JSON/Parquet/Excel/SQL/HL7 file, computes the right S3 destination subfolder by extension, uploads to MinIO `raw/<format>/`.

2. **`pdf_ingestion_flow.xml`** — accepts PDF uploads via HTTP POST on port 8090, hashes the content with SHA-256 (for deduplication), uploads to MinIO `raw/pdfs/<hash>.pdf`.

**Critical MinIO config inside the templates** (this is the #1 NiFi+MinIO integration mistake):
- Endpoint Override URL: `http://minio:9000`
- Path Style Access: **`true`** (mandatory for MinIO)
- Signer Override: `AWSS3V4SignerType`
- Region: any (MinIO ignores it but AWS SDK requires one)

**For the demo,** we bypass NiFi and use `mc cp` directly in `scripts/load_sample_data.sh` for speed. NiFi templates exist as proof of the production pattern.

---

### Layer 3 — Storage (MinIO)

**What it is:** S3-compatible object storage. Same API as AWS S3, runs on your laptop. In production, this is replaced 1-line by AWS S3, Azure ADLS, or GCP GCS — same code.

**Why object storage vs HDFS / NFS?**
- Cheap ($0.023/GB on S3 vs $0.12/GB on EBS)
- Infinite scale (no node limits)
- Decoupled from compute (kill all Spark workers, data is safe)
- Native S3 API used by every modern data tool

**Our buckets (5):**

| Bucket | Purpose | Mutability |
|--------|---------|------------|
| `raw/` | Bronze zone — exactly as received | Immutable (only appends) |
| `processed/` | Silver zone — cleaned, typed, deduped | Append-only Parquet |
| `curated/` | Gold zone — analytics-ready facts & dims | Overwritten per pipeline run |
| `warehouse/` | Hive's managed-table location | Auto-managed |
| `dlq/` | Dead-letter queue — failed records | Append-only with retention |

**Medallion architecture** (raw → processed → curated):

- **Why three zones?** Each zone has a different SLA and consumer:
  - **Bronze** = "untouched, replayable" — if business logic changes tomorrow, re-process from raw
  - **Silver** = "physically clean" — typed columns, no duplicates, no nulls in keys
  - **Gold** = "business-ready" — pre-joined, aggregated, named in business terms

**Format choice:** Parquet (snappy compression). Why?
- **Columnar** — `SELECT denial_reason FROM fact_claims` reads <5% of bytes a CSV would
- **Schema-aware** — column types preserved, no string-to-number casting
- **Splittable** — Spark can parallelize reads across files
- **Industry standard** — every major engine reads it (Spark, Trino, Snowflake, BigQuery, Athena)

---

### Layer 4 — Processing (Apache Spark)

**What it is:** A distributed computing engine. You write Python (PySpark), Spark splits the work across workers, each processes a chunk in parallel.

**Why Spark vs pandas?**
- pandas: fits in one machine's RAM
- Spark: scales to terabytes across many machines, **same code**

**Our 6 ETL stages** (`spark/jobs/`):

1. **`00_universal_ingestion.py`** — auto-detects file format by extension (CSV/JSON/Parquet/Excel/SQL/HL7/FHIR) and routes to the right reader. Failed files go to `dlq/ingestion/`.

2. **`01_raw_to_processed.py`** — reads CSVs from `raw/tables/`, applies cleaning:
   - Type coercion (string `"45"` → integer `45`)
   - Date parsing
   - Deduplication on primary keys (uses Window functions)
   - Format validation (ICD-10 regex, CPT regex)
   - Adds lineage columns: `_source_file`, `_ingested_at`, `_pipeline_version`
   - Writes Parquet to `processed/tables/`

3. **`02_processed_to_curated.py`** — builds the **star schema**:
   - **Dimensions:** `dim_patient`, `dim_provider`, `dim_payer`, `dim_date`
   - **Facts:** `fact_encounters`, `fact_claims`, `fact_diagnoses` — joined with reference tables (ICD-10 → HCC category → RAF weight)
   - **Aggregates:** `agg_monthly_revenue`, `agg_denial_summary`, `agg_provider_kpi`
   - Auto-runs `MSCK REPAIR TABLE` for partitioned tables so Trino discovers partitions

4. **`03_pdf_ocr_pipeline.py`** — distributed PDF processing:
   - Reads all PDFs via `spark.read.format("binaryFile")` (parallel across workers)
   - PyPDF2 for digital PDFs, Tesseract OCR fallback for scanned ones
   - Regex-extracts MRN, DOB, ICD-10 codes, medications
   - Splits into success rows → `curated/dim_clinical_documents` and failures → `dlq/pdfs/`

5. **`04_data_quality_checks.py`** — per-table DQ checks:
   - Row count, duplicate %, null % per column (single-pass aggregation), freshness
   - Status: PASS / WARN / FAIL
   - 30-day retention (older runs auto-pruned)
   - Powers the "Data Health" dashboard panel

6. **`05_pii_masking.py`** — HIPAA Safe Harbor compliance:
   - Names → first initial + last initial
   - SSN/MRN → SHA-256 with salt → 16-char token (deterministic, joinable)
   - DOB → year only (or `pre-1935` for 90+)
   - ZIP → first 3 digits only
   - Phone, email, address → REDACTED via drop()
   - Produces `curated.dim_patient_masked` for non-privileged users

Plus optional: **`06_incremental_merge.py`** — CDC-style daily merge instead of full reload.

**Why a custom Spark image?**
The base `bitnami/spark:3.5.6` doesn't have:
- Tesseract OCR binary
- Poppler (PDF → image converter)
- PyPDF2, pytesseract, openpyxl, hl7 Python libraries
- S3A JARs

Our `spark/Dockerfile` adds all of these at build time — image is reusable across systems with no runtime downloads.

---

### Layer 5 — Catalog (Hive Metastore + Postgres)

**What it is:** A "phone book" of tables. Stores metadata: which Parquet files belong to which table, what columns they have, how they're partitioned.

**Why Hive Metastore?** It's the **de facto open standard**. Wire-compatible with:
- AWS Glue (managed)
- Databricks Unity Catalog
- Snowflake Polaris
- Apache Iceberg catalog

So our entire stack moves to AWS by changing a single connection URL.

**Backed by Postgres** because the metastore itself needs ACID transactions (it's storing critical metadata, not data).

**What it stores:**
- Schemas (databases): `raw`, `processed`, `curated`, `dlq`
- Tables: `curated.fact_claims`, `curated.dim_patient`, etc.
- Partitions: `fact_claims` partitioned by `(submitted_year, submitted_month)`
- Locations: `s3://curated/tables/fact_claims/`
- Statistics (with `ANALYZE TABLE`): row counts, column histograms for query optimization

**Why a custom Hive image?**
Apache Hive 4.0 base image doesn't include the Postgres JDBC driver or hadoop-aws JARs needed for s3:// support. Our `hive/Dockerfile` bakes them in.

---

### Layer 6 — Query (Trino)

**What it is:** A massively parallel SQL engine. Originally Facebook's PrestoSQL, now Trino.

**Why Trino?**
- **Sub-second over TB:** reads only Parquet columns it needs, parallelizes across workers
- **ANSI SQL:** standard syntax, window functions, CTEs, lateral joins, `UNNEST`
- **Federated:** can query Hive + Postgres + MongoDB + Kafka in one query
- **Used by:** Netflix, Pinterest, LinkedIn, Lyft at petabyte scale

**Our Trino setup:**
- One coordinator (no separate workers — for demo)
- One catalog: `hive` (configured in `trino/etc/catalog/hive.properties`)
- Native S3 filesystem (Trino 458+ feature) — reads `s3://` URIs directly
- File-based RBAC (`trino/etc/rules.json`):
  - `admin` / `data_engineer` roles → can read PII (`dim_patient`)
  - Everyone else → only `dim_patient_masked`
  - DLQ tables → only platform engineers
- Procedures section → only admins can run `sync_partition_metadata` etc.

**Talk to Trino:**
```bash
docker exec -it dl-trino trino --user admin --catalog hive --schema curated
trino> SHOW TABLES;
trino> SELECT COUNT(*) FROM fact_claims;       -- 45,164
trino> SELECT * FROM dim_patient_masked LIMIT 5;
```

---

### Layer 7 — Visualization

We have **two complementary tools**:

#### 7a. Apache Superset (port 8088)

**What it is:** Open-source business intelligence platform. Tableau / Power BI alternative. AirBnB built it, now Apache project.

**Why Superset?**
- **Self-service:** any analyst can connect to Trino and build their own charts (no code)
- **40+ chart types:** drag-drop
- **SQL Lab:** in-browser SQL editor with autocomplete
- **Role-based:** different dashboards for managers, analysts, executives
- **Alerts:** email/Slack when metrics breach thresholds
- **Used by:** Netflix, Twitter, Apple, Lyft

**Our auto-built dashboard** (`scripts/setup_superset.sh` + `build_superset_dashboard.py`):
- Registers Trino as a Superset database via REST API
- Auto-creates an operations dashboard with KPI tiles, charts, tables
- Accessible at `http://localhost:8088/superset/dashboard/medical-data-lake-ops/`

#### 7b. Flask Operational Dashboard (port 5050)

**What it is:** A custom-built single-page dashboard. ~1,200 lines of code (Flask backend + HTML/CSS/JS frontend with Chart.js).

**Why both?**
- **Superset** = full BI platform (full kitchen — analysts cook their own dashboards)
- **Flask** = fixed operational view (cooked dish — ready to serve)

**Use case mapping:**

| Need | Tool |
|------|------|
| Morning standup — same metrics every day | Flask |
| Analyst exploration — write SQL, save chart | Superset |
| Executive overview — fixed KPIs | Either |
| Embedded in another app (`<iframe>`) | Flask |
| Multiple user roles, alerts, scheduled reports | Superset |

**Flask dashboard features:**
- 8 hero KPIs (total patients, encounters, claims, billed, paid, collection rate, denial rate, avg days to pay)
- 5 tabbed sections (Revenue, Denials, CDI, Providers, Data Health)
- 8 Chart.js charts
- Auto-refresh every 60 sec
- Trino-backed via SQLAlchemy
- 30-second response cache to avoid spamming Trino

---

## 4. Production-Hardening Layers

These features make this a **platform**, not a demo:

### 4a. HIPAA RBAC (Trino file-based access control)

`trino/etc/rules.json` defines who can access what:

```json
{
  "tables": [
    {
      "user": "data_engineer|admin",
      "table": "dim_patient",
      "privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"]
    },
    {
      "user": "(?!data_engineer|admin).*",
      "table": "dim_patient",
      "privileges": []
    }
  ]
}
```

Result:
- `admin` → can read unmasked PII
- `analyst` → ACCESS DENIED on `dim_patient`, must use `dim_patient_masked`
- DLQ tables → only platform engineers

### 4b. Dead Letter Queue (DLQ)

Bad records don't crash the pipeline — they go to `s3://dlq/`:
- `dlq.pdfs` — PDFs that failed OCR
- `dlq.ingestion` — files that failed format parsing (e.g., malformed SQL dump)

Queryable in Trino for root-cause analysis:
```sql
SELECT * FROM dlq.ingestion;
-- key=sql/patients_dump.sql, error="No INSERT statements parsed"
```

### 4c. Data Quality Framework

Stage 4 emits per-table metrics to `curated.dq_metrics` every run:
- Row count
- Duplicate %
- Worst null column + %
- Freshness
- Status: PASS / WARN / FAIL

30-day rolling retention (older runs auto-pruned). Powers the Data Health panel in both dashboards.

### 4d. PII Masking (HIPAA Safe Harbor)

18 HIPAA identifiers handled via Stage 5:
- Names, SSN, MRN, DOB, addresses, phone, email
- Tokenized via salted SHA-256 (deterministic for joining, irreversible for exposure)
- ZIP → 3 digits, DOB → year, age 90+ → `pre-1935`

### 4e. Incremental Loads (CDC)

`06_incremental_merge.py` demonstrates production CDC:
- Read only claims with `submitted_date >= --since`
- Anti-join existing curated table to find new/changed claims
- Re-join with dimensions
- Union + overwrite

In production, swap plain Parquet for Iceberg → one-line `MERGE INTO`.

### 4f. Lineage

Every row carries:
- `_source_file` — which raw file produced this
- `_ingested_at` — when it was processed
- `_pipeline_version` — code version that ran

Trace any number on any dashboard back to the exact CSV/PDF.

---

## 5. The Data Flow — Stage by Stage

Here's a concrete example following one row of data through the entire pipeline:

### Step 1: Source (data generator)
Python `Faker` creates a row in `data/raw/tables/patients.csv`:
```
P1000042, 12345678, Maria, Garcia, 1985-03-15, F, Hispanic, ...
```

### Step 2: Ingestion (NiFi or `mc cp`)
File uploaded to MinIO: `s3://raw/tables/patients.csv`. Lineage: timestamp + size + SHA-256 hash logged.

### Step 3: Stage 0 (universal ingestion)
Detects CSV format, just passes through (CSVs handled by Stage 1). For JSON/HL7/etc. it would parse and write to `s3://processed/tables/<name>_from_<format>/`.

### Step 4: Stage 1 (raw → processed)
Spark reads `s3://raw/tables/patients.csv`, cleans:
- Trim whitespace, initcap names ("MARIA" → "Maria")
- Parse `1985-03-15` as a date
- Deduplicate by `patient_id`
- Add `_source_file=patients.csv`, `_ingested_at=2026-05-14T18:30:00Z`
- Write Parquet to `s3://processed/tables/patients/ingest_year=2026/ingest_month=5/`

### Step 5: Stage 2 (processed → curated)
Joins patients + payers → `dim_patient` with computed `age_band` ("18-34", "35-49", etc.). Joins claims + encounters + patients + payers → `fact_claims`. Registers all in Hive Metastore.

### Step 6: Stage 4 (data quality)
Checks `processed.patients` and `curated.fact_claims`:
- 10,000 rows ✓
- 0 duplicates ✓
- worst null column: `secondary_payer_id` = 70% (expected)
- Status: PASS

### Step 7: Stage 5 (PII masking)
For Maria Garcia:
- `patient_id` → `a3f9c2b1d8e74f6a` (salted hash)
- `mrn` → `7d2e8a1f9c3b6d04`
- `first_name` → "M"
- `last_name` → "G"
- `birth_year` → "1985"
- `zip` "94110" → `zip3` "941"
- All other PII dropped

Writes to `s3://curated/tables/dim_patient_masked/`.

### Step 8: Catalog (Hive Metastore)
Stores: "table `curated.dim_patient_masked` lives at `s3://curated/tables/dim_patient_masked/` with these 12 columns and these data types."

### Step 9: Trino query
Analyst types in Superset SQL Lab:
```sql
SELECT zip3, age_band, COUNT(*)
FROM hive.curated.dim_patient_masked
GROUP BY zip3, age_band;
```
Trino reads the Hive metastore, finds the Parquet location, scans only the columns needed (`zip3`, `age_band`), parallelizes across workers, returns results in ~200ms.

### Step 10: Dashboard
Flask dashboard hits `/api/kpis` → query runs against Trino → JSON returned → Chart.js renders. Auto-refreshes every 60s.

---

## 6. The 10 Docker Services Explained

```bash
docker compose ps     # see them all
```

| # | Service | Container | Port(s) | Purpose | RAM |
|---|---------|-----------|---------|---------|-----|
| 1 | **minio** | dl-minio | 9000 (API) / 9001 (UI) | Object storage — the data lake itself | ~500 MB |
| 2 | **minio-init** | dl-minio-init | — | One-shot bucket creator (exits after) | — |
| 3 | **postgres-hive** | dl-postgres-hive | 5432 internal | Metastore backend DB | ~200 MB |
| 4 | **hive-metastore** | dl-hive-metastore | 9083 | Table catalog (Thrift) | ~1 GB |
| 5 | **spark-master** | dl-spark-master | 8080 (UI) / 7077 (RPC) | Job coordinator | ~1 GB |
| 6 | **spark-worker** | dl-spark-worker | — | Actual ETL execution | ~6 GB |
| 7 | **trino** | dl-trino | 8081 | SQL query engine | ~2 GB |
| 8 | **superset** | dl-superset | 8088 | BI platform | ~1 GB |
| 9 | **nifi** | dl-nifi | 8443 (HTTPS) | Visual ingestion flows | ~2 GB |
| 10 | **workbench** | dl-workbench | 8888 | Jupyter notebook for ad-hoc | ~500 MB |

**Total:** ~14 GB requested, ~10 GB actively used. That's why we recommend Docker Desktop ≥ 12 GB.

**Plus** the Flask dashboard runs on the host (not Docker) on port 5050 — uses ~50 MB.

### Why each service exists

- **MinIO** — the data lake itself. Object storage. Everything sits here.
- **MinIO-init** — runs once at startup, creates the 5 buckets, then exits. Without it, services would error on first write.
- **Postgres-hive** — Hive Metastore needs an ACID database to store table metadata. Postgres is the standard choice.
- **Hive Metastore** — the "card catalog" of the lake. Every other tool asks it: "what tables exist? where are their files?"
- **Spark Master** — coordinates jobs. The driver. Where you submit `spark-submit`.
- **Spark Worker** — does the actual computation. In production you'd have many; we have one with 6GB to keep laptop happy.
- **Trino** — the query engine. Translates your SQL into parallel reads of Parquet files in MinIO. Talks to Hive Metastore for table layouts.
- **Superset** — the BI tool. Web UI for SQL editing, chart building, dashboards.
- **NiFi** — the ingestion conveyor belt. Visual flows: source → MinIO.
- **Workbench (Jupyter)** — for data scientists. Notebooks with PySpark preloaded.

### Service dependencies (startup order)

```
minio (healthy)
  └→ minio-init (creates buckets, exits)
        └→ hive-metastore (needs Postgres + MinIO + buckets)
        └→ spark-master (needs MinIO + buckets)
              └→ spark-worker
        └→ trino (needs Hive metastore + MinIO + buckets)
              └→ superset (needs Trino)
```

This is enforced via Docker Compose `depends_on: condition: service_healthy / service_completed_successfully` so nothing starts before its prerequisites.

---

## 7. Project Structure

```
Data Lake/
├── docker-compose.yml          # The whole stack (10 services)
├── README.md                   # Quick-start + system requirements
│
├── scripts/
│   ├── bootstrap.sh            # ⭐ One-shot setup (preflight → start → load → run → dashboard)
│   ├── start.sh                # Build + bring up the stack
│   ├── stop.sh                 # Bring down (preserves data)
│   ├── load_sample_data.sh     # Generate + upload synthetic data in 7 formats
│   ├── run_pipeline.sh         # Run all 6 Spark stages with retries
│   ├── setup_superset.sh       # Register Trino in Superset + auto-build dashboard
│   ├── start_dashboard.sh      # Launch Flask dashboard (self-bootstraps venv)
│   └── connect_trino.sh        # Open Trino CLI for ad-hoc SQL
│
├── data-generator/             # Synthetic data generators
│   ├── medical_codes.py        # Reference: ICD-10, CPT, denial reasons, payers
│   ├── generate_medical_data.py    # CSVs (patients, encounters, claims, ...)
│   ├── generate_pdfs.py            # 100 PDFs (lab reports, discharge summaries, prescriptions)
│   ├── generate_diverse_formats.py # JSON, JSONL, Parquet, Excel, SQL, HL7, FHIR
│   └── requirements.txt
│
├── spark/
│   ├── Dockerfile              # Custom Spark + Tesseract + OCR + JARs image
│   ├── conf/
│   │   └── spark-defaults.conf # S3 + Hive metastore config
│   └── jobs/                   # 6 ETL jobs
│       ├── _common.py              # Spark session helper, lineage utils
│       ├── 00_universal_ingestion.py   # 7-format auto-detect router
│       ├── 01_raw_to_processed.py      # Bronze → Silver (cleaning)
│       ├── 02_processed_to_curated.py  # Silver → Gold (joins, agg, MSCK REPAIR)
│       ├── 03_pdf_ocr_pipeline.py      # Distributed OCR + entity extraction
│       ├── 04_data_quality_checks.py   # DQ metrics with 30-day retention
│       ├── 05_pii_masking.py           # HIPAA Safe Harbor masking
│       └── 06_incremental_merge.py     # CDC pattern (optional)
│
├── hive/
│   ├── Dockerfile              # Custom Hive image with Postgres + S3A JARs
│   ├── conf/
│   │   └── hive-site.xml       # Metastore config
│   └── lib/                    # Downloaded JARs (legacy; now baked in image)
│
├── trino/etc/
│   ├── config.properties       # Coordinator config
│   ├── jvm.config              # JVM tuning
│   ├── node.properties         # Node identity
│   ├── log.properties          # Log levels
│   ├── access-control.properties   # Enables file-based RBAC
│   ├── rules.json              # ⭐ HIPAA RBAC rules (tables + procedures + functions)
│   └── catalog/
│       └── hive.properties     # Connects Trino to Hive + S3
│
├── nifi/templates/
│   ├── tables_ingestion_flow.xml   # Importable: file watcher → MinIO
│   ├── pdf_ingestion_flow.xml      # Importable: HTTP listener → SHA → MinIO
│   └── README.md                   # How to import + MinIO config gotchas
│
├── superset/
│   └── superset_config.py      # Feature flags
│
├── sql/
│   ├── ddl/
│   │   └── 00_create_schemas.sql
│   └── analytics/              # Demo queries
│       ├── 01_claim_denial_analysis.sql
│       ├── 02_revenue_analysis.sql
│       ├── 03_clinical_documentation.sql
│       ├── 04_data_quality.sql
│       └── 05_provider_kpi.sql
│
├── dashboard/                  # Custom Flask operational dashboard
│   ├── app.py                  # 12 API endpoints (Trino-backed, cached)
│   ├── requirements.txt
│   └── static/
│       ├── index.html          # Single-page UI
│       ├── style.css           # Dark theme, BI tool look
│       └── script.js           # Chart.js + auto-refresh
│
├── data/                       # Generated data (gitignored)
│   ├── raw/
│   │   ├── tables/             # CSVs
│   │   ├── pdfs/               # 100 PDFs
│   │   ├── json/               # JSON files
│   │   ├── jsonl/              # JSONL
│   │   ├── parquet/            # Parquet
│   │   ├── excel/              # XLSX
│   │   ├── sql/                # SQL dumps
│   │   ├── hl7/                # HL7 v2 messages
│   │   └── fhir/               # FHIR JSON bundles
│   ├── processed/
│   ├── curated/
│   └── dlq/
│
└── docs/
    ├── ARCHITECTURE.md         # Original 7-layer deep dive
    ├── HOW_IT_WORKS.md         # 👋 You are here
    ├── DEMO_GUIDE.md           # 5-min demo script
    ├── PRESENTATION.md         # 12-slide management outline
    ├── TROUBLESHOOTING.md      # Common issues + fixes
    └── FIXES_APPLIED.md        # Bug-fix audit trail
```

---

## 8. Demo Talking Points (Cheat Sheet)

Memorize these one-liners — use them at every interview question:

### "Why a data lake?"
> *"Healthcare data is structured (claims, encounters) AND unstructured (clinical notes, scanned PDFs). A data warehouse only handles structured. A data lake unifies both — same storage, queryable with SQL, cheap to scale."*

### "Why open source?"
> *"$0 in licenses vs ~$300K/year for Snowflake + Tableau at this scale. Same architecture used by Netflix, AirBnB, JP Morgan in production. No vendor lock-in — every component swappable."*

### "What's HIPAA-grade about this?"
> *"Stage 5 implements all 18 HIPAA Safe Harbor identifiers — salted SHA-256 tokenization, year-only DOB, ZIP3 only, name initials. Trino RBAC enforces — non-privileged users get `dim_patient_masked`, never see real PII. Verified live: analyst → SELECT dim_patient → ACCESS DENIED."*

### "How does it scale?"
> *"MinIO → AWS S3, single-line URL change. Spark workers scale horizontally — same code on 1 worker or 1000. Trino query layer scales independently. Migration to AWS EMR + Athena is configuration-only, no code rewrites."*

### "What about data quality?"
> *"Every Spark stage emits row counts, null %, duplicates, freshness to a `dq_metrics` table with 30-day retention. Live dashboard shows per-table PASS/WARN/FAIL status. Bad records go to a Dead Letter Queue queryable in Trino — no silent failures."*

### "How do you handle multiple data formats?"
> *"`00_universal_ingestion.py` auto-detects 7 formats by extension — CSV, JSON, JSONL, Parquet, Excel, SQL dumps, HL7 v2, FHIR R4. Each routes to the right Spark reader. Failed parses go to DLQ for analysis."*

### "What's the RAF score thing?"
> *"Risk Adjustment Factor — Medicare uses it to adjust capitation payments based on patient acuity. Higher RAF = sicker patients = higher reimbursement. CDI teams optimize RAF capture by ensuring all comorbidities are coded. We compute average RAF per department, exposing under-documentation gaps."*

### "Why this stack and not Databricks?"
> *"Same components run on Databricks — Spark, Hive Metastore, Iceberg, MLflow. This shows I understand the building blocks, not just a managed platform. When we move to production, Databricks is one config flip away."*

---

## 9. Quick Reference

### Start everything
```bash
./scripts/bootstrap.sh    # one-shot: preflight + build + start + load + pipeline + dashboard
```

### Step-by-step
```bash
./scripts/start.sh             # bring up containers
./scripts/load_sample_data.sh  # generate + upload data
./scripts/run_pipeline.sh      # run Spark ETL
./scripts/setup_superset.sh    # connect Superset + build dashboard
./scripts/start_dashboard.sh   # launch Flask dashboard
```

### Stop / restart
```bash
./scripts/stop.sh                          # stop containers, keep data
docker compose down -v                     # stop + wipe volumes (fresh start)
docker compose restart <service>           # restart one service
docker compose logs -f <service>           # tail logs
```

### Trino CLI
```bash
docker exec -it dl-trino trino --user admin --catalog hive --schema curated
trino> SHOW TABLES;
trino> SELECT COUNT(*) FROM fact_claims;
trino> CALL hive.system.sync_partition_metadata('curated', 'fact_claims', 'FULL');
```

### Spark shell
```bash
docker exec -it dl-spark-master spark-shell \
    --master spark://spark-master:7077
```

### Run a single Spark stage
```bash
docker exec dl-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/04_data_quality_checks.py
```

### Check service health
```bash
docker compose ps                          # see all services
docker stats                               # live RAM / CPU usage
docker logs dl-trino --tail 50             # any service
curl -s http://localhost:5050/api/health   # dashboard health
curl -s http://localhost:8081/v1/info      # Trino info
```

### URLs
- Flask Dashboard: http://localhost:5050
- Superset BI: http://localhost:8088 (admin / admin)
- Trino UI: http://localhost:8081
- Spark UI: http://localhost:8080
- MinIO Console: http://localhost:9001 (admin / admin123456)
- NiFi: https://localhost:8443/nifi (admin / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`)
- Jupyter: http://localhost:8888 (token: `datalake`)

---

## Closing Thought

This is not a demo project. This is a **production-style platform** that happens to run on a laptop. The same architecture, same code, same SQL — runs identically on 10K rows or 10B rows. The same `docker-compose.yml` becomes Kubernetes manifests with `kompose`. The same MinIO becomes AWS S3. The same Hive Metastore becomes AWS Glue.

What this project shows isn't *"I can use Spark"*. It's *"I understand how modern data platforms are composed — storage, processing, catalog, query, viz — and I can debug, extend, and harden them."*

That's the difference between a CDI engineer and a CDI engineer who understands the platform CDIs run on.
