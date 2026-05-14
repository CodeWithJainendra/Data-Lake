# Medical & Billing Data Lake

A production-style, end-to-end Data Lake built on open-source tooling, designed for healthcare claims, billing, and clinical document processing.

## What This Is

A complete 7-layer data lake architecture running locally on Docker, ingesting both **structured medical/billing data** (300+ tables of patients, claims, encounters, providers) and **unstructured clinical PDFs** (lab reports, discharge summaries, prescriptions), then cleansing, cataloging, querying, and visualizing them — all from a single `docker compose up`.

Built specifically with the Clinical Documentation Improvement (CDI) and Revenue Cycle Management (RCM) workflow in mind.

## Architecture

```
┌─────────────┐   ┌──────────┐   ┌──────────────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────────┐
│ 1. SOURCES  │──▶│ 2. NIFI  │──▶│ 3. MINIO         │──▶│ 4. SPARK │──▶│ 5. HIVE  │──▶│ 6. TRINO │──▶│ 7. SUPERSET │
│  CSV + PDF  │   │ Ingest   │   │ raw/proc/curated │   │ ETL+OCR  │   │ Catalog  │   │  SQL     │   │  Dashboards │
└─────────────┘   └──────────┘   └──────────────────┘   └──────────┘   └──────────┘   └──────────┘   └─────────────┘
```

| Layer | Tech | Purpose |
|-------|------|---------|
| Data Sources | Python (Faker + medical lexicons) | Synthetic patient, encounter, claim, diagnosis, payment data + sample medical PDFs |
| Ingestion | Apache NiFi (or Python fallback) | Move CSVs into `raw/tables/`, PDFs into `raw/pdfs/` |
| Storage | MinIO (S3-compatible) | Three zones: raw (immutable) → processed (cleaned) → curated (analytics-ready) |
| Processing | Apache Spark (PySpark) | Cleansing, deduplication, ETL, joins, business rules, PDF OCR |
| Catalog | Apache Hive Metastore (Postgres-backed) | Schemas, partitions, lineage |
| Query | Trino | SQL-on-data-lake (sub-second over Parquet) |
| Visualization | Apache Superset | Dashboards: claim denials, revenue, denial reasons, document volume |

## Why This Is Production-Style (not a toy)

- **HIPAA-grade PII masking** layer in Spark — SSN, MRN, phone, address tokenized
- **Data Quality framework** — null/duplicate/outlier checks per zone, written to a `dq_metrics` table that powers a Superset health dashboard
- **Schema evolution** support via Parquet + Hive
- **Medallion architecture** (raw → bronze/processed → silver/curated → gold/analytics)
- **ICD-10 + CPT code enrichment** — diagnoses joined to reference tables for severity, HCC, RAF score signals
- **Incremental loads** (CDC-style) via merge keys, not full reloads
- **Lineage** — every curated row traces back to a raw file via `_source_file` + `_ingested_at`

## System Requirements

The full stack runs 10 services and processes 200K+ rows. You need:

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| **RAM** (allocated to Docker) | 10 GB | **12 GB** |
| **Free disk** | 15 GB | **20 GB** |
| **CPU cores** | 4 | 6+ |
| **Docker Desktop** | 4.x with Compose v2 | latest |
| **Architecture** | arm64 (Apple Silicon) or amd64 (Intel/AMD) — both work |
| **Network (first run only)** | yes (to pull images + Maven JARs) | — |

> **Increase Docker Desktop memory:** Settings → Resources → Memory ≥ 12 GB.
> With 8 GB you'll see Spark worker OOMs and slow PDF OCR.

## Quick Start

**One-shot bootstrap** (recommended) — validates resources, builds images, loads data, runs pipeline, opens dashboard:

```bash
./scripts/bootstrap.sh
```

**Manual** (if you want step-by-step control):

```bash
./scripts/start.sh             # builds custom images, brings up 10 services
./scripts/load_sample_data.sh  # generates synthetic data in 7 formats
./scripts/run_pipeline.sh      # runs the 6-stage Spark ETL
./scripts/start_dashboard.sh   # launches the operational dashboard on :5050
```

**Access the UIs:**

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dashboard** | http://localhost:5050 | — |
| MinIO Console | http://localhost:9001 | admin / admin123456 |
| Spark Master UI | http://localhost:8080 | — |
| Trino UI | http://localhost:8081 | (use admin user) |
| Superset | http://localhost:8088 | admin / admin |
| NiFi | https://localhost:8443/nifi | admin / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB` |
| Jupyter | http://localhost:8888 | token: `datalake` |

## Portability

Everything is portable across systems — `git clone` + `./scripts/bootstrap.sh` works on any host (macOS Intel/Silicon, Linux, Windows WSL2) running Docker Desktop 4.x or Docker Engine 24.x. No `/Users/...` paths are hardcoded. All required JARs are baked into the custom Spark + Hive Dockerfiles, so no external Maven downloads happen after the first image build (offline-friendly).

The Docker network name is fixed (`medical_datalake`), so scripts work regardless of the parent folder name.

## Project Structure

```
.
├── docker-compose.yml          # The full stack
├── scripts/                    # start/stop/load/pipeline
├── data-generator/             # Synthetic medical data
├── spark/jobs/                 # PySpark ETL jobs
├── trino/etc/                  # Trino + Hive catalog config
├── hive/conf/                  # Hive metastore config
├── superset/                   # Dashboard JSON exports
├── sql/                        # DDL + analytics queries
└── docs/                       # Architecture, demo guide
```

## Demo Flow (5-minute presentation)

1. **Show MinIO** — three buckets, raw zone with CSVs + PDFs visible
2. **Run a Spark job live** — `spark-submit 01_raw_to_processed.py`, show the data move
3. **Show OCR pipeline** — pick a scanned PDF, show extracted text landing in curated zone
4. **Open Trino UI** — run `SELECT * FROM curated.fact_claims LIMIT 10;` — show SQL-on-files
5. **Open Superset** — Claim Denial Dashboard with 4 charts
6. **Show Data Quality panel** — live metrics from `dq_metrics` table
7. **Talk through the design** — why each component was chosen, how it would scale to billions of rows

## Author

Built by Shivam — CDIS Engineer
