# NiFi Templates

This directory contains **two importable NiFi flow templates** (XML). They are
auto-mounted into NiFi at `/opt/nifi/nifi-current/templates/`. NiFi state is
persisted across restarts via dedicated Docker volumes so once you import and
start them, they stay.

## What's Included

| File | Purpose | Trigger |
|------|---------|---------|
| `tables_ingestion_flow.xml` | Pick up CSV/JSON/Parquet/Excel/SQL/HL7 files from `/opt/data/incoming/tables/` and push to MinIO `raw/<format>/` | Filesystem watch every 10 sec |
| `pdf_ingestion_flow.xml`    | Accept PDF uploads via HTTP, dedupe by SHA-256, push to MinIO `raw/pdfs/` | HTTP POST to `:8090/pdf` |

## How to Import

1. Open NiFi: `https://localhost:8443/nifi` (admin / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`)
2. From the top toolbar, click the **Upload Template** icon (looks like an upward-arrow folder)
3. Select `tables_ingestion_flow.xml` from `/opt/nifi/nifi-current/templates/`
4. Drag the **Template** icon (purple paper-stack) onto the canvas
5. Pick `tables_ingestion_flow` from the dropdown → click **Add**
6. Right-click each processor → **Start**
7. Repeat for `pdf_ingestion_flow.xml`

## Critical MinIO Configuration on PutS3Object

These are pre-configured in the templates — listed here as reference:

| Property | Value | Why |
|----------|-------|-----|
| Bucket | `raw` | Where files land |
| Endpoint Override URL | `http://minio:9000` | MinIO container DNS |
| Region | `us-east-1` | MinIO ignores it but AWS SDK requires one |
| Access Key | `admin` | Match `MINIO_ROOT_USER` |
| Secret Key | `admin123456` | Match `MINIO_ROOT_PASSWORD` |
| Use Path Style Access | `true` | **Mandatory for MinIO** — without this you get `SignatureDoesNotMatch` |
| Signer Override | `AWSS3V4SignerType` | MinIO uses v4 signing |

Without **path-style access** + **endpoint override**, every PutS3Object request fails with HTTP 403. This is the #1 NiFi+MinIO integration mistake.

## Testing the PDF Flow

Once the `pdf_ingestion_flow` is started, drop any PDF in like this:

```bash
curl -k -X POST -F "file=@/path/to/lab_report.pdf" https://localhost:8443/pdf
```

Within seconds the file will appear at MinIO console under `raw/pdfs/<sha256>.pdf`.

## Why NiFi vs Plain Scripts?

In production you want:

- **Visual lineage** — every flowfile is traceable through the UI (regulatory audit-ready)
- **Backpressure** — if MinIO slows, NiFi automatically pauses upstream (no data loss)
- **Provenance** — every file's journey is logged for HIPAA compliance audits
- **Replay** — re-fire a failed flowfile without re-running Spark
- **No code deploys** — hospital ops team can re-wire flows without engineering involvement

## Persistence

NiFi's flow state is stored in the `nifi_data`, `nifi_conf`, `nifi_state`,
`nifi_flowfile`, `nifi_content`, and `nifi_provenance` Docker volumes
(see `docker-compose.yml`). `docker compose down` preserves them; only
`docker compose down -v` wipes them.
