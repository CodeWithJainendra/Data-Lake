# Troubleshooting Guide

Common issues when running the stack on Apple Silicon (M1/M2/M3/M4) and their fixes.

---

## "Cannot connect to the Docker daemon"

Make sure Docker Desktop is running. On M-series Macs:
```
open -a Docker
```
Wait until the whale icon in the menu bar is steady (not animated).

## "Port already in use" (8080, 9000, etc.)

Find and kill the offender:
```bash
lsof -i :8080         # find PID
kill -9 <PID>
```

Or change the port in `docker-compose.yml` (left side of `"8080:8080"`).

## Hive Metastore container crashes / "JdbcStoreManager" errors

The Postgres JDBC driver wasn't downloaded. Re-run:
```bash
./scripts/start.sh
```

It downloads `hive/lib/postgresql-42.7.3.jar` and mounts it.

## Spark "ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem"

The S3A jars weren't downloaded. The `spark-submit` command in `run_pipeline.sh` passes them via `--packages`. The first run takes 2-3 minutes as Spark downloads them. Subsequent runs are cached.

## Trino "Schema 'curated' not found"

The Spark jobs haven't registered the tables yet. Run:
```bash
./scripts/run_pipeline.sh
```

Or manually:
```bash
docker exec dl-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/02_processed_to_curated.py
```

## Superset blank page / login fails

Wait 60 seconds after first start — Superset is initializing its DB. Then:
```bash
docker logs dl-superset --tail 50
```

If still broken:
```bash
docker exec -it dl-superset superset fab create-admin \
    --username admin --firstname A --lastname B --email a@b.com --password admin
```

## NiFi "this site can't be reached"

NiFi uses **HTTPS** on port 8443, not HTTP. Use `https://localhost:8443/nifi`. Accept the self-signed cert warning.

## "platform mismatch" / "exec format error" on M-series

A specific image doesn't have an arm64 build. Add to that service in `docker-compose.yml`:
```yaml
platform: linux/amd64
```
This runs under Rosetta 2 emulation. Slower but works.

## Out of memory / containers crashing

Open Docker Desktop → Settings → Resources, and bump:
- CPU: 6 cores
- Memory: 12 GB
- Swap: 2 GB

Then `docker compose down && ./scripts/start.sh`.

## PDF OCR step fails: "tesseract not found"

The PDF OCR pipeline uses Tesseract. It's installed by `run_pipeline.sh` automatically, but if you skipped that:
```bash
docker exec dl-spark-master apt-get update && apt-get install -y tesseract-ocr poppler-utils
docker exec dl-spark-worker apt-get update && apt-get install -y tesseract-ocr poppler-utils
```

## Wiping everything and starting fresh

```bash
docker compose down -v          # remove containers + volumes
rm -rf data/                    # remove generated data
./scripts/start.sh              # bring it back up
./scripts/load_sample_data.sh
./scripts/run_pipeline.sh
```

---

## When all else fails

```bash
docker compose logs <service-name> --tail 100
```

The error is almost always in the logs of the first container that crashed.
