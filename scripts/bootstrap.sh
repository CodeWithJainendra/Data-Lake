#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# One-shot bootstrap for the Medical & Billing Data Lake.
#
# What it does (in order):
#   1. Validates host system (Docker, RAM, disk, port availability)
#   2. Detects architecture (arm64 / amd64) for sanity check
#   3. Builds custom Spark + Hive images (first run only — cached after)
#   4. Brings up the full stack (10 services)
#   5. Loads synthetic medical data (10K patients, 100 PDFs, 7 formats)
#   6. Runs the 6-stage ETL pipeline with retries
#   7. Starts the operational dashboard on :5050
#   8. Opens the dashboard in the default browser (macOS / Linux)
#
# Total runtime: ~10-15 min on first run, ~3 min on subsequent runs.
# ─────────────────────────────────────────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

bold()  { printf "\033[1m%s\033[0m\n" "$*"; }
ok()    { printf "  \033[32m✓\033[0m %s\n" "$*"; }
warn()  { printf "  \033[33m⚠\033[0m %s\n" "$*"; }
fail()  { printf "  \033[31m✗\033[0m %s\n" "$*"; }

bold "════════════════════════════════════════════════════════════"
bold "  Medical & Billing Data Lake — Bootstrap"
bold "════════════════════════════════════════════════════════════"

# ── 1. Preflight ────────────────────────────────────────────────────
bold "[1/8] Preflight checks"

# Docker available?
if ! command -v docker >/dev/null 2>&1; then
    fail "docker not found. Install Docker Desktop (https://docker.com/desktop)."
    exit 1
fi
ok "docker = $(docker --version | head -1)"

# Docker daemon running?
if ! docker info >/dev/null 2>&1; then
    fail "Docker daemon not running. Start Docker Desktop and retry."
    exit 1
fi
ok "docker daemon is responsive"

# Docker Compose v2?
if ! docker compose version >/dev/null 2>&1; then
    fail "docker compose plugin missing. Update to Docker Desktop 4.x+."
    exit 1
fi
ok "docker compose = $(docker compose version --short)"

# Architecture
ARCH=$(uname -m)
case "$ARCH" in
    arm64|aarch64) ok "architecture = arm64 (Apple Silicon / ARM)" ;;
    x86_64|amd64)  ok "architecture = amd64 (Intel / AMD)" ;;
    *)             warn "unknown architecture: $ARCH — images may not run" ;;
esac

# RAM check (best effort — Docker Desktop's allocation, not host)
DOCKER_RAM=$(docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%.0f", $1/1024/1024/1024}')
if [ -n "$DOCKER_RAM" ] && [ "$DOCKER_RAM" -gt 0 ]; then
    if [ "$DOCKER_RAM" -lt 10 ]; then
        warn "Docker has only ${DOCKER_RAM} GB RAM allocated. Recommended: ≥ 12 GB."
        warn "Open Docker Desktop → Settings → Resources to increase."
    else
        ok "Docker RAM = ${DOCKER_RAM} GB"
    fi
fi

# Free disk
FREE_GB=$(df -k . | awk 'NR==2 {printf "%.0f", $4/1024/1024}')
if [ "$FREE_GB" -lt 20 ]; then
    warn "Only ${FREE_GB} GB free disk. Recommended: ≥ 20 GB."
else
    ok "free disk = ${FREE_GB} GB"
fi

# Port check
check_port() {
    local p=$1 name=$2
    if lsof -ti:"$p" >/dev/null 2>&1; then
        warn "port $p ($name) is occupied — service may fail to start"
    fi
}
for p in 9000:MinIO 9001:MinIO-Console 9083:Hive 8080:Spark 8081:Trino 8088:Superset 8443:NiFi 8888:Jupyter 5050:Dashboard 7077:Spark-RPC; do
    check_port "${p%%:*}" "${p##*:}"
done

# ── 2. Build images ─────────────────────────────────────────────────
bold ""
bold "[2/8] Build custom images (Spark+OCR, Hive)  ⏱ ~5 min first run"
docker compose build --quiet spark-master spark-worker hive-metastore
ok "images built"

# ── 3. Start stack ──────────────────────────────────────────────────
bold ""
bold "[3/8] Bring up the stack (10 services)"
docker compose up -d
ok "containers started"

# Wait for health
bold ""
bold "[4/8] Waiting for Trino to become healthy (~60s)"
for i in $(seq 1 30); do
    if curl -s -o /dev/null -w "%{http_code}" --max-time 2 http://localhost:8081/v1/info | grep -q 200; then
        ok "Trino healthy after ${i}×2s"
        break
    fi
    sleep 2
    printf "."
done
echo ""

# ── 5. Load data ────────────────────────────────────────────────────
bold ""
bold "[5/8] Generate + upload synthetic medical data (~3 min)"
"$PROJECT_DIR/scripts/load_sample_data.sh"

# ── 6. Run pipeline ─────────────────────────────────────────────────
bold ""
bold "[6/8] Run ETL pipeline — stages 0-5 (~5 min)"
"$PROJECT_DIR/scripts/run_pipeline.sh"

# ── 7. Dashboard ────────────────────────────────────────────────────
bold ""
bold "[7/8] Start operational dashboard"
nohup "$PROJECT_DIR/scripts/start_dashboard.sh" > /tmp/dashboard.log 2>&1 &
DASH_PID=$!
sleep 4
if curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://localhost:5050/ | grep -q 200; then
    ok "dashboard running on http://localhost:5050 (pid $DASH_PID)"
else
    warn "dashboard may still be starting (logs: /tmp/dashboard.log)"
fi

# ── 8. Open browser ─────────────────────────────────────────────────
bold ""
bold "[8/8] Open dashboard"
case "$(uname)" in
    Darwin)            open "http://localhost:5050" ;;
    Linux)             xdg-open "http://localhost:5050" 2>/dev/null || true ;;
    MINGW*|CYGWIN*)    start "http://localhost:5050" ;;
esac

bold ""
bold "════════════════════════════════════════════════════════════"
bold "  ✓ BOOTSTRAP COMPLETE"
bold "════════════════════════════════════════════════════════════"
echo "  Dashboard         → http://localhost:5050"
echo "  MinIO Console     → http://localhost:9001  (admin / admin123456)"
echo "  Spark Master UI   → http://localhost:8080"
echo "  Trino Web UI      → http://localhost:8081"
echo "  Superset          → http://localhost:8088  (admin / admin)"
echo "  NiFi              → https://localhost:8443/nifi"
echo "  Jupyter           → http://localhost:8888  (token: datalake)"
echo ""
echo "  To stop:          ./scripts/stop.sh"
echo "  Logs:             docker compose logs -f <service>"
