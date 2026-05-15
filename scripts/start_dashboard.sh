#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Start the operational dashboard (Flask + Trino client).
# Self-bootstrapping: creates venv on first run, installs deps, serves UI.
# Idempotent — re-running kills any existing dashboard on :5050 and relaunches.
# ─────────────────────────────────────────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DASH_DIR="$PROJECT_DIR/dashboard"

cd "$DASH_DIR"

# 1. Verify python3
if ! command -v python3 >/dev/null 2>&1; then
    echo "✗ python3 not found in PATH. Install Python 3.10+ and retry."
    exit 1
fi
PYV=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "✓ python3 = $PYV"

# 2. Create venv if missing or broken (first run on this machine)
if [ ! -f .venv/bin/activate ] || [ ! -x .venv/bin/python ] || ! .venv/bin/python -c 'import sys' >/dev/null 2>&1; then
    echo "Creating .venv (one-time) ..."
    rm -rf .venv
    python3 -m venv .venv || {
        echo "✗ Failed to create venv. Try: sudo apt install python3-venv python3-full"
        exit 1
    }
fi

# 3. Install deps using the venv interpreter directly
VENV_PY="$DASH_DIR/.venv/bin/python"
"$VENV_PY" -m pip install -q --upgrade pip
"$VENV_PY" -m pip install -q -r requirements.txt
echo "✓ dependencies installed in $DASH_DIR/.venv"

# 4. Check Trino reachability
if curl -s -o /dev/null -w "%{http_code}" --max-time 3 http://localhost:8081/v1/info | grep -q 200; then
    echo "✓ Trino is reachable at http://localhost:8081"
else
    echo "⚠ Trino at localhost:8081 not responding — dashboard will load with errors."
    echo "  Did you run ./scripts/start.sh ?"
fi

# 5. Kill anything currently on port 5050 (idempotent restart)
if lsof -ti:5050 >/dev/null 2>&1; then
    echo "Stopping previous dashboard on :5050 ..."
    lsof -ti:5050 | xargs kill 2>/dev/null || true
    sleep 1
fi

# 6. Launch
echo ""
echo "============================================================"
echo "  Dashboard starting → http://localhost:5050"
echo "  Stop with Ctrl-C"
echo "============================================================"
exec "$VENV_PY" app.py
