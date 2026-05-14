#!/usr/bin/env bash
set -e
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"
echo "Stopping Data Lake stack..."
docker compose down
echo "Done. Volumes preserved. Run 'docker compose down -v' to wipe all data."
