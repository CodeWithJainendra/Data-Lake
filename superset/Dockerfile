# ============================================================================
# Custom Superset image with Trino SQLAlchemy dialect pre-installed.
# Base: apache/superset:latest
#
# Why custom?
#   • Superset runs out of /app/.venv (its own venv has no pip executable).
#   • Runtime `pip install ...` at container start installs into the wrong
#     site-packages (system Python) — Superset can't see it.
#   • Permission denied if we try `pip install --target=/app/.venv/...` at
#     runtime as the non-root container user.
#
# Solution: install at build time (as root) directly into the venv site-pkgs,
# with --no-deps so Superset's pinned SQLAlchemy 1.4 is preserved (newer
# SQLAlchemy 2.x breaks Superset — removes `eagerload`).
# ============================================================================
FROM apache/superset:latest

USER root

RUN pip install --target=/app/.venv/lib/python3.10/site-packages --no-deps \
        trino==0.337.0 \
        sqlalchemy-trino==0.5.0 \
        tzlocal==5.3.1

USER superset
