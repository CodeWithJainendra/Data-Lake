"""
Superset configuration — adds Trino DB driver and sane defaults.
"""
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789")

# Connection to embedded SQLite for Superset metadata (local dev only)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Disable CSRF for easy dev
WTF_CSRF_ENABLED = False

# Enable feature flags
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_RBAC": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERTS_ATTACH_REPORTS": True,
    "EMBEDDED_SUPERSET": True,
}

# Allowed DB connections
PREVENT_UNSAFE_DB_CONNECTIONS = False
