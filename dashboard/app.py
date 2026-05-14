"""
Medical & Billing Data Lake — Operational Dashboard
====================================================
Flask backend that proxies analytical queries to Trino and serves a
single-page interactive dashboard. Endpoints are deliberately small and
focused (one chart per endpoint) so the frontend can render any subset
without waiting on the slowest query.

Endpoints:
  GET /                       → static dashboard
  GET /api/health             → liveness + Trino connectivity check
  GET /api/kpis               → 8 hero KPIs
  GET /api/monthly-revenue    → billed/paid/denied by month
  GET /api/top-denials        → top 10 denial reasons + dollars at risk
  GET /api/department-revenue → billing by department
  GET /api/payer-mix          → revenue share per payer
  GET /api/ar-aging           → accounts receivable aging buckets
  GET /api/raf-by-department  → average RAF + severe-dx capture
  GET /api/document-types     → OCR doc type breakdown
  GET /api/top-providers      → 10 providers by total charges
  GET /api/dq-status          → latest data-quality run per table
  GET /api/dlq-summary        → DLQ entries (ingestion + PDFs)
"""
from datetime import datetime, timedelta
from functools import wraps
from threading import Lock

from flask import Flask, jsonify, send_from_directory
from trino.dbapi import connect
from trino.exceptions import TrinoUserError

app = Flask(__name__, static_folder="static", static_url_path="")

# ── Trino connection ────────────────────────────────────────────────
TRINO_HOST     = "localhost"
TRINO_PORT     = 8081
TRINO_USER     = "admin"          # admin role — passes RBAC for dim_patient
TRINO_CATALOG  = "hive"
TRINO_SCHEMA   = "curated"

# ── Simple in-process response cache (30 s TTL per endpoint) ────────
_cache = {}
_cache_lock = Lock()
CACHE_TTL = timedelta(seconds=30)


def cached(name):
    """Cache an endpoint's JSON response for CACHE_TTL seconds."""
    def deco(fn):
        @wraps(fn)
        def wrap(*a, **kw):
            now = datetime.utcnow()
            with _cache_lock:
                hit = _cache.get(name)
                if hit and now - hit[0] < CACHE_TTL:
                    return hit[1]
            result = fn(*a, **kw)
            with _cache_lock:
                _cache[name] = (now, result)
            return result
        return wrap
    return deco


def query(sql):
    """Run SQL against Trino and return a list of dicts."""
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
                   catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def query_safe(sql, default=None):
    """Run a query; return default on failure (e.g. table doesn't exist)."""
    try:
        return query(sql)
    except (TrinoUserError, Exception) as e:
        print(f"[query_safe] {e}")
        return default if default is not None else []


# ══════════════════════════════════════════════════════════════════════
#  STATIC
# ══════════════════════════════════════════════════════════════════════
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ══════════════════════════════════════════════════════════════════════
#  HEALTH
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/health")
def health():
    try:
        v = query("SELECT 1 AS ok")[0]["ok"]
        return jsonify({"status": "ok", "trino_reachable": v == 1})
    except Exception as e:
        return jsonify({"status": "degraded", "error": str(e)}), 503


# ══════════════════════════════════════════════════════════════════════
#  KPIs — 8 hero numbers
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/kpis")
@cached("kpis")
def kpis():
    r = query("""
        SELECT
          (SELECT COUNT(*) FROM fact_claims)                                       AS total_claims,
          (SELECT SUM(billed_amount) FROM fact_claims)                             AS total_billed,
          (SELECT SUM(paid_amount) FROM fact_claims)                               AS total_paid,
          (SELECT 100.0 * COUNT(*) FILTER (WHERE denial_reason_code IS NOT NULL)
                  / NULLIF(COUNT(*), 0) FROM fact_claims)                           AS denial_rate,
          (SELECT AVG(days_to_adjudicate) FROM fact_claims
            WHERE days_to_adjudicate IS NOT NULL)                                   AS avg_days_to_pay,
          (SELECT COUNT(*) FROM dim_patient)                                       AS total_patients,
          (SELECT COUNT(*) FROM fact_encounters)                                   AS total_encounters,
          (SELECT COUNT(*) FROM dim_clinical_documents)                            AS total_documents
    """)[0]
    # Derived KPI: collection rate
    if r["total_billed"]:
        r["collection_rate"] = float(r["total_paid"] or 0) / float(r["total_billed"]) * 100
    else:
        r["collection_rate"] = 0
    return jsonify(r)


# ══════════════════════════════════════════════════════════════════════
#  REVENUE CYCLE
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/monthly-revenue")
@cached("monthly_revenue")
def monthly_revenue():
    return jsonify(query("""
        SELECT submitted_year AS y, submitted_month AS m,
               SUM(total_billed)  AS billed,
               SUM(total_paid)    AS paid,
               SUM(denied_count)  AS denied,
               SUM(claim_count)   AS claims
        FROM agg_monthly_revenue
        GROUP BY submitted_year, submitted_month
        ORDER BY submitted_year, submitted_month
    """))


@app.route("/api/department-revenue")
@cached("dept_revenue")
def department_revenue():
    return jsonify(query("""
        SELECT department,
               SUM(total_billed) AS billed,
               SUM(total_paid)   AS paid,
               SUM(claim_count)  AS claims
        FROM agg_monthly_revenue
        WHERE department IS NOT NULL
        GROUP BY department
        ORDER BY billed DESC
        LIMIT 10
    """))


@app.route("/api/payer-mix")
@cached("payer_mix")
def payer_mix():
    return jsonify(query("""
        SELECT payer_name, payer_type,
               SUM(total_paid) AS paid,
               SUM(claim_count) AS claims
        FROM agg_monthly_revenue
        WHERE payer_name IS NOT NULL
        GROUP BY payer_name, payer_type
        ORDER BY paid DESC
    """))


@app.route("/api/ar-aging")
@cached("ar_aging")
def ar_aging():
    return jsonify(query("""
        SELECT
          CASE
            WHEN days_to_adjudicate IS NULL   THEN 'Pending'
            WHEN days_to_adjudicate <= 15     THEN '0–15 days'
            WHEN days_to_adjudicate <= 30     THEN '16–30 days'
            WHEN days_to_adjudicate <= 60     THEN '31–60 days'
            WHEN days_to_adjudicate <= 90     THEN '61–90 days'
            ELSE                                   '90+ days'
          END                                                    AS bucket,
          COUNT(*)                                               AS claim_count,
          SUM(billed_amount)                                     AS billed
        FROM fact_claims
        GROUP BY 1
    """))


# ══════════════════════════════════════════════════════════════════════
#  DENIALS
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/top-denials")
@cached("top_denials")
def top_denials():
    return jsonify(query("""
        SELECT denial_reason_code AS code,
               SUM(denial_count)  AS denials,
               SUM(billed_at_risk) AS at_risk
        FROM agg_denial_summary
        WHERE denial_reason_code IS NOT NULL
        GROUP BY denial_reason_code
        ORDER BY at_risk DESC
        LIMIT 10
    """))


# ══════════════════════════════════════════════════════════════════════
#  CLINICAL / CDI
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/raf-by-department")
@cached("raf_dept")
def raf_by_department():
    return jsonify(query("""
        SELECT department,
               AVG(avg_raf_score) AS avg_raf,
               SUM(severe_dx_encounters) AS severe_dx,
               SUM(encounter_count) AS encounters
        FROM agg_provider_kpi
        WHERE department IS NOT NULL
        GROUP BY department
        ORDER BY avg_raf DESC
    """))


@app.route("/api/document-types")
@cached("doc_types")
def document_types():
    return jsonify(query("""
        SELECT document_type AS type, COUNT(*) AS n
        FROM dim_clinical_documents
        GROUP BY document_type
        ORDER BY n DESC
    """))


# ══════════════════════════════════════════════════════════════════════
#  PROVIDERS
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/top-providers")
@cached("top_providers")
def top_providers():
    return jsonify(query("""
        SELECT provider_id,
               department,
               encounter_count        AS encounters,
               total_charges          AS charges,
               avg_raf_score          AS raf,
               severe_dx_encounters   AS severe_dx
        FROM agg_provider_kpi
        ORDER BY total_charges DESC
        LIMIT 10
    """))


# ══════════════════════════════════════════════════════════════════════
#  DATA HEALTH
# ══════════════════════════════════════════════════════════════════════
@app.route("/api/dq-status")
@cached("dq_status")
def dq_status():
    return jsonify(query("""
        WITH latest AS (
          SELECT zone, table_name, MAX(run_timestamp) AS run_ts
          FROM dq_metrics
          GROUP BY zone, table_name
        )
        SELECT dq.zone, dq.table_name, dq.row_count, dq.duplicate_pct,
               dq.worst_null_column, dq.worst_null_pct, dq.status,
               dq.run_timestamp
        FROM dq_metrics dq
        JOIN latest l ON dq.zone = l.zone
                      AND dq.table_name = l.table_name
                      AND dq.run_timestamp = l.run_ts
        ORDER BY dq.zone, dq.table_name
    """))


@app.route("/api/dlq-summary")
@cached("dlq")
def dlq_summary():
    """DLQ entries — both PDF parse failures and ingestion failures.
    Tables may not exist if there are zero DLQ entries (idempotency design).
    """
    pdf_rows = query_safe("""
        SELECT 'pdf_parse' AS source, source_key AS item,
               document_type, processed_at AS failed_at
        FROM dlq.pdfs
        LIMIT 50
    """, default=[])
    ingest_rows = query_safe("""
        SELECT 'ingestion' AS source, key AS item,
               format AS document_type, failed_at
        FROM dlq.ingestion
        LIMIT 50
    """, default=[])
    return jsonify(pdf_rows + ingest_rows)


# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  Medical & Billing Data Lake — Dashboard")
    print(f"  http://localhost:5050   (Trino: {TRINO_HOST}:{TRINO_PORT})")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)
