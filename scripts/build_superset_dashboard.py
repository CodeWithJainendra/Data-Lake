#!/usr/bin/env python3
"""
Build a complete Superset dashboard via the API.
Registers curated tables as datasets, creates 6 charts, and assembles them
into a single "Medical Data Lake — Operations" dashboard.

Idempotent: re-running deletes the dashboard + its charts/datasets first.
"""
import json
import sys
import time
import urllib.request
import urllib.parse

BASE = "http://localhost:8088"
USER = "admin"
PASS = "admin"
DB_NAME = "Trino (Data Lake)"
DASHBOARD_TITLE = "Medical Data Lake — Operations"


# ── HTTP helper ────────────────────────────────────────────────────────
class Client:
    def __init__(self):
        self.token = None
        self.csrf = None
        self.cookies = []

    def login(self):
        body = json.dumps({"username": USER, "password": PASS,
                           "provider": "db", "refresh": True}).encode()
        req = urllib.request.Request(f"{BASE}/api/v1/security/login",
                                     data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            self.token = json.loads(r.read())["access_token"]
            self.cookies = r.headers.get_all("Set-Cookie") or []

        # CSRF
        req = urllib.request.Request(f"{BASE}/api/v1/security/csrf_token/",
                                     headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req) as r:
            data = json.loads(r.read())
            self.csrf = data["result"]
            new_cookies = r.headers.get_all("Set-Cookie") or []
            self.cookies = self.cookies + new_cookies

    def _cookie_header(self):
        # Take the value before first ';' from each Set-Cookie
        items = []
        for raw in self.cookies:
            items.append(raw.split(";", 1)[0])
        return "; ".join(items)

    def request(self, method, path, body=None, params=None):
        url = f"{BASE}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Cookie": self._cookie_header(),
        }
        if method != "GET" and self.csrf:
            headers["X-CSRFToken"] = self.csrf
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            print(f"  ✗ {method} {path}: {e.code} {e.read().decode()[:200]}")
            raise


c = Client()


# ── 1. Login + find Trino database id ─────────────────────────────────
def find_db_id():
    rs = c.request("GET", "/api/v1/database/")
    for r in rs.get("result", []):
        if r["database_name"] == DB_NAME:
            return r["id"]
    print(f"✗ Database '{DB_NAME}' not registered. Run setup_superset.sh first.")
    sys.exit(1)


# ── 2. Register tables as datasets (idempotent: skip if exists) ────────
TABLES = [
    "fact_claims", "fact_encounters", "dim_patient_masked",
    "dim_clinical_documents", "agg_monthly_revenue",
    "agg_denial_summary", "agg_provider_kpi", "dq_metrics",
]


def existing_datasets():
    rs = c.request("GET", "/api/v1/dataset/")
    return {(r["schema"], r["table_name"]): r["id"] for r in rs["result"]}


def ensure_datasets(db_id):
    ex = existing_datasets()
    ds_ids = {}
    for t in TABLES:
        key = ("curated", t)
        if key in ex:
            ds_ids[t] = ex[key]
            print(f"  • dataset exists: {t} (id={ex[key]})")
            continue
        try:
            r = c.request("POST", "/api/v1/dataset/",
                          body={"database": db_id, "schema": "curated", "table_name": t})
            ds_ids[t] = r["id"]
            print(f"  ✓ created dataset: {t} (id={r['id']})")
        except Exception as e:
            print(f"  ✗ {t}: {e}")
    return ds_ids


# ── 3. Delete prior dashboard + its charts (clean slate per run) ───────
def cleanup_prior():
    rs = c.request("GET", "/api/v1/dashboard/")
    matching = [d for d in rs.get("result", []) if d["dashboard_title"] == DASHBOARD_TITLE]
    for d in matching:
        # Get charts in this dashboard
        try:
            detail = c.request("GET", f"/api/v1/dashboard/{d['id']}/charts")
            chart_ids = [item["id"] for item in detail.get("result", [])]
        except Exception:
            chart_ids = []
        c.request("DELETE", f"/api/v1/dashboard/{d['id']}")
        print(f"  ✓ deleted prior dashboard {d['id']}")
        for cid in chart_ids:
            try:
                c.request("DELETE", f"/api/v1/chart/{cid}")
                print(f"    ✓ deleted chart {cid}")
            except Exception:
                pass


# ── 4. Create charts ───────────────────────────────────────────────────
def create_chart(name, datasource_id, viz_type, params):
    body = {
        "slice_name": name,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    r = c.request("POST", "/api/v1/chart/", body=body)
    print(f"  ✓ chart: {name} (id={r['id']})")
    return r["id"]


def build_charts(ds):
    ids = {}

    # 1. Total billed (Big Number)
    ids["billed"] = create_chart(
        "Total Billed", ds["fact_claims"], "big_number_total",
        {
            "datasource": f"{ds['fact_claims']}__table",
            "viz_type": "big_number_total",
            "metric": {"aggregate": "SUM", "column": {"column_name": "billed_amount"},
                       "expressionType": "SIMPLE", "label": "Total Billed"},
            "adhoc_filters": [],
            "y_axis_format": "$,.0f",
        })

    # 2. Total paid
    ids["paid"] = create_chart(
        "Total Paid", ds["fact_claims"], "big_number_total",
        {
            "datasource": f"{ds['fact_claims']}__table",
            "viz_type": "big_number_total",
            "metric": {"aggregate": "SUM", "column": {"column_name": "paid_amount"},
                       "expressionType": "SIMPLE", "label": "Total Paid"},
            "adhoc_filters": [],
            "y_axis_format": "$,.0f",
        })

    # 3. Denial rate (calculated metric via SQL expression)
    ids["denial_rate"] = create_chart(
        "Denial Rate %", ds["fact_claims"], "big_number_total",
        {
            "datasource": f"{ds['fact_claims']}__table",
            "viz_type": "big_number_total",
            "metric": {"label": "Denial Rate",
                       "expressionType": "SQL",
                       "sqlExpression": "100.0 * COUNT(CASE WHEN denial_reason_code IS NOT NULL THEN 1 END) / COUNT(*)"},
            "adhoc_filters": [],
            "y_axis_format": ".2f",
        })

    # 4. Top denial reasons (Bar)
    ids["denials"] = create_chart(
        "Top 10 Denial Reasons", ds["agg_denial_summary"], "dist_bar",
        {
            "datasource": f"{ds['agg_denial_summary']}__table",
            "viz_type": "dist_bar",
            "metrics": [{"aggregate": "SUM", "column": {"column_name": "denial_count"},
                         "expressionType": "SIMPLE", "label": "Denials"}],
            "groupby": ["denial_reason_code"],
            "row_limit": 10,
            "order_desc": True,
            "show_legend": False,
        })

    # 5. Monthly revenue (Line)
    ids["monthly"] = create_chart(
        "Monthly Revenue Trend", ds["agg_monthly_revenue"], "line",
        {
            "datasource": f"{ds['agg_monthly_revenue']}__table",
            "viz_type": "line",
            "metrics": [
                {"aggregate": "SUM", "column": {"column_name": "total_billed"},
                 "expressionType": "SIMPLE", "label": "Billed"},
                {"aggregate": "SUM", "column": {"column_name": "total_paid"},
                 "expressionType": "SIMPLE", "label": "Paid"},
            ],
            "groupby": [],
            "x_axis": "submitted_month",
            "row_limit": 1000,
            "y_axis_format": "$,.0f",
        })

    # 6. Revenue by department (Bar)
    ids["dept"] = create_chart(
        "Revenue by Department", ds["agg_monthly_revenue"], "dist_bar",
        {
            "datasource": f"{ds['agg_monthly_revenue']}__table",
            "viz_type": "dist_bar",
            "metrics": [{"aggregate": "SUM", "column": {"column_name": "total_billed"},
                         "expressionType": "SIMPLE", "label": "Billed"}],
            "groupby": ["department"],
            "row_limit": 10,
            "order_desc": True,
        })

    # 7. Document types (Pie)
    ids["docs"] = create_chart(
        "Clinical Documents by Type", ds["dim_clinical_documents"], "pie",
        {
            "datasource": f"{ds['dim_clinical_documents']}__table",
            "viz_type": "pie",
            "metric": {"aggregate": "COUNT", "column": {"column_name": "document_id"},
                       "expressionType": "SIMPLE", "label": "Docs"},
            "groupby": ["document_type"],
            "row_limit": 25,
        })

    # 8. DQ status (Table)
    ids["dq"] = create_chart(
        "Data Quality Status", ds["dq_metrics"], "table",
        {
            "datasource": f"{ds['dq_metrics']}__table",
            "viz_type": "table",
            "all_columns": ["table_name", "zone", "row_count", "duplicate_pct",
                            "worst_null_column", "worst_null_pct", "status"],
            "row_limit": 50,
            "order_by_cols": ['["table_name",true]'],
        })

    return ids


# ── 5. Build dashboard with proper ROW-based grid layout ───────────────
def build_dashboard(chart_ids):
    # Superset's 12-col grid. CHART must live inside a ROW, not directly in GRID.
    # Each ROW's children share that row; multiple CHART widths sum ≤ 12.
    def chart(key, cid, width, height):
        return {
            "type": "CHART",
            "id": key,
            "meta": {"chartId": cid, "width": width, "height": height},
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
        }

    def row(key, children):
        return {
            "type": "ROW",
            "id": key,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "children": children,
            "parents": ["ROOT_ID", "GRID_ID"],
        }

    # Layout — 4 rows (KPI, monthly+denials, dept+docs, full-width DQ)
    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": ["ROW-kpi", "ROW-rev", "ROW-detail", "ROW-dq"],
            "parents": ["ROOT_ID"],
        },

        # Row 1: KPIs (3 big numbers, each 4/12)
        "ROW-kpi": row("ROW-kpi", ["CHART-billed", "CHART-paid", "CHART-denial"]),
        "CHART-billed": chart("CHART-billed", chart_ids["billed"],      4, 30),
        "CHART-paid":   chart("CHART-paid",   chart_ids["paid"],        4, 30),
        "CHART-denial": chart("CHART-denial", chart_ids["denial_rate"], 4, 30),

        # Row 2: Monthly trend + Top denials (each 6/12)
        "ROW-rev": row("ROW-rev", ["CHART-monthly", "CHART-denials"]),
        "CHART-monthly": chart("CHART-monthly", chart_ids["monthly"], 6, 50),
        "CHART-denials": chart("CHART-denials", chart_ids["denials"], 6, 50),

        # Row 3: Department revenue + Documents (each 6/12)
        "ROW-detail": row("ROW-detail", ["CHART-dept", "CHART-docs"]),
        "CHART-dept":   chart("CHART-dept", chart_ids["dept"], 6, 50),
        "CHART-docs":   chart("CHART-docs", chart_ids["docs"], 6, 50),

        # Row 4: Full-width DQ table
        "ROW-dq": row("ROW-dq", ["CHART-dq"]),
        "CHART-dq": chart("CHART-dq", chart_ids["dq"], 12, 50),
    }

    body = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": "medical-data-lake-ops",
        "published": True,
        "position_json": json.dumps(positions),
    }
    r = c.request("POST", "/api/v1/dashboard/", body=body)
    dash_id = r["id"]
    print(f"  ✓ dashboard created (id={dash_id})")

    # Attach charts to the dashboard
    for cid in chart_ids.values():
        try:
            c.request("PUT", f"/api/v1/chart/{cid}",
                      body={"dashboards": [dash_id]})
        except Exception as e:
            print(f"  ⚠ link chart {cid}: {e}")
    return dash_id


def main():
    print("== Login ==")
    c.login()
    print(f"  ✓ token acquired, csrf set")

    print("== Find Trino DB ==")
    db_id = find_db_id()
    print(f"  ✓ db_id = {db_id}")

    print("== Cleanup prior dashboard ==")
    cleanup_prior()

    print("== Register datasets ==")
    ds_ids = ensure_datasets(db_id)
    if len(ds_ids) < len(TABLES):
        print(f"✗ missing datasets ({len(ds_ids)}/{len(TABLES)})")
        sys.exit(1)

    print("== Create charts ==")
    chart_ids = build_charts(ds_ids)

    print("== Build dashboard ==")
    dash_id = build_dashboard(chart_ids)

    print()
    print("=" * 60)
    print(f" ✓ Dashboard ready: {BASE}/superset/dashboard/{dash_id}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
