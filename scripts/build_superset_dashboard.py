#!/usr/bin/env python3
"""
Build a comprehensive Superset dashboard via the API, mirroring the Flask
dashboard's structure (8 hero KPIs + tabbed sections: Revenue Cycle, Denials,
Clinical Documentation, Providers, Data Health).

Uses modern echarts_* viz types (Superset 4.x+ — legacy `line`/`dist_bar`
were removed from the default plugin registry).

Idempotent: re-running deletes the dashboard + its charts first.
"""
import json
import sys
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

        req = urllib.request.Request(f"{BASE}/api/v1/security/csrf_token/",
                                     headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req) as r:
            self.csrf = json.loads(r.read())["result"]
            self.cookies += r.headers.get_all("Set-Cookie") or []

    def _cookie_header(self):
        return "; ".join(raw.split(";", 1)[0] for raw in self.cookies)

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
            headers["Referer"] = BASE
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            print(f"  ✗ {method} {path}: {e.code} {e.read().decode()[:200]}")
            raise


c = Client()


# ── Metric / column helpers ────────────────────────────────────────────
def sum_metric(col, label):
    return {"aggregate": "SUM", "column": {"column_name": col},
            "expressionType": "SIMPLE", "label": label,
            "optionName": f"metric_sum_{col}"}


def count_metric(col, label):
    return {"aggregate": "COUNT", "column": {"column_name": col},
            "expressionType": "SIMPLE", "label": label,
            "optionName": f"metric_count_{col}"}


def avg_metric(col, label):
    return {"aggregate": "AVG", "column": {"column_name": col},
            "expressionType": "SIMPLE", "label": label,
            "optionName": f"metric_avg_{col}"}


def sql_metric(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql,
            "label": label, "optionName": f"metric_sql_{label.replace(' ', '_')}"}


# ── 1. Login + find Trino database id ─────────────────────────────────
def find_db_id():
    rs = c.request("GET", "/api/v1/database/")
    for r in rs.get("result", []):
        if r["database_name"] == DB_NAME:
            return r["id"]
    print(f"✗ Database '{DB_NAME}' not registered. Run setup_superset.sh first.")
    sys.exit(1)


# ── 2. Register tables as datasets (idempotent) ────────────────────────
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


# ── 3. Delete prior dashboard + its charts ─────────────────────────────
def cleanup_prior():
    rs = c.request("GET", "/api/v1/dashboard/")
    matching = [d for d in rs.get("result", []) if d["dashboard_title"] == DASHBOARD_TITLE]
    for d in matching:
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
            except Exception:
                pass
        if chart_ids:
            print(f"    ✓ deleted {len(chart_ids)} associated charts")


# ── 4. Create charts ───────────────────────────────────────────────────
def chart(name, datasource_id, viz_type, params_extra):
    params = {
        "datasource": f"{datasource_id}__table",
        "viz_type": viz_type,
        "adhoc_filters": [],
        **params_extra,
    }
    body = {
        "slice_name": name,
        "datasource_id": datasource_id,
        "datasource_type": "table",
        "viz_type": viz_type,
        "params": json.dumps(params),
    }
    r = c.request("POST", "/api/v1/chart/", body=body)
    print(f"  ✓ {viz_type:30s} {name} (id={r['id']})")
    return r["id"]


def build_charts(ds):
    ids = {}

    # ─── HERO KPIs (8 big numbers) ────────────────────────────────────
    ids["k_patients"] = chart(
        "Total Patients", ds["dim_patient_masked"], "big_number_total",
        {"metric": count_metric("patient_token", "Patients"),
         "y_axis_format": ",d", "subheader": "unique MRNs in dim_patient"})

    ids["k_encounters"] = chart(
        "Encounters", ds["fact_encounters"], "big_number_total",
        {"metric": count_metric("encounter_id", "Encounters"),
         "y_axis_format": ",d", "subheader": "clinical visits tracked"})

    ids["k_claims"] = chart(
        "Claims", ds["fact_claims"], "big_number_total",
        {"metric": count_metric("claim_id", "Claims"),
         "y_axis_format": ",d", "subheader": "submitted to payers"})

    ids["k_billed"] = chart(
        "Billed Amount", ds["fact_claims"], "big_number_total",
        {"metric": sum_metric("billed_amount", "Billed"),
         "y_axis_format": "$,.2s", "subheader": "gross billed"})

    ids["k_paid"] = chart(
        "Collected", ds["fact_claims"], "big_number_total",
        {"metric": sum_metric("paid_amount", "Paid"),
         "y_axis_format": "$,.2s", "subheader": "net payments received"})

    ids["k_collection_rate"] = chart(
        "Collection Rate %", ds["fact_claims"], "big_number_total",
        {"metric": sql_metric(
            "100.0 * SUM(paid_amount) / NULLIF(SUM(billed_amount), 0)",
            "Collection Rate"),
         "y_axis_format": ".2f", "subheader": "paid ÷ billed"})

    ids["k_denial_rate"] = chart(
        "Denial Rate %", ds["fact_claims"], "big_number_total",
        {"metric": sql_metric(
            "100.0 * COUNT(CASE WHEN denial_reason_code IS NOT NULL THEN 1 END) / COUNT(*)",
            "Denial Rate"),
         "y_axis_format": ".2f", "subheader": "% of claims denied"})

    ids["k_days"] = chart(
        "Avg Days to Pay", ds["fact_claims"], "big_number_total",
        {"metric": avg_metric("days_to_adjudicate", "Days"),
         "y_axis_format": ".1f", "subheader": "submission → adjudication"})

    # ─── REVENUE CYCLE ────────────────────────────────────────────────
    ids["monthly"] = chart(
        "Monthly Revenue Trend", ds["agg_monthly_revenue"],
        "echarts_timeseries_line",
        {
            "x_axis": "submitted_month",
            "metrics": [
                sum_metric("total_billed", "Billed"),
                sum_metric("total_paid",   "Paid"),
            ],
            "groupby": [],
            "row_limit": 1000,
            "y_axis_format": "$,.2s",
            "show_legend": True,
            "rich_tooltip": True,
            "seriesType": "line",
            "markerEnabled": True,
            "color_scheme": "supersetColors",
        })

    ids["payer"] = chart(
        "Payer Mix", ds["agg_monthly_revenue"], "pie",
        {
            "groupby": ["payer_name"],
            "metric": sum_metric("total_paid", "Paid"),
            "row_limit": 25,
            "donut": True,
            "innerRadius": 50,
            "outerRadius": 80,
            "show_legend": True,
            "label_type": "key",
            "number_format": "$,.2s",
            "color_scheme": "supersetColors",
        })

    ids["dept"] = chart(
        "Revenue by Department", ds["agg_monthly_revenue"],
        "echarts_timeseries_bar",
        {
            "x_axis": "department",
            "metrics": [
                sum_metric("total_billed", "Billed"),
                sum_metric("total_paid",   "Paid"),
            ],
            "groupby": [],
            "row_limit": 10,
            "y_axis_format": "$,.2s",
            "show_legend": True,
            "color_scheme": "supersetColors",
        })

    # ─── DENIALS ──────────────────────────────────────────────────────
    ids["denials"] = chart(
        "Top 10 Denial Reasons", ds["agg_denial_summary"],
        "echarts_timeseries_bar",
        {
            "x_axis": "denial_reason_code",
            "metrics": [
                sum_metric("denial_count",   "Denials"),
                sum_metric("billed_at_risk", "Revenue at Risk"),
            ],
            "groupby": [],
            "row_limit": 10,
            "show_legend": True,
            "color_scheme": "supersetColors",
        })

    # ─── CLINICAL DOCUMENTATION ───────────────────────────────────────
    ids["raf"] = chart(
        "Avg RAF Score by Department", ds["agg_provider_kpi"],
        "echarts_timeseries_bar",
        {
            "x_axis": "department",
            "metrics": [avg_metric("avg_raf_score", "Avg RAF")],
            "groupby": [],
            "row_limit": 15,
            "show_legend": False,
            "color_scheme": "supersetColors",
        })

    ids["docs"] = chart(
        "Clinical Documents by Type", ds["dim_clinical_documents"], "pie",
        {
            "groupby": ["document_type"],
            "metric": count_metric("document_id", "Docs"),
            "row_limit": 25,
            "donut": True,
            "innerRadius": 50,
            "outerRadius": 80,
            "show_legend": True,
            "label_type": "key_value_percent",
            "color_scheme": "supersetColors",
        })

    # ─── PROVIDERS ────────────────────────────────────────────────────
    ids["providers"] = chart(
        "Top Providers by Revenue", ds["agg_provider_kpi"], "table",
        {
            "all_columns": ["provider_id", "department", "encounter_count",
                            "total_charges", "avg_raf_score", "severe_dx_encounters"],
            "row_limit": 10,
            "order_by_cols": ['["total_charges",false]'],
            "table_timestamp_format": "smart_date",
        })

    # ─── DATA HEALTH ──────────────────────────────────────────────────
    ids["dq"] = chart(
        "Data Quality Status", ds["dq_metrics"], "table",
        {
            "all_columns": ["table_name", "zone", "row_count", "duplicate_pct",
                            "worst_null_column", "worst_null_pct", "status"],
            "row_limit": 50,
            "order_by_cols": ['["table_name",true]'],
        })

    return ids


# ── 5. Build dashboard with tabbed layout ──────────────────────────────
def build_dashboard(chart_ids):
    def chart_el(key, cid, w, h, parents):
        return {
            "type": "CHART",
            "id": key,
            "meta": {"chartId": cid, "width": w, "height": h},
            "children": [],
            "parents": parents,
        }

    def row(key, children, parents):
        return {
            "type": "ROW",
            "id": key,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "children": children,
            "parents": parents,
        }

    def tab(key, name, children, parents):
        return {
            "type": "TAB",
            "id": key,
            "meta": {"text": name},
            "children": children,
            "parents": parents,
        }

    PG = ["ROOT_ID", "GRID_ID"]                          # parents for KPI rows
    TPG = ["ROOT_ID", "GRID_ID", "TABS-MAIN"]            # parents for tabs

    positions = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID", "id": "GRID_ID",
            "children": ["ROW-kpi-1", "ROW-kpi-2", "TABS-MAIN"],
            "parents": ["ROOT_ID"],
        },

        # ── KPI ROW 1 (4 big numbers, w=3 each = 12 total) ───
        "ROW-kpi-1": row("ROW-kpi-1",
            ["CHART-patients", "CHART-encounters", "CHART-claims", "CHART-billed"], PG),
        "CHART-patients":   chart_el("CHART-patients",   chart_ids["k_patients"],   3, 30, PG + ["ROW-kpi-1"]),
        "CHART-encounters": chart_el("CHART-encounters", chart_ids["k_encounters"], 3, 30, PG + ["ROW-kpi-1"]),
        "CHART-claims":     chart_el("CHART-claims",     chart_ids["k_claims"],     3, 30, PG + ["ROW-kpi-1"]),
        "CHART-billed":     chart_el("CHART-billed",     chart_ids["k_billed"],     3, 30, PG + ["ROW-kpi-1"]),

        # ── KPI ROW 2 ───
        "ROW-kpi-2": row("ROW-kpi-2",
            ["CHART-paid", "CHART-collrate", "CHART-denrate", "CHART-days"], PG),
        "CHART-paid":     chart_el("CHART-paid",     chart_ids["k_paid"],            3, 30, PG + ["ROW-kpi-2"]),
        "CHART-collrate": chart_el("CHART-collrate", chart_ids["k_collection_rate"], 3, 30, PG + ["ROW-kpi-2"]),
        "CHART-denrate":  chart_el("CHART-denrate",  chart_ids["k_denial_rate"],     3, 30, PG + ["ROW-kpi-2"]),
        "CHART-days":     chart_el("CHART-days",     chart_ids["k_days"],            3, 30, PG + ["ROW-kpi-2"]),

        # ── TABS ────────────────────────────────────────────
        "TABS-MAIN": {
            "type": "TABS", "id": "TABS-MAIN",
            "meta": {},
            "children": ["TAB-rev", "TAB-den", "TAB-cdi", "TAB-prov", "TAB-dq"],
            "parents": ["ROOT_ID", "GRID_ID"],
        },

        # ── TAB 1: Revenue Cycle ────
        "TAB-rev": tab("TAB-rev", "Revenue Cycle", ["ROW-rev-1", "ROW-rev-2"], ["ROOT_ID", "GRID_ID", "TABS-MAIN"]),
        "ROW-rev-1": row("ROW-rev-1", ["CHART-monthly", "CHART-payer"], ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-rev"]),
        "CHART-monthly": chart_el("CHART-monthly", chart_ids["monthly"], 7, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-rev", "ROW-rev-1"]),
        "CHART-payer":   chart_el("CHART-payer",   chart_ids["payer"],   5, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-rev", "ROW-rev-1"]),
        "ROW-rev-2": row("ROW-rev-2", ["CHART-dept"], ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-rev"]),
        "CHART-dept": chart_el("CHART-dept", chart_ids["dept"], 12, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-rev", "ROW-rev-2"]),

        # ── TAB 2: Denials ────
        "TAB-den": tab("TAB-den", "Denials", ["ROW-den-1"], ["ROOT_ID", "GRID_ID", "TABS-MAIN"]),
        "ROW-den-1": row("ROW-den-1", ["CHART-denials"], ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-den"]),
        "CHART-denials": chart_el("CHART-denials", chart_ids["denials"], 12, 60, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-den", "ROW-den-1"]),

        # ── TAB 3: Clinical Documentation ────
        "TAB-cdi": tab("TAB-cdi", "Clinical Documentation", ["ROW-cdi-1"], ["ROOT_ID", "GRID_ID", "TABS-MAIN"]),
        "ROW-cdi-1": row("ROW-cdi-1", ["CHART-raf", "CHART-docs"], ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-cdi"]),
        "CHART-raf":  chart_el("CHART-raf",  chart_ids["raf"],  7, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-cdi", "ROW-cdi-1"]),
        "CHART-docs": chart_el("CHART-docs", chart_ids["docs"], 5, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-cdi", "ROW-cdi-1"]),

        # ── TAB 4: Providers ────
        "TAB-prov": tab("TAB-prov", "Providers", ["ROW-prov-1"], ["ROOT_ID", "GRID_ID", "TABS-MAIN"]),
        "ROW-prov-1": row("ROW-prov-1", ["CHART-providers"], ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-prov"]),
        "CHART-providers": chart_el("CHART-providers", chart_ids["providers"], 12, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-prov", "ROW-prov-1"]),

        # ── TAB 5: Data Health ────
        "TAB-dq": tab("TAB-dq", "Data Health", ["ROW-dq-1"], ["ROOT_ID", "GRID_ID", "TABS-MAIN"]),
        "ROW-dq-1": row("ROW-dq-1", ["CHART-dq"], ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-dq"]),
        "CHART-dq": chart_el("CHART-dq", chart_ids["dq"], 12, 50, ["ROOT_ID", "GRID_ID", "TABS-MAIN", "TAB-dq", "ROW-dq-1"]),
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
            c.request("PUT", f"/api/v1/chart/{cid}", body={"dashboards": [dash_id]})
        except Exception as e:
            print(f"  ⚠ link chart {cid}: {e}")
    return dash_id


def main():
    print("== Login ==")
    c.login()
    print("  ✓ token acquired, csrf set")

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
