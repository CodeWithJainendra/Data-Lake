// ============================================================
// Medical & Billing Data Lake — Dashboard
// All charts render in parallel, auto-refresh every 60s.
// ============================================================

// ── Chart.js defaults ──────────────────────────────────────────
Chart.defaults.color = "#94a3b8";
Chart.defaults.borderColor = "#243047";
Chart.defaults.font.family = "ui-sans-serif, system-ui, -apple-system";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.position = "bottom";
Chart.defaults.plugins.legend.labels.boxWidth = 10;
Chart.defaults.plugins.legend.labels.padding = 12;

// ── Palette ────────────────────────────────────────────────────
const C = {
  accent:  "#06b6d4",
  accent2: "#14b8a6",
  accent3: "#22d3ee",
  pos:     "#22c55e",
  warn:    "#f59e0b",
  neg:     "#ef4444",
  info:    "#60a5fa",
  pal: [
    "#06b6d4", "#14b8a6", "#22c55e", "#60a5fa",
    "#a78bfa", "#f59e0b", "#ec4899", "#ef4444",
    "#84cc16", "#06b6d4"
  ],
};

// ── Formatters ─────────────────────────────────────────────────
const fmt = {
  int:  (v) => v == null ? "—" : Number(v).toLocaleString("en-US"),
  money: (v) => {
    if (v == null) return "—";
    const n = Number(v);
    if (Math.abs(n) >= 1e9) return "$" + (n / 1e9).toFixed(2) + "B";
    if (Math.abs(n) >= 1e6) return "$" + (n / 1e6).toFixed(2) + "M";
    if (Math.abs(n) >= 1e3) return "$" + (n / 1e3).toFixed(1) + "K";
    return "$" + n.toFixed(0);
  },
  pct: (v) => v == null ? "—" : Number(v).toFixed(1) + "%",
  dec: (v) => v == null ? "—" : Number(v).toFixed(2),
};

// ── Fetch helper ───────────────────────────────────────────────
async function getJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path} → ${r.status}`);
  return r.json();
}

// ── KPI render ─────────────────────────────────────────────────
function renderKPIs(d) {
  const map = {
    total_patients:   fmt.int(d.total_patients),
    total_encounters: fmt.int(d.total_encounters),
    total_claims:     fmt.int(d.total_claims),
    total_billed:     fmt.money(d.total_billed),
    total_paid:       fmt.money(d.total_paid),
    collection_rate:  fmt.pct(d.collection_rate),
    denial_rate:      fmt.pct(d.denial_rate),
    avg_days_to_pay:  d.avg_days_to_pay != null ? fmt.dec(d.avg_days_to_pay) + " d" : "—",
  };
  for (const [k, v] of Object.entries(map)) {
    const el = document.querySelector(`[data-kpi="${k}"]`);
    if (el) el.textContent = v;
  }
}

// ── Chart registry (so we can destroy + redraw on refresh) ─────
const charts = {};
function makeChart(id, config) {
  const el = document.getElementById(id);
  if (!el) return;
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart(el, config);
}

// ── 1. Monthly revenue trend ───────────────────────────────────
function renderMonthly(rows) {
  rows.sort((a, b) => a.y - b.y || a.m - b.m);
  const labels = rows.map(r => `${r.y}-${String(r.m).padStart(2,"0")}`);
  makeChart("chart-monthly", {
    type: "line",
    data: {
      labels,
      datasets: [
        { label: "Billed", data: rows.map(r => +r.billed || 0),
          borderColor: C.accent,  backgroundColor: C.accent + "33",
          tension: 0.35, fill: true, borderWidth: 2 },
        { label: "Paid",   data: rows.map(r => +r.paid || 0),
          borderColor: C.pos,     backgroundColor: C.pos + "22",
          tension: 0.35, fill: true, borderWidth: 2 },
        { label: "Denied (count)", data: rows.map(r => +r.denied || 0),
          borderColor: C.neg,     backgroundColor: "transparent",
          borderDash: [4,3], tension: 0.35, borderWidth: 1.5, yAxisID: "y1" },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        y:  { ticks: { callback: v => fmt.money(v) } },
        y1: { position: "right", grid: { display: false }, ticks: { color: C.neg } },
        x:  { grid: { display: false } },
      },
      plugins: {
        tooltip: { callbacks: { label: ctx => {
          const v = ctx.parsed.y;
          if (ctx.dataset.yAxisID === "y1") return `${ctx.dataset.label}: ${fmt.int(v)}`;
          return `${ctx.dataset.label}: ${fmt.money(v)}`;
        }}},
      },
    },
  });
}

// ── 2. Payer mix (donut) ───────────────────────────────────────
function renderPayer(rows) {
  rows = rows.filter(r => r.paid && r.paid > 0).slice(0, 8);
  makeChart("chart-payer", {
    type: "doughnut",
    data: {
      labels: rows.map(r => r.payer_name),
      datasets: [{
        data: rows.map(r => +r.paid),
        backgroundColor: rows.map((_, i) => C.pal[i % C.pal.length]),
        borderColor: "#0f172a", borderWidth: 2,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      cutout: "60%",
      plugins: { tooltip: { callbacks: { label: ctx => `${ctx.label}: ${fmt.money(ctx.parsed)}` }}},
    },
  });
}

// ── 3. AR Aging (horizontal bar) ───────────────────────────────
function renderAging(rows) {
  const order = ["0–15 days","16–30 days","31–60 days","61–90 days","90+ days","Pending"];
  rows.sort((a,b) => order.indexOf(a.bucket) - order.indexOf(b.bucket));
  makeChart("chart-aging", {
    type: "bar",
    data: {
      labels: rows.map(r => r.bucket),
      datasets: [
        { label: "Claims",  data: rows.map(r => +r.claim_count || 0),
          backgroundColor: C.accent + "cc", borderRadius: 4 },
        { label: "Billed $",data: rows.map(r => +r.billed || 0),
          backgroundColor: C.warn + "cc",   borderRadius: 4, yAxisID: "y1" },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y:  { ticks: { callback: fmt.int } },
        y1: { position: "right", grid: { display: false }, ticks: { callback: fmt.money, color: C.warn } },
        x:  { grid: { display: false } },
      },
      plugins: { tooltip: { callbacks: { label: ctx => {
        const v = ctx.parsed.y;
        return ctx.dataset.yAxisID === "y1"
          ? `${ctx.dataset.label}: ${fmt.money(v)}`
          : `${ctx.dataset.label}: ${fmt.int(v)}`;
      }}}},
    },
  });
}

// ── 4. Revenue by department ───────────────────────────────────
function renderDept(rows) {
  rows.sort((a,b) => +b.billed - +a.billed);
  makeChart("chart-dept", {
    type: "bar",
    data: {
      labels: rows.map(r => r.department),
      datasets: [
        { label: "Billed", data: rows.map(r => +r.billed),
          backgroundColor: C.accent + "cc", borderRadius: 4 },
        { label: "Paid",   data: rows.map(r => +r.paid),
          backgroundColor: C.pos + "cc",    borderRadius: 4 },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      indexAxis: "y",
      scales: { x: { ticks: { callback: fmt.money } } },
      plugins: { tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmt.money(ctx.parsed.x)}` }}},
    },
  });
}

// ── 5. Top denials ─────────────────────────────────────────────
function renderDenials(rows) {
  rows.sort((a,b) => +b.at_risk - +a.at_risk);
  makeChart("chart-denials", {
    type: "bar",
    data: {
      labels: rows.map(r => r.code),
      datasets: [
        { label: "Dollars at risk", data: rows.map(r => +r.at_risk),
          backgroundColor: C.neg + "cc", borderRadius: 4 },
        { label: "Denial count", data: rows.map(r => +r.denials),
          backgroundColor: C.warn + "cc", borderRadius: 4, yAxisID: "y1" },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        y:  { ticks: { callback: fmt.money } },
        y1: { position: "right", grid: { display: false }, ticks: { callback: fmt.int, color: C.warn } },
        x:  { grid: { display: false } },
      },
      plugins: { tooltip: { callbacks: { label: ctx => {
        const v = ctx.parsed.y;
        return ctx.dataset.yAxisID === "y1"
          ? `${ctx.dataset.label}: ${fmt.int(v)}`
          : `${ctx.dataset.label}: ${fmt.money(v)}`;
      }}}},
    },
  });
}

// ── 6. RAF by department ───────────────────────────────────────
function renderRaf(rows) {
  rows.sort((a,b) => +b.avg_raf - +a.avg_raf);
  makeChart("chart-raf", {
    type: "bar",
    data: {
      labels: rows.map(r => r.department),
      datasets: [{
        label: "Avg RAF Score",
        data: rows.map(r => +r.avg_raf || 0),
        backgroundColor: rows.map((_, i) => C.pal[i % C.pal.length]),
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      indexAxis: "y",
      scales: { x: { ticks: { callback: fmt.dec } } },
    },
  });
}

// ── 7. Document types ──────────────────────────────────────────
function renderDocs(rows) {
  makeChart("chart-docs", {
    type: "doughnut",
    data: {
      labels: rows.map(r => r.type),
      datasets: [{
        data: rows.map(r => +r.n),
        backgroundColor: rows.map((_, i) => C.pal[i % C.pal.length]),
        borderColor: "#0f172a", borderWidth: 2,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      cutout: "60%",
      plugins: { tooltip: { callbacks: { label: ctx => `${ctx.label}: ${fmt.int(ctx.parsed)}` }}},
    },
  });
}

// ── 8. Severe Dx capture vs encounters ─────────────────────────
function renderSevere(rows) {
  rows.sort((a,b) => +b.encounters - +a.encounters);
  makeChart("chart-severe", {
    type: "bar",
    data: {
      labels: rows.map(r => r.department),
      datasets: [
        { label: "Encounters", data: rows.map(r => +r.encounters),
          backgroundColor: C.info + "aa", borderRadius: 4 },
        { label: "Severe Dx captured", data: rows.map(r => +r.severe_dx),
          backgroundColor: C.pos + "cc", borderRadius: 4 },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: { y: { ticks: { callback: fmt.int } } },
    },
  });
}

// ── 9. Provider table ──────────────────────────────────────────
function renderProviders(rows) {
  const tbody = document.getElementById("providers-tbody");
  if (!rows.length) { tbody.innerHTML = '<tr><td colspan="6" class="empty">No data.</td></tr>'; return; }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><code>${r.provider_id}</code></td>
      <td>${r.department ?? "—"}</td>
      <td class="num">${fmt.int(r.encounters)}</td>
      <td class="num">${fmt.money(r.charges)}</td>
      <td class="num">${fmt.dec(r.raf)}</td>
      <td class="num">${fmt.int(r.severe_dx)}</td>
    </tr>
  `).join("");
}

// ── 10. DQ table ───────────────────────────────────────────────
function renderDQ(rows) {
  const tbody = document.getElementById("dq-tbody");
  if (!rows.length) { tbody.innerHTML = '<tr><td colspan="7" class="empty">No DQ runs yet.</td></tr>'; return; }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.zone}</td>
      <td><code>${r.table_name}</code></td>
      <td class="num">${fmt.int(r.row_count)}</td>
      <td class="num">${fmt.dec(r.duplicate_pct)}</td>
      <td>${r.worst_null_column ?? "—"}</td>
      <td class="num">${fmt.dec(r.worst_null_pct)}</td>
      <td><span class="status-badge status-${r.status}">${r.status}</span></td>
    </tr>
  `).join("");
}

// ── 11. DLQ table ──────────────────────────────────────────────
function renderDLQ(rows) {
  const tbody = document.getElementById("dlq-tbody");
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="4" class="empty">✓ No DLQ entries — clean pipeline.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><span class="status-badge status-WARN">${r.source}</span></td>
      <td><code>${r.item}</code></td>
      <td>${r.document_type ?? "—"}</td>
      <td>${r.failed_at ?? "—"}</td>
    </tr>
  `).join("");
}

// ── Health pill ────────────────────────────────────────────────
async function updateHealth() {
  const pill = document.getElementById("health-pill");
  const txt  = document.getElementById("health-text");
  try {
    const r = await getJSON("/api/health");
    pill.classList.remove("bad");
    pill.classList.add("ok");
    txt.textContent = "Trino: online";
  } catch (e) {
    pill.classList.remove("ok");
    pill.classList.add("bad");
    txt.textContent = "Trino: unreachable";
  }
}

function setUpdatedNow() {
  const el = document.getElementById("updated-pill");
  const t = new Date();
  el.textContent = "Updated " + t.toLocaleTimeString();
}

// ── Loader: fire all queries in parallel ───────────────────────
async function loadAll() {
  const btn = document.getElementById("refresh-btn");
  btn.classList.add("spinning");

  await Promise.allSettled([
    getJSON("/api/kpis").then(renderKPIs),
    getJSON("/api/monthly-revenue").then(renderMonthly),
    getJSON("/api/payer-mix").then(renderPayer),
    getJSON("/api/ar-aging").then(renderAging),
    getJSON("/api/department-revenue").then(renderDept),
    getJSON("/api/top-denials").then(renderDenials),
    getJSON("/api/raf-by-department").then(renderRaf),
    getJSON("/api/document-types").then(renderDocs),
    getJSON("/api/raf-by-department").then(renderSevere),
    getJSON("/api/top-providers").then(renderProviders),
    getJSON("/api/dq-status").then(renderDQ),
    getJSON("/api/dlq-summary").then(renderDLQ),
    updateHealth(),
  ]);

  setUpdatedNow();
  setTimeout(() => btn.classList.remove("spinning"), 600);
}

// ── Tab switcher ───────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".tab").forEach(tab => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
      document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
      tab.classList.add("active");
      document.getElementById("panel-" + tab.dataset.tab).classList.add("active");
    });
  });
  document.getElementById("refresh-btn").addEventListener("click", loadAll);

  loadAll();
  // Auto-refresh every 60s
  setInterval(loadAll, 60_000);
});
