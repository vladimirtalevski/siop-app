import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { fetchBOMForecast, fetchCompanies } from "../api";

const MONTHS = [
  { key: "jan_2026", label: "Jan" }, { key: "feb_2026", label: "Feb" },
  { key: "mar_2026", label: "Mar" }, { key: "apr_2026", label: "Apr" },
  { key: "may_2026", label: "May" }, { key: "jun_2026", label: "Jun" },
  { key: "jul_2026", label: "Jul" }, { key: "aug_2026", label: "Aug" },
  { key: "sep_2026", label: "Sep" }, { key: "oct_2026", label: "Oct" },
  { key: "nov_2026", label: "Nov" }, { key: "dec_2026", label: "Dec" },
];

const SI_MONTHS = [
  { key: "si_jan", label: "Jan" }, { key: "si_feb", label: "Feb" },
  { key: "si_mar", label: "Mar" }, { key: "si_apr", label: "Apr" },
  { key: "si_may", label: "May" }, { key: "si_jun", label: "Jun" },
  { key: "si_jul", label: "Jul" }, { key: "si_aug", label: "Aug" },
  { key: "si_sep", label: "Sep" }, { key: "si_oct", label: "Oct" },
  { key: "si_nov", label: "Nov" }, { key: "si_dec", label: "Dec" },
];

function fmt(n) {
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(1);
}

const g = (r, ...keys) => { for (const k of keys) if (r[k] != null) return r[k]; return null; };

function siColor(v) {
  const n = parseFloat(v);
  if (isNaN(n)) return "#64748b";
  if (n >= 1.2) return "#34d399";
  if (n <= 0.8) return "#f87171";
  return "#94a3b8";
}

export default function ForecastPage() {
  const [rows, setRows] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [bomLevel, setBomLevel] = useState("");
  const [showSI, setShowSI] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => { fetchCompanies().then(c => setCompanies(c)); }, []);

  useEffect(() => {
    setLoading(true);
    const params = {};
    if (company) params.company = company;
    fetchBOMForecast(params)
      .then(r => setRows(r))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [company]);

  const filtered = bomLevel !== ""
    ? rows.filter(r => String(g(r, "bom_level", "BOM_LEVEL")) === bomLevel)
    : rows;

  // Monthly totals for bar chart
  const monthlyChart = MONTHS.map(m => ({
    month: m.label,
    qty: filtered.reduce((s, r) => s + (parseFloat(g(r, m.key, m.key.toUpperCase())) || 0), 0),
  }));

  // Top 15 components by annual total
  const byComponent = Object.values(
    filtered.reduce((acc, r) => {
      const id = g(r, "component_itemid", "COMPONENT_ITEMID");
      if (!acc[id]) acc[id] = { id, name: g(r, "part_name", "PART_NAME") || id, total: 0 };
      acc[id].total += parseFloat(g(r, "annual_2026", "ANNUAL_2026")) || 0;
      return acc;
    }, {})
  ).sort((a, b) => b.total - a.total).slice(0, 15);

  // KPIs
  const totalAnnual = filtered.reduce((s, r) => s + (parseFloat(g(r, "annual_2026", "ANNUAL_2026")) || 0), 0);
  const distinctComponents = new Set(filtered.map(r => g(r, "component_itemid", "COMPONENT_ITEMID"))).size;
  const avgMonthly = totalAnnual / 12;
  const fullHistory = filtered.filter(r => parseInt(g(r, "months_with_data", "MONTHS_WITH_DATA")) >= 12).length;

  return (
    <div>
      <div className="page-header">
        <h1>Demand Forecast 2026</h1>
        <p>BOM explosion with WMA baseline + seasonal index — component-level monthly demand</p>
      </div>

      <div className="filters">
        <label>Company:</label>
        <select value={company} onChange={e => setCompany(e.target.value)}>
          <option value="">All Companies</option>
          {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
        </select>

        <label style={{ marginLeft: 16 }}>BOM Level:</label>
        <select value={bomLevel} onChange={e => setBomLevel(e.target.value)}>
          <option value="">All Levels</option>
          {[0, 1, 2, 3, 4, 5, 6, 7].map(l => <option key={l} value={String(l)}>Level {l}</option>)}
        </select>

        <label style={{ marginLeft: 16 }}>Seasonal Indices:</label>
        <button onClick={() => setShowSI(v => !v)} style={{
          padding: "4px 14px", borderRadius: 6, border: "none", cursor: "pointer", fontSize: 13,
          background: showSI ? "#3b82f6" : "#1e2535", color: showSI ? "#fff" : "#94a3b8",
        }}>{showSI ? "Visible" : "Hidden"}</button>
      </div>

      {loading && <div className="loading">Running BOM explosion forecast...</div>}
      {error && <div className="error">Error: {error}</div>}

      {!loading && !error && (
        <>
          <div className="kpi-row">
            <div className="kpi-card blue">
              <div className="label">2026 Annual Demand</div>
              <div className="value">{fmt(totalAnnual)}</div>
              <div className="sub">total component units</div>
            </div>
            <div className="kpi-card green">
              <div className="label">Avg Monthly Demand</div>
              <div className="value">{fmt(avgMonthly)}</div>
              <div className="sub">units/month across components</div>
            </div>
            <div className="kpi-card amber">
              <div className="label">Distinct Components</div>
              <div className="value">{distinctComponents.toLocaleString()}</div>
              <div className="sub">unique part numbers</div>
            </div>
            <div className="kpi-card">
              <div className="label">Full History</div>
              <div className="value">{fullHistory}</div>
              <div className="sub">lines with 12+ months data</div>
            </div>
          </div>

          <div className="chart-grid">
            <div className="chart-card">
              <h3>Monthly Component Demand — 2026</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={monthlyChart}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis dataKey="month" tick={{ fill: "#64748b", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v) => [fmt(v), "Forecast Qty"]}
                  />
                  <Bar dataKey="qty" name="Forecast Qty" fill="#a78bfa" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-card">
              <h3>Top 15 Components by Annual Demand</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={byComponent} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis type="number" tick={{ fill: "#64748b", fontSize: 10 }} tickFormatter={fmt} />
                  <YAxis type="category" dataKey="id" tick={{ fill: "#64748b", fontSize: 10 }} width={110} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v, _n, p) => [fmt(v), p.payload.name || p.payload.id]}
                  />
                  <Bar dataKey="total" name="Annual 2026" fill="#60a5fa" radius={[0,4,4,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="table-wrap" style={{ overflowX: "auto" }}>
            <table className="data-table" style={{ minWidth: showSI ? 2400 : 1400 }}>
              <thead>
                <tr>
                  <th style={{ position: "sticky", left: 0, background: "#0f1420", zIndex: 2 }}>Component</th>
                  <th style={{ minWidth: 160 }}>Description</th>
                  <th>Company</th>
                  <th>BOM Lvl</th>
                  <th>WMA Base</th>
                  <th>Months</th>
                  {MONTHS.map(m => <th key={m.key} style={{ minWidth: 60 }}>{m.label}</th>)}
                  <th style={{ fontWeight: 700, color: "#60a5fa" }}>Annual</th>
                  {showSI && SI_MONTHS.map(m => <th key={m.key} style={{ minWidth: 52, fontSize: 11, color: "#64748b" }}>SI {m.label}</th>)}
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 500).map((r, i) => {
                  const lvl = parseInt(g(r, "bom_level", "BOM_LEVEL")) || 0;
                  return (
                    <tr key={i}>
                      <td style={{ position: "sticky", left: 0, background: "#0f1420", fontFamily: "monospace", fontSize: 11, zIndex: 1 }}>
                        {g(r, "component_itemid", "COMPONENT_ITEMID")}
                      </td>
                      <td style={{ maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "#94a3b8", fontSize: 12 }}>
                        {g(r, "part_name", "PART_NAME") || "—"}
                      </td>
                      <td><span className="badge badge-blue">{g(r, "company", "COMPANY")}</span></td>
                      <td>
                        <span className="badge badge-amber" style={{ fontSize: 11 }}>L{lvl}</span>
                      </td>
                      <td style={{ color: "#64748b" }}>{fmt(g(r, "wma_baseline", "WMA_BASELINE"))}</td>
                      <td style={{ color: "#64748b", fontSize: 12 }}>{g(r, "months_with_data", "MONTHS_WITH_DATA")}</td>
                      {MONTHS.map(m => {
                        const v = parseFloat(g(r, m.key, m.key.toUpperCase())) || 0;
                        return (
                          <td key={m.key} style={{ fontSize: 12, color: v > 0 ? "#e2e8f0" : "#334155" }}>
                            {v > 0 ? fmt(v) : "—"}
                          </td>
                        );
                      })}
                      <td style={{ fontWeight: 700, color: "#60a5fa" }}>
                        {fmt(g(r, "annual_2026", "ANNUAL_2026"))}
                      </td>
                      {showSI && SI_MONTHS.map(m => {
                        const v = parseFloat(g(r, m.key, m.key.toUpperCase()));
                        return (
                          <td key={m.key} style={{ fontSize: 11, color: siColor(v), textAlign: "center" }}>
                            {isNaN(v) ? "—" : v.toFixed(2)}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
