import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, ReferenceLine } from "recharts";
import { fetchSupplyDemandGap, fetchCompanies } from "../api";

function fmt(n) {
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(1);
}

function GapBar({ value }) {
  const color = value > 0 ? "#34d399" : value < 0 ? "#f87171" : "#64748b";
  const width = Math.min(100, Math.abs(value / 10));
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <div style={{ background: color, height: 8, width: `${width}px`, borderRadius: 4, minWidth: 4 }} />
      <span style={{ color, fontWeight: 600 }}>{fmt(value)}</span>
    </div>
  );
}

export default function SupplyDemandPage() {
  const [rows, setRows] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [filter, setFilter] = useState("all");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => { fetchCompanies().then(setCompanies); }, []);

  useEffect(() => {
    setLoading(true);
    const params = {};
    if (company) params.company = company;
    fetchSupplyDemandGap(params)
      .then(setRows)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [company]);

  const filtered = rows.filter(r => {
    const gap = parseFloat(r.GAP || r.gap) || 0;
    if (filter === "shortage") return gap < 0;
    if (filter === "surplus") return gap > 0;
    return true;
  });

  const top20 = filtered.slice(0, 20).map(r => ({
    item: (r.ITEM_ID || r.item_id || "").slice(0, 12),
    gap: parseFloat(r.GAP || r.gap) || 0,
    supply: (parseFloat(r.AVAIL_PHYSICAL || r.avail_physical) || 0) + (parseFloat(r.ON_ORDER || r.on_order) || 0),
    demand: parseFloat(r.FORECAST_DEMAND || r.forecast_demand) || 0,
  }));

  const shortages = rows.filter(r => (parseFloat(r.GAP || r.gap) || 0) < 0).length;
  const surpluses = rows.filter(r => (parseFloat(r.GAP || r.gap) || 0) > 0).length;

  return (
    <div>
      <div className="page-header">
        <h1>Supply vs Demand Gap</h1>
        <p>3-month forward view: available + on-order supply vs. forecasted demand</p>
      </div>

      <div className="kpi-row" style={{ marginBottom: 20 }}>
        <div className="kpi-card red">
          <div className="label">Items at Shortage Risk</div>
          <div className="value">{shortages}</div>
          <div className="sub">gap &lt; 0</div>
        </div>
        <div className="kpi-card green">
          <div className="label">Items with Surplus</div>
          <div className="value">{surpluses}</div>
          <div className="sub">gap &gt; 0</div>
        </div>
        <div className="kpi-card">
          <div className="label">Total Items Analyzed</div>
          <div className="value">{rows.length}</div>
          <div className="sub">with active forecast</div>
        </div>
      </div>

      <div className="filters">
        <label>Company:</label>
        <select value={company} onChange={e => setCompany(e.target.value)}>
          <option value="">All Companies</option>
          {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
        </select>
        <label>Show:</label>
        <select value={filter} onChange={e => setFilter(e.target.value)}>
          <option value="all">All Items</option>
          <option value="shortage">Shortages Only</option>
          <option value="surplus">Surplus Only</option>
        </select>
      </div>

      {loading && <div className="loading">Calculating supply/demand gap...</div>}
      {error && <div className="error">Error: {error}</div>}

      {!loading && !error && (
        <>
          <div className="chart-grid full" style={{ marginBottom: 24 }}>
            <div className="chart-card">
              <h3>Top 20 Items — Gap (Supply − Demand)</h3>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={top20} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" horizontal={false} />
                  <XAxis type="number" tick={{ fill: "#64748b", fontSize: 10 }} tickFormatter={fmt} />
                  <YAxis type="category" dataKey="item" tick={{ fill: "#94a3b8", fontSize: 10 }} width={90} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v) => [fmt(v), ""]}
                  />
                  <ReferenceLine x={0} stroke="#475569" />
                  <Bar dataKey="gap" name="Gap" fill="#60a5fa" radius={[0,4,4,0]}
                    label={false}
                    fill="url(#gapColor)"
                  />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Company</th>
                  <th>Item ID</th>
                  <th>Site</th>
                  <th>Avail Physical</th>
                  <th>On Order</th>
                  <th>Forecast Demand (3mo)</th>
                  <th>Gap</th>
                  <th>Risk</th>
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 150).map((r, i) => {
                  const gap = parseFloat(r.GAP || r.gap) || 0;
                  const riskBadge = gap < 0 ? "badge-red" : gap === 0 ? "badge-amber" : "badge-green";
                  const riskLabel = gap < 0 ? "Shortage" : gap === 0 ? "Balanced" : "Surplus";
                  return (
                    <tr key={i}>
                      <td><span className="badge badge-blue">{r.COMPANY || r.company}</span></td>
                      <td style={{ fontFamily: "monospace" }}>{r.ITEM_ID || r.item_id}</td>
                      <td>{r.SITE || r.site}</td>
                      <td>{fmt(r.AVAIL_PHYSICAL || r.avail_physical)}</td>
                      <td>{fmt(r.ON_ORDER || r.on_order)}</td>
                      <td>{fmt(r.FORECAST_DEMAND || r.forecast_demand)}</td>
                      <td><GapBar value={gap} /></td>
                      <td><span className={`badge ${riskBadge}`}>{riskLabel}</span></td>
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
