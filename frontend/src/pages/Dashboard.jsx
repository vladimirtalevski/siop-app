import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { fetchInventorySummary, fetchPOSummary, fetchForecastByMonth } from "../api";

function fmt(n) {
  if (n == null) return "—";
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(0);
}

export default function Dashboard({ onNavigate }) {
  const [inv, setInv] = useState([]);
  const [pos, setPOs] = useState([]);
  const [forecast, setForecast] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      fetchInventorySummary(),
      fetchPOSummary(),
      fetchForecastByMonth(),
    ])
      .then(([i, p, f]) => { setInv(i); setPOs(p); setForecast(f); })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return (
    <div className="loading" style={{ flexDirection: "column", gap: 12 }}>
      <div>Querying Snowflake...</div>
      <div style={{ fontSize: 13, color: "#64748b" }}>
        First load takes ~30 sec while Snowflake cold-starts.<br />
        Check the backend terminal — complete the SSO popup if prompted.
      </div>
    </div>
  );
  if (error) return (
    <div className="error">
      <strong>Connection error:</strong> {error}
      <div style={{ marginTop: 8, fontSize: 13, color: "#94a3b8" }}>
        Make sure the backend is running at <a href="http://localhost:8000/health" target="_blank" rel="noreferrer" style={{color:"#60a5fa"}}>localhost:8000/health</a> and SSO login was completed.
      </div>
    </div>
  );

  const g = (r, ...keys) => { for (const k of keys) if (r[k] != null) return r[k]; return null; };

  const totalItems = inv.reduce((s, r) => s + (parseInt(g(r, "distinct_items", "DISTINCT_ITEMS")) || 0), 0);
  const totalPhysical = inv.reduce((s, r) => s + (parseFloat(g(r, "total_avail_physical", "TOTAL_AVAIL_PHYSICAL")) || 0), 0);
  const totalOnOrder = inv.reduce((s, r) => s + (parseFloat(g(r, "total_on_order", "TOTAL_ON_ORDER")) || 0), 0);
  const openPOValue = pos.reduce((s, r) => s + (parseFloat(g(r, "open_value", "OPEN_VALUE", "remaining_value", "REMAINING_VALUE")) || 0), 0);

  const invByCompany = inv.reduce((acc, r) => {
    const c = g(r, "company", "COMPANY");
    if (!acc[c]) acc[c] = { company: c, avail: 0, onOrder: 0 };
    acc[c].avail += parseFloat(g(r, "total_avail_physical", "TOTAL_AVAIL_PHYSICAL")) || 0;
    acc[c].onOrder += parseFloat(g(r, "total_on_order", "TOTAL_ON_ORDER")) || 0;
    return acc;
  }, {});
  const invChartData = Object.values(invByCompany);

  const fcastChartData = forecast
    .filter(r => g(r, "month", "MONTH"))
    .slice(-12)
    .map(r => ({
      month: (g(r, "month", "MONTH") || "").slice(0, 7),
      qty: parseFloat(g(r, "total_forecast_qty", "TOTAL_FORECAST_QTY")) || 0,
      amount: parseFloat(g(r, "total_forecast_amount", "TOTAL_FORECAST_AMOUNT")) || 0,
    }));

  return (
    <div>
      <div className="page-header">
        <h1>SIOP Dashboard</h1>
        <p>Supply, Inventory &amp; Operations — FLSmidth Global</p>
      </div>

      <div className="kpi-row">
        <div className="kpi-card blue">
          <div className="label">Distinct Items (On-Hand)</div>
          <div className="value">{fmt(totalItems)}</div>
          <div className="sub">across all sites</div>
        </div>
        <div className="kpi-card green">
          <div className="label">Avail Physical Qty</div>
          <div className="value">{fmt(totalPhysical)}</div>
          <div className="sub">total units available</div>
        </div>
        <div className="kpi-card amber">
          <div className="label">On Order Qty</div>
          <div className="value">{fmt(totalOnOrder)}</div>
          <div className="sub">inbound supply</div>
        </div>
        <div className="kpi-card red">
          <div className="label">Open PO Value</div>
          <div className="value">${fmt(openPOValue)}</div>
          <div className="sub">pending delivery</div>
        </div>
        <div className="kpi-card">
          <div className="label">Legal Entities</div>
          <div className="value">{invByCompany ? Object.keys(invByCompany).length : "—"}</div>
          <div className="sub">companies tracked</div>
        </div>
      </div>

      <div className="chart-grid">
        <div className="chart-card">
          <h3>Available Inventory by Company</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={invChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
              <XAxis dataKey="company" tick={{ fill: "#64748b", fontSize: 11 }} />
              <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
              <Tooltip
                contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8" }}
                formatter={(v) => [fmt(v), ""]}
              />
              <Bar dataKey="avail" name="Avail Physical" fill="#60a5fa" radius={[4,4,0,0]} />
              <Bar dataKey="onOrder" name="On Order" fill="#34d399" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h3>Demand Forecast by Month</h3>
          {fcastChartData.length === 0
            ? <div className="loading" style={{height:220}}>No forecast data</div>
            : <ResponsiveContainer width="100%" height={220}>
                <BarChart data={fcastChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis dataKey="month" tick={{ fill: "#64748b", fontSize: 10 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v) => [fmt(v), ""]}
                  />
                  <Bar dataKey="qty" name="Forecast Qty" fill="#a78bfa" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
          }
        </div>
      </div>

      <div className="chart-card" style={{ marginBottom: 24 }}>
        <h3>Navigate</h3>
        <div className="quick-nav">
          <button onClick={() => onNavigate("forecast")}>Demand Forecast →</button>
          <button onClick={() => onNavigate("inventory")}>Inventory Detail →</button>
          <button onClick={() => onNavigate("purchase-orders")}>Purchase Orders →</button>
          <button onClick={() => onNavigate("sales-orders")}>Sales Order Lines →</button>
          <button onClick={() => onNavigate("supply-demand")}>Supply vs Demand Gap →</button>
        </div>
      </div>
    </div>
  );
}
