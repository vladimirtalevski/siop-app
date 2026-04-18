import { useState } from "react";
import {
  ComposedChart, Area, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, Legend, ReferenceLine,
} from "recharts";
import { fetchMLForecast, fetchCompanies } from "../api";
import { useEffect } from "react";

function fmt(n) {
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(1);
}

export default function MLForecastPage() {
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [itemId, setItemId] = useState("");
  const [periods, setPeriods] = useState(12);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => { fetchCompanies().then(setCompanies); }, []);

  const runForecast = () => {
    setLoading(true);
    setError(null);
    setResult(null);
    const params = { periods };
    if (company) params.company = company;
    if (itemId.trim()) params.item_id = itemId.trim();
    fetchMLForecast(params)
      .then(r => {
        if (r.error) setError(r.error);
        else setResult(r);
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  };

  const todayStr = new Date().toISOString().slice(0, 10);
  const chartData = result?.series?.map(r => ({
    ds: r.ds?.slice(0, 7),
    actual: r.actual != null ? parseFloat(r.actual) : null,
    forecast: parseFloat(r.yhat) || 0,
    lower: parseFloat(r.yhat_lower) || 0,
    upper: parseFloat(r.yhat_upper) || 0,
    isFuture: r.is_forecast,
  })) || [];

  return (
    <div>
      <div className="page-header">
        <h1>ML Demand Forecast</h1>
        <p>Prophet time-series model trained on D365 F&amp;O demand data</p>
      </div>

      <div className="chart-card" style={{ marginBottom: 24 }}>
        <h3>Configure Forecast</h3>
        <div className="filters" style={{ marginBottom: 0, marginTop: 12 }}>
          <label>Company:</label>
          <select value={company} onChange={e => setCompany(e.target.value)}>
            <option value="">All Companies</option>
            {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
          </select>
          <label>Item ID (optional):</label>
          <input
            type="text"
            value={itemId}
            onChange={e => setItemId(e.target.value)}
            placeholder="e.g. C8048G-RV"
            style={{
              background: "#161b27", border: "1px solid #1e2535", color: "#e2e8f0",
              padding: "7px 12px", borderRadius: 7, fontSize: 13, width: 180,
            }}
          />
          <label>Forecast Months:</label>
          <select value={periods} onChange={e => setPeriods(Number(e.target.value))}>
            {[3, 6, 9, 12, 18, 24].map(p => <option key={p} value={p}>{p} months</option>)}
          </select>
          <button
            onClick={runForecast}
            disabled={loading}
            style={{
              background: loading ? "#1e2535" : "#2563eb",
              border: "none", color: "#e2e8f0", padding: "8px 20px",
              borderRadius: 7, fontSize: 13, cursor: loading ? "not-allowed" : "pointer",
              fontWeight: 600,
            }}
          >
            {loading ? "Running..." : "Run Forecast"}
          </button>
        </div>
      </div>

      {loading && <div className="loading">Fitting Prophet model on Snowflake data...</div>}
      {error && <div className="error">Error: {error}</div>}

      {result && !error && (
        <>
          <div className="kpi-row" style={{ marginBottom: 20 }}>
            <div className="kpi-card blue">
              <div className="label">Data Points Used</div>
              <div className="value">{result.data_points_used}</div>
              <div className="sub">monthly observations</div>
            </div>
            <div className="kpi-card green">
              <div className="label">Forecast Horizon</div>
              <div className="value">{result.forecast_periods} mo</div>
              <div className="sub">forward projection</div>
            </div>
            <div className="kpi-card amber">
              <div className="label">Avg Forecast / Month</div>
              <div className="value">
                {fmt(
                  chartData.filter(r => r.isFuture).reduce((s, r) => s + r.forecast, 0) /
                  Math.max(1, chartData.filter(r => r.isFuture).length)
                )}
              </div>
              <div className="sub">units projected</div>
            </div>
          </div>

          <div className="chart-grid full" style={{ marginBottom: 24 }}>
            <div className="chart-card">
              <h3>Actual vs Forecasted Demand — {result.item_id || "All Items"} {result.company ? `(${result.company.toUpperCase()})` : ""}</h3>
              <ResponsiveContainer width="100%" height={320}>
                <ComposedChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis dataKey="ds" tick={{ fill: "#64748b", fontSize: 10 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v, name) => [fmt(v), name]}
                  />
                  <Legend wrapperStyle={{ color: "#64748b", fontSize: 12 }} />
                  <Area
                    type="monotone"
                    dataKey="upper"
                    fill="#1e3a5f"
                    stroke="transparent"
                    name="80% CI Upper"
                    legendType="none"
                  />
                  <Area
                    type="monotone"
                    dataKey="lower"
                    fill="#0f1117"
                    stroke="transparent"
                    name="80% CI"
                    legendType="square"
                  />
                  <Line
                    type="monotone"
                    dataKey="forecast"
                    stroke="#60a5fa"
                    strokeWidth={2}
                    dot={false}
                    name="Prophet Forecast"
                    strokeDasharray="5 3"
                  />
                  <Line
                    type="monotone"
                    dataKey="actual"
                    stroke="#34d399"
                    strokeWidth={2.5}
                    dot={false}
                    name="Actual Demand"
                    connectNulls={false}
                  />
                  <ReferenceLine x={todayStr.slice(0, 7)} stroke="#475569" strokeDasharray="4 2" label={{ value: "Today", fill: "#64748b", fontSize: 10 }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Month</th>
                  <th>Actual</th>
                  <th>Forecast</th>
                  <th>Lower (80%)</th>
                  <th>Upper (80%)</th>
                  <th>Type</th>
                </tr>
              </thead>
              <tbody>
                {chartData.map((r, i) => (
                  <tr key={i}>
                    <td style={{ fontFamily: "monospace" }}>{r.ds}</td>
                    <td>{r.actual != null ? fmt(r.actual) : "—"}</td>
                    <td style={{ color: "#60a5fa" }}>{fmt(r.forecast)}</td>
                    <td style={{ color: "#475569" }}>{fmt(r.lower)}</td>
                    <td style={{ color: "#475569" }}>{fmt(r.upper)}</td>
                    <td>
                      <span className={`badge ${r.isFuture ? "badge-amber" : "badge-green"}`}>
                        {r.isFuture ? "Forecast" : "Historical"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
