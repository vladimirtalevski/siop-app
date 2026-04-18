import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell, PieChart, Pie, Legend } from "recharts";
import { fetchSlowMoving, fetchCompanies } from "../api";

function fmt(n) {
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(1);
}

const g = (r, ...keys) => { for (const k of keys) if (r[k] != null) return r[k]; return null; };

const CATEGORY_COLORS = {
  "Never moved":       "#f87171",  // red
  "Non-moving (>1yr)": "#fb923c",  // orange
  "Slow-moving":       "#fbbf24",  // amber
};

const CATEGORY_BADGE = {
  "Never moved":       "badge-red",
  "Non-moving (>1yr)": "badge-amber",
  "Slow-moving":       "badge-amber",
};

export default function SlowMovingPage() {
  const [rows, setRows] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => { fetchCompanies().then(c => setCompanies(c)); }, []);

  useEffect(() => {
    setLoading(true);
    const params = {};
    if (company) params.company = company;
    fetchSlowMoving(params)
      .then(r => setRows(r))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [company]);

  const filtered = categoryFilter
    ? rows.filter(r => g(r, "movement_category", "MOVEMENT_CATEGORY") === categoryFilter)
    : rows;

  // KPIs
  const neverMoved    = rows.filter(r => g(r, "movement_category", "MOVEMENT_CATEGORY") === "Never moved").length;
  const nonMoving     = rows.filter(r => g(r, "movement_category", "MOVEMENT_CATEGORY") === "Non-moving (>1yr)").length;
  const slowMoving    = rows.filter(r => g(r, "movement_category", "MOVEMENT_CATEGORY") === "Slow-moving").length;
  const totalValueAt  = rows.reduce((s, r) => s + (parseFloat(g(r, "onhand_value_local", "ONHAND_VALUE_LOCAL")) || 0), 0);
  const totalOnHand   = rows.reduce((s, r) => s + (parseFloat(g(r, "on_hand_qty", "ON_HAND_QTY")) || 0), 0);

  // Pie chart data
  const pieData = [
    { name: "Never moved",       value: neverMoved,  color: "#f87171" },
    { name: "Non-moving (>1yr)", value: nonMoving,   color: "#fb923c" },
    { name: "Slow-moving",       value: slowMoving,  color: "#fbbf24" },
  ].filter(d => d.value > 0);

  // Bar: value at risk by company
  const valueByCompany = rows.reduce((acc, r) => {
    const c = g(r, "company", "COMPANY");
    if (!acc[c]) acc[c] = { company: c, value: 0, count: 0 };
    acc[c].value += parseFloat(g(r, "onhand_value_local", "ONHAND_VALUE_LOCAL")) || 0;
    acc[c].count += 1;
    return acc;
  }, {});
  const barData = Object.values(valueByCompany).sort((a, b) => b.value - a.value);

  return (
    <div>
      <div className="page-header">
        <h1>Slow-Moving Items</h1>
        <p>Items with no or limited physical inventory movement — value tied up in stock</p>
      </div>

      <div className="filters">
        <label>Company:</label>
        <select value={company} onChange={e => setCompany(e.target.value)}>
          <option value="">All Companies</option>
          {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
        </select>

        <label style={{ marginLeft: 16 }}>Category:</label>
        <select value={categoryFilter} onChange={e => setCategoryFilter(e.target.value)}>
          <option value="">All Categories</option>
          <option value="Never moved">Never moved</option>
          <option value="Non-moving (>1yr)">Non-moving (&gt;1yr)</option>
          <option value="Slow-moving">Slow-moving</option>
        </select>
      </div>

      {loading && <div className="loading">Analysing inventory movement...</div>}
      {error && <div className="error">Error: {error}</div>}

      {!loading && !error && (
        <>
          <div className="kpi-row">
            <div className="kpi-card red">
              <div className="label">Never Moved</div>
              <div className="value">{neverMoved.toLocaleString()}</div>
              <div className="sub">no physical movement ever</div>
            </div>
            <div className="kpi-card amber">
              <div className="label">Non-Moving (&gt;1yr)</div>
              <div className="value">{nonMoving.toLocaleString()}</div>
              <div className="sub">last move over 1 year ago</div>
            </div>
            <div className="kpi-card" style={{ borderColor: "#fbbf24" }}>
              <div className="label">Slow-Moving</div>
              <div className="value">{slowMoving.toLocaleString()}</div>
              <div className="sub">180d+ idle or &lt;3 tx/year</div>
            </div>
            <div className="kpi-card blue">
              <div className="label">Value at Risk (Local)</div>
              <div className="value">{fmt(totalValueAt)}</div>
              <div className="sub">on-hand × standard cost</div>
            </div>
            <div className="kpi-card green">
              <div className="label">Total Qty at Risk</div>
              <div className="value">{fmt(totalOnHand)}</div>
              <div className="sub">physical units tied up</div>
            </div>
          </div>

          <div className="chart-grid">
            <div className="chart-card">
              <h3>Items by Category</h3>
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80} label={({ name, value }) => `${name}: ${value}`}>
                    {pieData.map((d, i) => <Cell key={i} fill={d.color} />)}
                  </Pie>
                  <Tooltip contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }} />
                </PieChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-card">
              <h3>Value at Risk by Company (Local CCY)</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={barData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis dataKey="company" tick={{ fill: "#64748b", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v) => [fmt(v), "Value (Local)"]}
                  />
                  <Bar dataKey="value" name="Value at Risk" fill="#fb923c" radius={[4,4,0,0]} />
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
                  <th>Description</th>
                  <th>Warehouse</th>
                  <th>Category</th>
                  <th>Last Move</th>
                  <th>Days Idle</th>
                  <th>Tx / 365d</th>
                  <th>On-Hand Qty</th>
                  <th>Available Qty</th>
                  <th>Unit Price</th>
                  <th>Value at Risk</th>
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 500).map((r, i) => {
                  const cat      = g(r, "movement_category", "MOVEMENT_CATEGORY") || "";
                  const days     = parseInt(g(r, "days_since_last_move", "DAYS_SINCE_LAST_MOVE")) || 0;
                  const daysCls  = days > 365 ? "#f87171" : days > 180 ? "#fb923c" : "#fbbf24";
                  const value    = parseFloat(g(r, "onhand_value_local", "ONHAND_VALUE_LOCAL")) || 0;
                  return (
                    <tr key={i}>
                      <td><span className="badge badge-blue">{g(r, "company", "COMPANY")}</span></td>
                      <td style={{ fontFamily: "monospace", fontSize: 11 }}>{g(r, "item_id", "ITEM_ID")}</td>
                      <td style={{ maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "#94a3b8", fontSize: 12 }}>
                        {g(r, "part_description", "PART_DESCRIPTION") || "—"}
                      </td>
                      <td style={{ fontSize: 12 }}>{g(r, "warehouse", "WAREHOUSE") || "—"}</td>
                      <td>
                        <span className={`badge ${CATEGORY_BADGE[cat] || "badge-amber"}`} style={{ fontSize: 11 }}>
                          {cat}
                        </span>
                      </td>
                      <td style={{ color: "#64748b", fontSize: 12 }}>
                        {(g(r, "last_physical_move", "LAST_PHYSICAL_MOVE") || "—").slice(0, 10)}
                      </td>
                      <td style={{ color: daysCls, fontWeight: 600 }}>{days || "—"}</td>
                      <td style={{ color: "#94a3b8" }}>{g(r, "tx_last_365_days", "TX_LAST_365_DAYS") ?? "—"}</td>
                      <td>{fmt(g(r, "on_hand_qty", "ON_HAND_QTY"))}</td>
                      <td>{fmt(g(r, "available_qty", "AVAILABLE_QTY"))}</td>
                      <td style={{ fontSize: 12 }}>{fmt(g(r, "unit_price", "UNIT_PRICE"))}</td>
                      <td style={{ fontWeight: 600, color: value > 0 ? "#fb923c" : "#64748b" }}>
                        {fmt(value)}
                      </td>
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
