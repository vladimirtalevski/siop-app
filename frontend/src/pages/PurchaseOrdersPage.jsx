import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, PieChart, Pie, Cell } from "recharts";
import { fetchOpenPOs, fetchCompanies } from "../api";

function fmt(n) {
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(0);
}

function fmtDate(d) {
  if (!d) return "—";
  const s = String(d).slice(0, 10);
  return s === "1900-01-01" ? "—" : s;
}

function g(r, ...keys) {
  for (const k of keys) if (r[k] != null && r[k] !== "") return r[k];
  return null;
}

const STATUS_BADGE = {
  "Invoiced": "badge-blue", "Received": "badge-green",
  "Open order": "badge-amber", "Canceled": "badge-red",
};
const MOC_COLORS = {
  Value: "#60a5fa", Pump: "#34d399", DOE: "#a78bfa",
  Cyclone: "#f59e0b", Manifold: "#f87171", Spiral: "#22d3ee", Other: "#64748b",
};

export default function PurchaseOrdersPage() {
  const [rows, setRows] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [mocFilter, setMocFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => { fetchCompanies().then(setCompanies); }, []);

  useEffect(() => {
    setLoading(true);
    fetchOpenPOs()
      .then(setRows)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const displayed = rows.filter(r => {
    if (company) {
      const c = String(g(r, "company", "COMPANY") || "").toLowerCase();
      if (c !== company.toLowerCase()) return false;
    }
    if (mocFilter && g(r, "moc_group", "MOC_GROUP") !== mocFilter) return false;
    if (statusFilter && g(r, "po_status", "PO_STATUS") !== statusFilter) return false;
    return true;
  });

  // KPIs
  const totalValue = displayed.reduce((s, r) => s + (parseFloat(g(r, "order_price", "ORDER_PRICE")) || 0), 0);
  const uniqueSuppliers = new Set(displayed.map(r => g(r, "supplier_code", "SUPPLIER_CODE")).filter(Boolean)).size;
  const uniquePOs = new Set(displayed.map(r => g(r, "po_number", "PO_NUMBER")).filter(Boolean)).size;

  // Chart: order value by company
  const byCompany = displayed.reduce((acc, r) => {
    const c = String(g(r, "company", "COMPANY") || "?").toUpperCase();
    if (!acc[c]) acc[c] = { company: c, value: 0, lines: 0 };
    acc[c].value += parseFloat(g(r, "order_price", "ORDER_PRICE")) || 0;
    acc[c].lines += 1;
    return acc;
  }, {});
  const companyChart = Object.values(byCompany).sort((a, b) => b.value - a.value);

  // MOC group pie
  const mocCounts = displayed.reduce((acc, r) => {
    const v = g(r, "moc_group", "MOC_GROUP") || "Other";
    acc[v] = (acc[v] || 0) + 1;
    return acc;
  }, {});
  const mocPie = Object.entries(mocCounts).map(([name, value]) => ({ name, value }));

  // Unique MOC groups and statuses for filter dropdowns
  const allMocGroups = [...new Set(rows.map(r => g(r, "moc_group", "MOC_GROUP")).filter(Boolean))].sort();
  const allStatuses = [...new Set(rows.map(r => g(r, "po_status", "PO_STATUS")).filter(Boolean))].sort();

  return (
    <div>
      <div className="page-header">
        <h1>Purchase Orders — OTS Supplier Data</h1>
        <p>On-Time Shipping tracking with MOC groups, lead times &amp; incoterms</p>
      </div>

      <div className="kpi-row">
        <div className="kpi-card amber">
          <div className="label">Total Order Value</div>
          <div className="value">${fmt(totalValue)}</div>
          <div className="sub">filtered lines</div>
        </div>
        <div className="kpi-card blue">
          <div className="label">Unique POs</div>
          <div className="value">{uniquePOs}</div>
          <div className="sub">purchase orders</div>
        </div>
        <div className="kpi-card green">
          <div className="label">Unique Suppliers</div>
          <div className="value">{uniqueSuppliers}</div>
          <div className="sub">active vendors</div>
        </div>
        <div className="kpi-card">
          <div className="label">PO Lines</div>
          <div className="value">{displayed.length}</div>
          <div className="sub">after filters</div>
        </div>
      </div>

      <div className="filters">
        <label>Entity:</label>
        <select value={company} onChange={e => setCompany(e.target.value)}>
          <option value="">All Entities</option>
          {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
        </select>

        <label>MOC Group:</label>
        <select value={mocFilter} onChange={e => setMocFilter(e.target.value)}>
          <option value="">All Groups</option>
          {allMocGroups.map(m => <option key={m} value={m}>{m}</option>)}
        </select>

        <label>Status:</label>
        <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
          <option value="">All Statuses</option>
          {allStatuses.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      {loading && <div className="loading">Loading purchase orders...</div>}
      {error && <div className="error">Error: {error}</div>}

      {!loading && !error && (
        <>
          <div className="chart-grid" style={{ marginBottom: 24 }}>
            <div className="chart-card">
              <h3>Order Value by Entity</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={companyChart}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis dataKey="company" tick={{ fill: "#64748b", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={v => ["$" + fmt(v), ""]}
                  />
                  <Bar dataKey="value" name="Order Value" fill="#f59e0b" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-card">
              <h3>MOC Group Distribution</h3>
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Pie data={mocPie} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80}
                    label={({ name, value }) => `${name}: ${value}`} labelLine={false}>
                    {mocPie.map((e, i) => <Cell key={i} fill={MOC_COLORS[e.name] || "#64748b"} />)}
                  </Pie>
                  <Tooltip contentStyle={{ background: "#161b27", border: "1px solid #1e2535" }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Entity</th>
                  <th>PO #</th>
                  <th>Line</th>
                  <th>Item ID</th>
                  <th>Item Name</th>
                  <th>Supplier</th>
                  <th>MOC Group</th>
                  <th>Ordered Qty</th>
                  <th>Unit Price</th>
                  <th>Order Price</th>
                  <th>Currency</th>
                  <th>Req Receipt Date</th>
                  <th>Conf Receipt Date</th>
                  <th>Conf Ship Date</th>
                  <th>SIFOT LT (days)</th>
                  <th>Ship LT (days)</th>
                  <th>Supplier LT (days)</th>
                  <th>Incoterm</th>
                  <th>Status</th>
                  <th>Expedite</th>
                  <th>Site</th>
                </tr>
              </thead>
              <tbody>
                {displayed.slice(0, 500).map((r, i) => {
                  const poStatus = g(r, "po_status", "PO_STATUS") || "";
                  const mocGroup = g(r, "moc_group", "MOC_GROUP") || "Other";
                  return (
                    <tr key={i}>
                      <td><span className="badge badge-blue">{String(g(r,"company","COMPANY")||"").toUpperCase()}</span></td>
                      <td style={{ fontFamily: "monospace", fontSize: 11 }}>{g(r,"po_number","PO_NUMBER")}</td>
                      <td style={{ textAlign: "center" }}>{g(r,"po_line_number","PO_LINE_NUMBER")}</td>
                      <td style={{ fontFamily: "monospace", fontSize: 11 }}>{g(r,"item_id","ITEM_ID")}</td>
                      <td style={{ maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                          title={g(r,"item_name","ITEM_NAME") || ""}>{g(r,"item_name","ITEM_NAME") || "—"}</td>
                      <td style={{ fontSize: 11 }}>{g(r,"supplier_name","SUPPLIER_NAME") || g(r,"supplier_code","SUPPLIER_CODE") || "—"}</td>
                      <td>
                        <span className="badge" style={{ background: MOC_COLORS[mocGroup] + "33", color: MOC_COLORS[mocGroup], border: `1px solid ${MOC_COLORS[mocGroup]}44` }}>
                          {mocGroup}
                        </span>
                      </td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"ordered_qty","ORDERED_QTY"))}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"unit_price","UNIT_PRICE"))}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"order_price","ORDER_PRICE"))}</td>
                      <td>{g(r,"currency","CURRENCY")}</td>
                      <td>{fmtDate(g(r,"requested_receipt_date","REQUESTED_RECEIPT_DATE"))}</td>
                      <td>{fmtDate(g(r,"confirmed_receipt_date","CONFIRMED_RECEIPT_DATE"))}</td>
                      <td>{fmtDate(g(r,"confirmed_ship_date","CONFIRMED_SHIP_DATE"))}</td>
                      <td style={{ textAlign: "right" }}>{g(r,"sifot_lead_time","SIFOT_LEAD_TIME") ?? "—"}</td>
                      <td style={{ textAlign: "right" }}>{g(r,"shipping_lead_time","SHIPPING_LEAD_TIME") ?? "—"}</td>
                      <td style={{ textAlign: "right" }}>{g(r,"supplier_production_lead_time","SUPPLIER_PRODUCTION_LEAD_TIME") ?? "—"}</td>
                      <td style={{ fontSize: 11 }}>{g(r,"incoterm","INCOTERM") || "—"}</td>
                      <td><span className={`badge ${STATUS_BADGE[poStatus] || "badge-blue"}`}>{poStatus || "—"}</span></td>
                      <td style={{ fontSize: 11 }}>{g(r,"expedite_status","EXPEDITE_STATUS") || "—"}</td>
                      <td style={{ fontSize: 11 }}>{g(r,"site_name","SITE_NAME") || "—"}</td>
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
