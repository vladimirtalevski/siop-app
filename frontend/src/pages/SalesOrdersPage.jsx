import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, PieChart, Pie, Cell } from "recharts";
import { fetchSalesOrderLines, fetchCompanies } from "../api";

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

const DIFOT_COLORS = { "ON-TIME": "#34d399", LATE: "#f87171", "N/A": "#64748b" };
const DIFOT_BADGE = { "ON-TIME": "badge-green", LATE: "badge-red", "N/A": "badge-blue" };
const SUPPLIER_TYPE_BADGE = { INTERNAL: "badge-blue", EXTERNAL: "badge-green" };

export default function SalesOrdersPage() {
  const [rows, setRows] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [difotFilter, setDifotFilter] = useState("");
  const [supplierTypeFilter, setSupplierTypeFilter] = useState("");
  const [difotExclusion, setDifotExclusion] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => { fetchCompanies().then(setCompanies); }, []);

  useEffect(() => {
    setLoading(true);
    fetchSalesOrderLines()
      .then(setRows)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const displayed = rows.filter(r => {
    if (company) {
      const c = String(g(r, "company", "COMPANY") || "").toLowerCase();
      if (c !== company.toLowerCase()) return false;
    }
    if (difotFilter && g(r, "line_status", "LINE_STATUS") !== difotFilter) return false;
    if (supplierTypeFilter && g(r, "delivery_supplier_type", "DELIVERY_SUPPLIER_TYPE") !== supplierTypeFilter) return false;
    if (difotExclusion && g(r, "difot_exclusion", "DIFOT_EXCLUSION") !== difotExclusion) return false;
    return true;
  });

  // KPIs
  const totalSOValue = displayed.reduce((s, r) => s + (parseFloat(g(r, "so_line_value", "SO_LINE_VALUE")) || 0), 0);
  const totalInvoiced = displayed.reduce((s, r) => s + (parseFloat(g(r, "invoiced_line_amount", "INVOICED_LINE_AMOUNT")) || 0), 0);
  const onTimeCount = displayed.filter(r => g(r, "line_status", "LINE_STATUS") === "ON-TIME").length;
  const lateCount = displayed.filter(r => g(r, "line_status", "LINE_STATUS") === "LATE").length;
  const includeLines = displayed.filter(r => g(r, "difot_exclusion", "DIFOT_EXCLUSION") === "INCLUDE");
  const difotPct = includeLines.length > 0
    ? Math.round(includeLines.filter(r => g(r, "line_status", "LINE_STATUS") === "ON-TIME").length / includeLines.length * 100)
    : null;

  // Value by company
  const byCompany = displayed.reduce((acc, r) => {
    const c = String(g(r, "company", "COMPANY") || "?").toUpperCase();
    if (!acc[c]) acc[c] = { company: c, so_value: 0, invoiced: 0 };
    acc[c].so_value += parseFloat(g(r, "so_line_value", "SO_LINE_VALUE")) || 0;
    acc[c].invoiced += parseFloat(g(r, "invoiced_line_amount", "INVOICED_LINE_AMOUNT")) || 0;
    return acc;
  }, {});
  const companyChart = Object.values(byCompany).sort((a, b) => b.so_value - a.so_value);

  // DIFOT pie (INCLUDE lines only)
  const difotCounts = includeLines.reduce((acc, r) => {
    const v = g(r, "line_status", "LINE_STATUS") || "N/A";
    acc[v] = (acc[v] || 0) + 1;
    return acc;
  }, {});
  const difotPie = Object.entries(difotCounts).map(([name, value]) => ({ name, value }));

  // Region breakdown
  const byRegion = displayed.reduce((acc, r) => {
    const region = g(r, "destination_region", "DESTINATION_REGION") || "Unknown";
    if (!acc[region]) acc[region] = { region, value: 0 };
    acc[region].value += parseFloat(g(r, "so_line_value", "SO_LINE_VALUE")) || 0;
    return acc;
  }, {});
  const regionChart = Object.values(byRegion).filter(x => x.value > 0).sort((a, b) => b.value - a.value).slice(0, 10);

  return (
    <div>
      <div className="page-header">
        <h1>Sales Order Lines</h1>
        <p>DIFOT tracking, invoicing status &amp; delivery performance — FLSmidth Global</p>
      </div>

      <div className="kpi-row">
        <div className="kpi-card blue">
          <div className="label">Total SO Value</div>
          <div className="value">${fmt(totalSOValue)}</div>
          <div className="sub">open line value</div>
        </div>
        <div className="kpi-card green">
          <div className="label">Invoiced Value</div>
          <div className="value">${fmt(totalInvoiced)}</div>
          <div className="sub">billed to customers</div>
        </div>
        <div className="kpi-card green">
          <div className="label">DIFOT %</div>
          <div className="value">{difotPct !== null ? difotPct + "%" : "—"}</div>
          <div className="sub">include lines only</div>
        </div>
        <div className="kpi-card red">
          <div className="label">Late Lines</div>
          <div className="value">{lateCount}</div>
          <div className="sub">past promised date</div>
        </div>
        <div className="kpi-card amber">
          <div className="label">On-Time Lines</div>
          <div className="value">{onTimeCount}</div>
          <div className="sub">delivered on time</div>
        </div>
      </div>

      <div className="filters">
        <label>Entity:</label>
        <select value={company} onChange={e => setCompany(e.target.value)}>
          <option value="">All Entities</option>
          {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
        </select>

        <label>DIFOT Status:</label>
        <select value={difotFilter} onChange={e => setDifotFilter(e.target.value)}>
          <option value="">All</option>
          <option value="ON-TIME">On-Time</option>
          <option value="LATE">Late</option>
          <option value="N/A">N/A</option>
        </select>

        <label>Delivery Type:</label>
        <select value={supplierTypeFilter} onChange={e => setSupplierTypeFilter(e.target.value)}>
          <option value="">All</option>
          <option value="INTERNAL">Internal</option>
          <option value="EXTERNAL">External</option>
        </select>

        <label>DIFOT Scope:</label>
        <select value={difotExclusion} onChange={e => setDifotExclusion(e.target.value)}>
          <option value="">All</option>
          <option value="INCLUDE">Include Only</option>
          <option value="EXCLUDE">Exclude Only</option>
        </select>
      </div>

      {loading && <div className="loading">Loading sales order lines...</div>}
      {error && <div className="error">Error: {error}</div>}

      {!loading && !error && (
        <>
          <div className="chart-grid" style={{ marginBottom: 24 }}>
            <div className="chart-card">
              <h3>SO Value vs Invoiced by Entity</h3>
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
                  <Bar dataKey="so_value" name="SO Value" fill="#60a5fa" radius={[4,4,0,0]} />
                  <Bar dataKey="invoiced" name="Invoiced" fill="#34d399" radius={[4,4,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-card">
              <h3>DIFOT Status (Include Lines)</h3>
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Pie data={difotPie} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={80}
                    label={({ name, value }) => `${name}: ${value}`} labelLine={false}>
                    {difotPie.map((e, i) => <Cell key={i} fill={DIFOT_COLORS[e.name] || "#64748b"} />)}
                  </Pie>
                  <Tooltip contentStyle={{ background: "#161b27", border: "1px solid #1e2535" }} />
                </PieChart>
              </ResponsiveContainer>
            </div>

            {regionChart.length > 0 && (
              <div className="chart-card">
                <h3>SO Value by Destination Region</h3>
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={regionChart} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                    <XAxis type="number" tick={{ fill: "#64748b", fontSize: 10 }} tickFormatter={fmt} />
                    <YAxis type="category" dataKey="region" tick={{ fill: "#64748b", fontSize: 10 }} width={100} />
                    <Tooltip
                      contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                      formatter={v => ["$" + fmt(v), ""]}
                    />
                    <Bar dataKey="value" name="SO Value" fill="#a78bfa" radius={[0,4,4,0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>

          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Entity</th>
                  <th>SO #</th>
                  <th>Line</th>
                  <th>Item ID</th>
                  <th>Item Name</th>
                  <th>Customer</th>
                  <th>Destination</th>
                  <th>Ordered Qty</th>
                  <th>Delivered Qty</th>
                  <th>Unit Price</th>
                  <th>SO Value</th>
                  <th>Invoiced</th>
                  <th>Currency</th>
                  <th>Promised Date</th>
                  <th>Delivery Date</th>
                  <th>Days Diff</th>
                  <th>DIFOT</th>
                  <th>DIFOT Scope</th>
                  <th>Delivery Type</th>
                  <th>Status</th>
                  <th>Invoice #</th>
                </tr>
              </thead>
              <tbody>
                {displayed.slice(0, 500).map((r, i) => {
                  const lineStatus = g(r, "line_status", "LINE_STATUS") || "N/A";
                  const supplierType = g(r, "delivery_supplier_type", "DELIVERY_SUPPLIER_TYPE") || "";
                  const difotExcl = g(r, "difot_exclusion", "DIFOT_EXCLUSION") || "";
                  const dateDiff = g(r, "date_difference", "DATE_DIFFERENCE");
                  return (
                    <tr key={i}>
                      <td><span className="badge badge-blue">{String(g(r,"company","COMPANY")||"").toUpperCase()}</span></td>
                      <td style={{ fontFamily: "monospace", fontSize: 11 }}>{g(r,"sales_order_id","SALES_ORDER_ID")}</td>
                      <td style={{ textAlign: "center" }}>{g(r,"line_num","LINE_NUM")}</td>
                      <td style={{ fontFamily: "monospace", fontSize: 11 }}>{g(r,"item_id","ITEM_ID")}</td>
                      <td style={{ maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                          title={g(r,"item_name","ITEM_NAME")||""}>{g(r,"item_name","ITEM_NAME")||"—"}</td>
                      <td style={{ fontSize: 11 }}>{g(r,"customer_name","CUSTOMER_NAME")||"—"}</td>
                      <td style={{ fontSize: 11 }}>{g(r,"destination_country","DESTINATION_COUNTRY") || g(r,"destination","DESTINATION") || "—"}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"ordered_qty","ORDERED_QTY"))}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"delivered_qty","DELIVERED_QTY"))}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"unit_price","UNIT_PRICE"))}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"so_line_value","SO_LINE_VALUE"))}</td>
                      <td style={{ textAlign: "right" }}>{fmt(g(r,"invoiced_line_amount","INVOICED_LINE_AMOUNT"))}</td>
                      <td>{g(r,"currency","CURRENCY")}</td>
                      <td>{fmtDate(g(r,"promised_date","PROMISED_DATE"))}</td>
                      <td>{fmtDate(g(r,"delivery_date","DELIVERY_DATE"))}</td>
                      <td style={{ textAlign: "right", color: dateDiff < 0 ? "#f87171" : dateDiff >= 0 ? "#34d399" : "#64748b" }}>
                        {dateDiff != null ? dateDiff : "—"}
                      </td>
                      <td><span className={`badge ${DIFOT_BADGE[lineStatus] || "badge-blue"}`}>{lineStatus}</span></td>
                      <td><span className={`badge ${difotExcl === "INCLUDE" ? "badge-green" : "badge-amber"}`}>{difotExcl || "—"}</span></td>
                      <td><span className={`badge ${SUPPLIER_TYPE_BADGE[supplierType] || "badge-blue"}`}>{supplierType || "—"}</span></td>
                      <td style={{ fontSize: 11 }}>{g(r,"sales_status","SALES_STATUS") || g(r,"doc_status","DOC_STATUS") || "—"}</td>
                      <td style={{ fontSize: 11 }}>{g(r,"invoice_number","INVOICE_NUMBER") || "—"}</td>
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
