import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { fetchInventorySummary, fetchInventory, fetchCompanies } from "../api";

function fmt(n) {
  const num = parseFloat(n);
  if (isNaN(num)) return "—";
  if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + "M";
  if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + "K";
  return num.toFixed(1);
}

function fmtVal(n, prefix) {
  const num = parseFloat(n);
  if (isNaN(num) || num === 0) return "—";
  if (Math.abs(num) >= 1e6) return `${prefix} ${(num / 1e6).toFixed(2)}M`;
  if (Math.abs(num) >= 1e3) return `${prefix} ${(num / 1e3).toFixed(1)}K`;
  return `${prefix} ${num.toFixed(0)}`;
}

const g = (r, ...keys) => { for (const k of keys) if (r[k] != null) return r[k]; return null; };

const MTS_MTO_COLORS = { MTS: "badge-blue", MTO: "badge-green", UNMAPPED: "badge-amber" };
const SOURCE_COLORS = { Purchase: "badge-blue", Production: "badge-green", Transfer: "badge-amber", Kanban: "badge-red" };

export default function InventoryPage() {
  const [rows, setRows] = useState([]);
  const [companies, setCompanies] = useState([]);
  const [company, setCompany] = useState("");
  const [mtsFilter, setMtsFilter] = useState("");
  const [valueCurrency, setValueCurrency] = useState("dkk");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchCompanies().then(c => setCompanies(c));
  }, []);

  useEffect(() => {
    setLoading(true);
    const params = { limit: 500 };
    if (company) params.company = company;
    fetchInventory(params)
      .then(r => setRows(r))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [company]);

  const filtered = mtsFilter ? rows.filter(r => g(r, "mts_mto", "MTS_MTO") === mtsFilter) : rows;

  // Chart: on-hand value by company
  const valueByCompany = filtered.reduce((acc, r) => {
    const c = g(r, "company", "COMPANY");
    if (!acc[c]) acc[c] = { company: c, value_dkk: 0, value_local: 0, available_qty: 0 };
    acc[c].value_dkk   += parseFloat(g(r, "onhand_value_dkk",   "ONHAND_VALUE_DKK"))   || 0;
    acc[c].value_local += parseFloat(g(r, "onhand_value_local", "ONHAND_VALUE_LOCAL")) || 0;
    acc[c].available_qty += parseFloat(g(r, "available_qty", "AVAILABLE_QTY")) || 0;
    return acc;
  }, {});
  const chartData = Object.values(valueByCompany);

  // KPIs
  const totalDKK      = filtered.reduce((s, r) => s + (parseFloat(g(r, "onhand_value_dkk",   "ONHAND_VALUE_DKK"))   || 0), 0);
  const totalLocal    = filtered.reduce((s, r) => s + (parseFloat(g(r, "onhand_value_local", "ONHAND_VALUE_LOCAL")) || 0), 0);
  const totalOnHand   = filtered.reduce((s, r) => s + (parseFloat(g(r, "on_hand_qty",  "ON_HAND_QTY"))  || 0), 0);
  const totalAvail    = filtered.reduce((s, r) => s + (parseFloat(g(r, "available_qty", "AVAILABLE_QTY")) || 0), 0);
  const totalReserved = filtered.reduce((s, r) => s + (parseFloat(g(r, "reserved_qty", "RESERVED_QTY"))  || 0), 0);
  const pricedCount   = filtered.filter(r => (g(r, "has_item_price", "HAS_ITEM_PRICE") || "") === "OK").length;

  const valueLabel = valueCurrency === "dkk" ? "DKK" : "Local CCY";
  const showValue  = valueCurrency === "dkk" ? totalDKK : totalLocal;

  return (
    <div>
      <div className="page-header">
        <h1>Inventory</h1>
        <p>On-hand stock with item master, planning parameters, and FX valuation</p>
      </div>

      <div className="filters">
        <label>Company:</label>
        <select value={company} onChange={e => setCompany(e.target.value)}>
          <option value="">All Companies</option>
          {companies.map(c => <option key={c} value={c}>{c.toUpperCase()}</option>)}
        </select>

        <label style={{ marginLeft: 16 }}>MTS / MTO:</label>
        <select value={mtsFilter} onChange={e => setMtsFilter(e.target.value)}>
          <option value="">All</option>
          <option value="MTS">MTS</option>
          <option value="MTO">MTO</option>
          <option value="UNMAPPED">Unmapped</option>
        </select>

        <label style={{ marginLeft: 16 }}>Value:</label>
        <div style={{ display: "flex", gap: 4 }}>
          {["dkk", "local"].map(v => (
            <button key={v} onClick={() => setValueCurrency(v)} style={{
              padding: "4px 14px", borderRadius: 6, border: "none", cursor: "pointer", fontSize: 13,
              background: valueCurrency === v ? "#3b82f6" : "#1e2535",
              color: valueCurrency === v ? "#fff" : "#94a3b8",
            }}>{v === "dkk" ? "DKK" : "Local CCY"}</button>
          ))}
        </div>
      </div>

      {loading && <div className="loading">Loading inventory...</div>}
      {error && <div className="error">Error: {error}</div>}

      {!loading && !error && (
        <>
          <div className="kpi-row">
            <div className="kpi-card blue">
              <div className="label">On-Hand Value ({valueLabel})</div>
              <div className="value">{fmt(showValue)}</div>
              <div className="sub">{valueCurrency === "dkk" ? "FX converted" : "local currency"}</div>
            </div>
            <div className="kpi-card green">
              <div className="label">On-Hand Qty</div>
              <div className="value">{fmt(totalOnHand)}</div>
              <div className="sub">physical stock</div>
            </div>
            <div className="kpi-card amber">
              <div className="label">Available Qty</div>
              <div className="value">{fmt(totalAvail)}</div>
              <div className="sub">on-hand minus reserved</div>
            </div>
            <div className="kpi-card red">
              <div className="label">Reserved Qty</div>
              <div className="value">{fmt(totalReserved)}</div>
              <div className="sub">physically reserved</div>
            </div>
            <div className="kpi-card">
              <div className="label">Priced Lines</div>
              <div className="value">{pricedCount} / {filtered.length}</div>
              <div className="sub">with active cost</div>
            </div>
          </div>

          <div className="chart-grid full" style={{ marginBottom: 24 }}>
            <div className="chart-card">
              <h3>On-Hand Value by Company ({valueLabel})</h3>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e2535" />
                  <XAxis dataKey="company" tick={{ fill: "#64748b", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 11 }} tickFormatter={fmt} />
                  <Tooltip
                    contentStyle={{ background: "#161b27", border: "1px solid #1e2535", borderRadius: 8 }}
                    labelStyle={{ color: "#94a3b8" }}
                    formatter={(v) => [fmt(v), ""]}
                  />
                  <Bar dataKey={valueCurrency === "dkk" ? "value_dkk" : "value_local"}
                       name={`Value (${valueLabel})`} fill="#60a5fa" radius={[4,4,0,0]} />
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
                  <th>Site</th>
                  <th>Warehouse</th>
                  <th>MTS/MTO</th>
                  <th>Source</th>
                  <th>On-Hand</th>
                  <th>Available</th>
                  <th>Reserved</th>
                  <th>On Order</th>
                  <th>Unit Price</th>
                  <th>Value ({valueLabel})</th>
                  <th>Lead Time</th>
                  <th>MOQ</th>
                  <th>Buyer Group</th>
                  <th>Price</th>
                </tr>
              </thead>
              <tbody>
                {filtered.slice(0, 300).map((r, i) => {
                  const mts     = g(r, "mts_mto",      "MTS_MTO")      || "UNMAPPED";
                  const src     = g(r, "source_type",  "SOURCE_TYPE")  || "";
                  const ccy     = g(r, "local_currency","LOCAL_CURRENCY") || "";
                  const price   = g(r, "unit_price",   "UNIT_PRICE");
                  const valDKK  = g(r, "onhand_value_dkk",   "ONHAND_VALUE_DKK");
                  const valLoc  = g(r, "onhand_value_local",  "ONHAND_VALUE_LOCAL");
                  const showV   = valueCurrency === "dkk" ? valDKK : valLoc;
                  const prefix  = valueCurrency === "dkk" ? "DKK" : ccy;
                  const hasPx   = g(r, "has_item_price", "HAS_ITEM_PRICE") || "";
                  return (
                    <tr key={i}>
                      <td><span className="badge badge-blue">{g(r, "company", "COMPANY")}</span></td>
                      <td style={{ fontFamily: "monospace", fontSize: 11 }}>{g(r, "item_id", "ITEM_ID")}</td>
                      <td style={{ maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "#94a3b8", fontSize: 12 }}>
                        {g(r, "part_description", "PART_DESCRIPTION") || "—"}
                      </td>
                      <td>{g(r, "site", "SITE")}</td>
                      <td style={{ fontSize: 11 }}>{g(r, "warehouse_id", "WAREHOUSE_ID") || g(r, "warehouse_location", "WAREHOUSE_LOCATION") || "—"}</td>
                      <td><span className={`badge ${MTS_MTO_COLORS[mts] || "badge-amber"}`}>{mts}</span></td>
                      <td><span className={`badge ${SOURCE_COLORS[src] || ""}`} style={{ fontSize: 11 }}>{src || "—"}</span></td>
                      <td>{fmt(g(r, "on_hand_qty",   "ON_HAND_QTY"))}</td>
                      <td style={{ color: parseFloat(g(r, "available_qty", "AVAILABLE_QTY")) > 0 ? "#34d399" : "#f87171" }}>
                        {fmt(g(r, "available_qty", "AVAILABLE_QTY"))}
                      </td>
                      <td>{fmt(g(r, "reserved_qty", "RESERVED_QTY"))}</td>
                      <td>{fmt(g(r, "on_order_qty", "ON_ORDER_QTY"))}</td>
                      <td style={{ fontSize: 12 }}>{ccy} {fmt(price)}</td>
                      <td style={{ fontWeight: 600 }}>{fmtVal(showV, prefix)}</td>
                      <td>{g(r, "lead_time", "LEAD_TIME") ?? "—"}</td>
                      <td>{fmt(g(r, "min_order_qty", "MIN_ORDER_QTY"))}</td>
                      <td style={{ fontSize: 11, color: "#94a3b8" }}>{g(r, "buyer_group", "BUYER_GROUP") || "—"}</td>
                      <td>
                        <span className={`badge ${hasPx === "OK" ? "badge-green" : "badge-red"}`} style={{ fontSize: 10 }}>
                          {hasPx === "OK" ? "OK" : "No Price"}
                        </span>
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
