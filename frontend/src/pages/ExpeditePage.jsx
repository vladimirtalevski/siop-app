import { useState, useEffect, useMemo } from "react";
import { fetchExpedite } from "../api";

const ACTION_CONFIG = {
  "Expedite Shortage": { bg: "#fee2e2", color: "#991b1b", border: "#fca5a5" },
  "ROP Shortage":      { bg: "#ffedd5", color: "#9a3412", border: "#fdba74" },
  "Decrease":          { bg: "#fef9c3", color: "#854d0e", border: "#fde047" },
  "Cancel":            { bg: "#ede9fe", color: "#5b21b6", border: "#c4b5fd" },
  "No Action":         { bg: "transparent", color: "#6b7280", border: "transparent" },
};

const REF_TYPE_COLOR = {
  "On-hand":        "#16a34a",
  "Purchase order": "#2563eb",
  "Sales order":    "#dc2626",
  "Production":     "#7c3aed",
  "Production line":"#9333ea",
  "BOM line":       "#0891b2",
  "Transfer Order": "#0d9488",
  "Safety stock":   "#6b7280",
};

const EXPEDITE_STATUS_LABELS = {
  ARQ: "Advance Request", AFRQ: "Advance Firm Req", ERQ: "Expedite Request",
  RRQ: "Rush Request", TRQ: "Transfer Request", T: "Transit",
  C: "Cancelled", O: "Open", W: "Waiting", FLSD: "FLS Delayed",
  SSD: "Supplier Delayed", OT: "On Track",
};

function fmtDate(d) {
  if (!d) return "—";
  return String(d).slice(0, 10);
}

function fmtQty(n) {
  const v = parseFloat(n);
  if (isNaN(v)) return "—";
  return v.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function AccBadge({ value, minOnHand, maxOnHand }) {
  const v = parseFloat(value);
  const min = parseFloat(minOnHand) || 0;
  const max = parseFloat(maxOnHand) || 0;
  if (isNaN(v)) return <span style={{ color: "#9ca3af" }}>—</span>;
  let color = "#22c55e";
  if (v < 0) color = "#ef4444";
  else if (min > 0 && v < min) color = "#f97316";
  else if (max > 0 && v > max) color = "#a855f7";
  return <span style={{ fontWeight: 600, color }}>{fmtQty(v)}</span>;
}

export default function ExpeditePage() {
  const [company, setCompany] = useState("US2");
  const [warehouse, setWarehouse] = useState("T01");
  const [itemSearch, setItemSearch] = useState("");
  const [actionFilter, setActionFilter] = useState("all");
  const [refTypeFilter, setRefTypeFilter] = useState("all");
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [staticMode, setStaticMode] = useState(false);
  const [expandedItems, setExpandedItems] = useState(new Set());

  const load = () => {
    setLoading(true);
    setError(null);
    fetchExpedite({ company, warehouse })
      .then((data) => {
        if (data?.static_mode) { setStaticMode(true); return; }
        setRows(Array.isArray(data) ? data : []);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const filtered = useMemo(() => {
    return rows.filter((r) => {
      if (itemSearch && !r.ITEMID?.toLowerCase().includes(itemSearch.toLowerCase())) return false;
      if (actionFilter !== "all" && r.ACTION_STATUS !== actionFilter) return false;
      if (refTypeFilter !== "all" && r.REFERENCE_TYPE !== refTypeFilter) return false;
      return true;
    });
  }, [rows, itemSearch, actionFilter, refTypeFilter]);

  const actionCounts = useMemo(() => {
    const counts = {};
    rows.forEach((r) => {
      const a = r.ACTION_STATUS || "No Action";
      counts[a] = (counts[a] || 0) + 1;
    });
    return counts;
  }, [rows]);

  const urgentItems = useMemo(() => {
    return [...new Set(
      rows.filter((r) => r.ACTION_STATUS === "Expedite Shortage" || r.ACTION_STATUS === "ROP Shortage")
          .map((r) => r.ITEMID)
    )].length;
  }, [rows]);

  const toggleItem = (itemId) => {
    setExpandedItems((prev) => {
      const next = new Set(prev);
      if (next.has(itemId)) next.delete(itemId);
      else next.add(itemId);
      return next;
    });
  };

  const expandAll = () => setExpandedItems(new Set(rows.map((r) => r.ITEMID)));
  const collapseAll = () => setExpandedItems(new Set());

  // Group rows by item for collapsible view
  const grouped = useMemo(() => {
    const map = new Map();
    filtered.forEach((r) => {
      if (!map.has(r.ITEMID)) map.set(r.ITEMID, []);
      map.get(r.ITEMID).push(r);
    });
    return map;
  }, [filtered]);

  if (staticMode) {
    return (
      <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
        <div style={{ fontSize: 32, marginBottom: 12 }}>⚡</div>
        <div style={{ fontSize: 18, fontWeight: 600, color: "#374151", marginBottom: 8 }}>
          Expedite Report requires a live backend connection
        </div>
        <div>Run locally with <code style={{ background: "#f3f4f6", padding: "2px 6px", borderRadius: 4 }}>start.bat</code> to use this feature.</div>
      </div>
    );
  }

  return (
    <div style={{ padding: "24px 32px", fontFamily: "inherit" }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: "#f9fafb" }}>Expedite Report</h1>
        <p style={{ margin: "4px 0 0", color: "#9ca3af", fontSize: 13 }}>
          MRP Net Requirements · Action-Status v60 · Warehouse: {warehouse}
        </p>
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 20, alignItems: "flex-end" }}>
        <div>
          <label style={{ display: "block", fontSize: 11, color: "#9ca3af", marginBottom: 4 }}>COMPANY</label>
          <select value={company} onChange={(e) => setCompany(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 13 }}>
            <option value="US2">US2 — USA</option>
            <option value="ZA4">ZA4 — South Africa (Wadeville)</option>
            <option value="ZA3">ZA3 — South Africa (Stormill)</option>
            <option value="DK1">DK1 — Denmark</option>
          </select>
        </div>
        <div>
          <label style={{ display: "block", fontSize: 11, color: "#9ca3af", marginBottom: 4 }}>WAREHOUSE</label>
          <input value={warehouse} onChange={(e) => setWarehouse(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 13, width: 80 }} />
        </div>
        <div>
          <label style={{ display: "block", fontSize: 11, color: "#9ca3af", marginBottom: 4 }}>ITEM ID</label>
          <input placeholder="Search item..." value={itemSearch} onChange={(e) => setItemSearch(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 13, width: 150 }} />
        </div>
        <div>
          <label style={{ display: "block", fontSize: 11, color: "#9ca3af", marginBottom: 4 }}>ACTION STATUS</label>
          <select value={actionFilter} onChange={(e) => setActionFilter(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 13 }}>
            <option value="all">All</option>
            {Object.keys(ACTION_CONFIG).map((a) => (
              <option key={a} value={a}>{a} {actionCounts[a] ? `(${actionCounts[a]})` : ""}</option>
            ))}
          </select>
        </div>
        <div>
          <label style={{ display: "block", fontSize: 11, color: "#9ca3af", marginBottom: 4 }}>REF TYPE</label>
          <select value={refTypeFilter} onChange={(e) => setRefTypeFilter(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 13 }}>
            <option value="all">All</option>
            {Object.keys(REF_TYPE_COLOR).map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <button onClick={load} disabled={loading}
          style={{ background: "#2563eb", color: "#fff", border: "none", borderRadius: 6, padding: "7px 18px", fontSize: 13, cursor: "pointer", fontWeight: 600 }}>
          {loading ? "Loading…" : "Run"}
        </button>
        <button onClick={expandAll} style={{ background: "#374151", color: "#d1d5db", border: "none", borderRadius: 6, padding: "7px 12px", fontSize: 12, cursor: "pointer" }}>Expand All</button>
        <button onClick={collapseAll} style={{ background: "#374151", color: "#d1d5db", border: "none", borderRadius: 6, padding: "7px 12px", fontSize: 12, cursor: "pointer" }}>Collapse All</button>
      </div>

      {error && (
        <div style={{ background: "#fef2f2", border: "1px solid #fca5a5", color: "#991b1b", borderRadius: 8, padding: "12px 16px", marginBottom: 16 }}>
          Error: {error}
        </div>
      )}

      {/* KPI Cards */}
      <div style={{ display: "flex", gap: 12, marginBottom: 24, flexWrap: "wrap" }}>
        {[
          { label: "Expedite Shortage", key: "Expedite Shortage", icon: "🚨" },
          { label: "ROP Shortage", key: "ROP Shortage", icon: "⚠️" },
          { label: "Decrease", key: "Decrease", icon: "📉" },
          { label: "Cancel", key: "Cancel", icon: "🚫" },
        ].map(({ label, key, icon }) => {
          const cfg = ACTION_CONFIG[key];
          const cnt = actionCounts[key] || 0;
          return (
            <div key={key}
              onClick={() => setActionFilter(actionFilter === key ? "all" : key)}
              style={{
                background: cnt > 0 ? cfg.bg : "#1f2937",
                border: `1px solid ${cnt > 0 ? cfg.border : "#374151"}`,
                borderRadius: 10, padding: "14px 20px", minWidth: 150, cursor: "pointer",
                opacity: actionFilter !== "all" && actionFilter !== key ? 0.5 : 1,
                transition: "opacity 0.15s",
              }}>
              <div style={{ fontSize: 20 }}>{icon}</div>
              <div style={{ fontSize: 28, fontWeight: 700, color: cnt > 0 ? cfg.color : "#6b7280", marginTop: 4 }}>{cnt}</div>
              <div style={{ fontSize: 11, color: cnt > 0 ? cfg.color : "#9ca3af", fontWeight: 600, marginTop: 2 }}>{label}</div>
            </div>
          );
        })}
        <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: "14px 20px", minWidth: 150 }}>
          <div style={{ fontSize: 20 }}>🔥</div>
          <div style={{ fontSize: 28, fontWeight: 700, color: urgentItems > 0 ? "#ef4444" : "#6b7280", marginTop: 4 }}>{urgentItems}</div>
          <div style={{ fontSize: 11, color: "#9ca3af", fontWeight: 600, marginTop: 2 }}>URGENT ITEMS</div>
        </div>
        <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: "14px 20px", minWidth: 150 }}>
          <div style={{ fontSize: 20 }}>📋</div>
          <div style={{ fontSize: 28, fontWeight: 700, color: "#f9fafb", marginTop: 4 }}>{grouped.size}</div>
          <div style={{ fontSize: 11, color: "#9ca3af", fontWeight: 600, marginTop: 2 }}>ITEMS IN PLAN</div>
        </div>
      </div>

      {/* Loading state */}
      {loading && (
        <div style={{ textAlign: "center", padding: 40, color: "#6b7280" }}>
          Running MRP query… this may take 30–60 seconds.
        </div>
      )}

      {/* Grouped table */}
      {!loading && grouped.size > 0 && (
        <div style={{ overflowX: "auto" }}>
          {[...grouped.entries()].map(([itemId, itemRows]) => {
            const isExpanded = expandedItems.has(itemId);
            const worstAction = itemRows.reduce((best, r) => {
              const priority = { "Expedite Shortage": 4, "ROP Shortage": 3, "Decrease": 2, "Cancel": 1, "No Action": 0 };
              return (priority[r.ACTION_STATUS] || 0) > (priority[best] || 0) ? r.ACTION_STATUS : best;
            }, "No Action");
            const cfg = ACTION_CONFIG[worstAction] || ACTION_CONFIG["No Action"];
            const firstRow = itemRows[0];
            const onHand = itemRows.find((r) => r.REFERENCE_TYPE === "On-hand");
            const poCount = itemRows.filter((r) => r.REFERENCE_TYPE === "Purchase order").length;
            const soCount = itemRows.filter((r) => r.REFERENCE_TYPE === "Sales order").length;
            const firstShortage = firstRow.FIRST_SHORTAGE_DATE;

            return (
              <div key={itemId} style={{ marginBottom: 4, border: `1px solid ${worstAction !== "No Action" ? cfg.border : "#374151"}`, borderRadius: 8, overflow: "hidden" }}>
                {/* Item header row */}
                <div
                  onClick={() => toggleItem(itemId)}
                  style={{
                    display: "flex", alignItems: "center", gap: 16, padding: "10px 16px",
                    background: worstAction !== "No Action" ? cfg.bg : "#1f2937",
                    cursor: "pointer", userSelect: "none",
                  }}>
                  <span style={{ color: "#9ca3af", fontSize: 14, width: 16 }}>{isExpanded ? "▼" : "▶"}</span>
                  <span style={{ fontWeight: 700, color: "#f9fafb", fontSize: 14, minWidth: 120 }}>{itemId}</span>
                  <span style={{ color: "#9ca3af", fontSize: 12, flex: 1 }}>
                    {itemRows.length} lines · {poCount} POs · {soCount} SOs
                    {firstShortage && firstShortage !== "null" && (
                      <span style={{ color: "#ef4444", marginLeft: 8 }}>· First shortage: {fmtDate(firstShortage)}</span>
                    )}
                  </span>
                  {firstRow.ITEMBUYERGROUPID && (
                    <span style={{ fontSize: 11, color: "#9ca3af", background: "#111827", borderRadius: 4, padding: "2px 6px" }}>
                      {firstRow.ITEMBUYERGROUPID}
                    </span>
                  )}
                  {worstAction !== "No Action" && (
                    <span style={{
                      fontSize: 11, fontWeight: 700, color: cfg.color,
                      background: cfg.bg, border: `1px solid ${cfg.border}`,
                      borderRadius: 4, padding: "2px 8px",
                    }}>{worstAction}</span>
                  )}
                  {onHand && (
                    <span style={{ fontSize: 12, color: "#22c55e", fontWeight: 600 }}>
                      OH: {fmtQty(onHand.QTY)}
                    </span>
                  )}
                </div>

                {/* Expanded lines */}
                {isExpanded && (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                      <thead>
                        <tr style={{ background: "#111827" }}>
                          {["Type", "Req Date", "Conf Ship", "Qty", "Prev Acc", "Accumulated", "Min", "Max", "Action", "Ref / PO / SO", "Vendor / Customer", "Expedite Status", "Expeditor", "Notes"].map((h) => (
                            <th key={h} style={{ padding: "6px 10px", color: "#6b7280", fontWeight: 600, textAlign: "left", whiteSpace: "nowrap", borderBottom: "1px solid #374151" }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {itemRows.map((r, i) => {
                          const acfg = ACTION_CONFIG[r.ACTION_STATUS] || ACTION_CONFIG["No Action"];
                          const rowBg = r.ACTION_STATUS !== "No Action" ? acfg.bg : i % 2 === 0 ? "#1f2937" : "#111827";
                          const refColor = REF_TYPE_COLOR[r.REFERENCE_TYPE] || "#9ca3af";
                          return (
                            <tr key={i} style={{ background: rowBg, borderBottom: "1px solid #1f2937" }}>
                              <td style={{ padding: "6px 10px", whiteSpace: "nowrap" }}>
                                <span style={{ color: refColor, fontWeight: 600, fontSize: 11 }}>{r.REFERENCE_TYPE}</span>
                              </td>
                              <td style={{ padding: "6px 10px", whiteSpace: "nowrap", color: "#d1d5db" }}>{fmtDate(r.REQ_DATE)}</td>
                              <td style={{ padding: "6px 10px", whiteSpace: "nowrap", color: "#9ca3af" }}>{fmtDate(r.CONFIRMEDSHIPDATE || r.CONFIRMED_RECEIPT_DATE)}</td>
                              <td style={{ padding: "6px 10px", textAlign: "right", color: parseFloat(r.QTY) < 0 ? "#f87171" : "#a7f3d0" }}>
                                {fmtQty(r.QTY)}
                              </td>
                              <td style={{ padding: "6px 10px", textAlign: "right" }}>
                                <AccBadge value={r.PREVACCUMULATED} minOnHand={r.MIN_ON_HAND} maxOnHand={r.MAX_ON_HAND} />
                              </td>
                              <td style={{ padding: "6px 10px", textAlign: "right" }}>
                                <AccBadge value={r.ACCUMULATED} minOnHand={r.MIN_ON_HAND} maxOnHand={r.MAX_ON_HAND} />
                              </td>
                              <td style={{ padding: "6px 10px", textAlign: "right", color: "#6b7280" }}>{fmtQty(r.MIN_ON_HAND)}</td>
                              <td style={{ padding: "6px 10px", textAlign: "right", color: "#6b7280" }}>{fmtQty(r.MAX_ON_HAND)}</td>
                              <td style={{ padding: "6px 10px", whiteSpace: "nowrap" }}>
                                {r.ACTION_STATUS && r.ACTION_STATUS !== "No Action" ? (
                                  <span style={{
                                    fontSize: 10, fontWeight: 700, color: acfg.color,
                                    background: acfg.bg, border: `1px solid ${acfg.border}`,
                                    borderRadius: 3, padding: "1px 5px",
                                  }}>{r.ACTION_STATUS}</span>
                                ) : <span style={{ color: "#4b5563" }}>—</span>}
                              </td>
                              <td style={{ padding: "6px 10px", whiteSpace: "nowrap", color: "#93c5fd", fontFamily: "monospace", fontSize: 11 }}>
                                {r.REFID || "—"}
                                {r.LINENUMBER ? <span style={{ color: "#6b7280" }}> #{r.LINENUMBER}</span> : ""}
                              </td>
                              <td style={{ padding: "6px 10px", color: "#d1d5db", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {r.VENDORNAME || r.CUSTOMERNAME || "—"}
                              </td>
                              <td style={{ padding: "6px 10px", whiteSpace: "nowrap" }}>
                                {r.EXPEDITE_STATUS ? (
                                  <span style={{ fontSize: 10, background: "#374151", color: "#d1d5db", borderRadius: 3, padding: "1px 5px", fontFamily: "monospace" }}>
                                    {r.EXPEDITE_STATUS}
                                    <span style={{ color: "#6b7280", marginLeft: 4 }}>{EXPEDITE_STATUS_LABELS[r.EXPEDITE_STATUS] || ""}</span>
                                  </span>
                                ) : <span style={{ color: "#4b5563" }}>—</span>}
                              </td>
                              <td style={{ padding: "6px 10px", color: "#9ca3af", fontSize: 11 }}>{r.EXPEDITOR || "—"}</td>
                              <td style={{ padding: "6px 10px", color: "#9ca3af", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {r.NOTES || "—"}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {!loading && rows.length > 0 && filtered.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: "#6b7280" }}>No rows match the current filters.</div>
      )}
      {!loading && rows.length === 0 && !error && !staticMode && (
        <div style={{ textAlign: "center", padding: 40, color: "#6b7280" }}>Click Run to load the expedite report.</div>
      )}
    </div>
  );
}
