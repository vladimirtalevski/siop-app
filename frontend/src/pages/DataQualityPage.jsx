import { useState, useEffect } from "react";
import { fetchDataQuality } from "../api";

function fmt(n) {
  if (n == null) return "—";
  const v = parseFloat(n);
  if (isNaN(v)) return n;
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return v.toLocaleString();
}

function pctColor(pct) {
  const v = parseFloat(pct);
  if (isNaN(v)) return "#6b7280";
  if (v >= 90) return "#22c55e";
  if (v >= 70) return "#f59e0b";
  return "#ef4444";
}

function StatusDot({ pct }) {
  const color = pctColor(pct);
  return <span style={{ display: "inline-block", width: 10, height: 10, borderRadius: "50%", background: color, marginRight: 6 }} />;
}

function Card({ title, children, accent }) {
  return (
    <div style={{
      background: "#1f2937", border: `1px solid ${accent || "#374151"}`,
      borderRadius: 12, padding: "20px 24px", marginBottom: 20,
    }}>
      <div style={{ fontWeight: 700, fontSize: 15, color: "#f9fafb", marginBottom: 16 }}>{title}</div>
      {children}
    </div>
  );
}

function Table({ rows, cols }) {
  if (!rows || rows.length === 0) return <div style={{ color: "#6b7280", fontSize: 13 }}>No data</div>;
  const keys = cols || Object.keys(rows[0]);
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr>
            {keys.map(k => (
              <th key={k} style={{ padding: "6px 12px", color: "#60a5fa", fontWeight: 600, textAlign: "left", borderBottom: "1px solid #374151", whiteSpace: "nowrap" }}>
                {k.replace(/_/g, " ").toUpperCase()}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "#111827" : "#1f2937" }}>
              {keys.map(k => {
                const v = row[k];
                const isNum = typeof v === "number";
                const isPct = k.includes("pct");
                return (
                  <td key={k} style={{
                    padding: "6px 12px", whiteSpace: "nowrap",
                    color: isPct ? pctColor(v) : isNum ? "#a7f3d0" : "#d1d5db",
                    textAlign: isNum ? "right" : "left",
                    fontWeight: isPct ? 700 : 400,
                  }}>
                    {isPct ? `${v}%` : isNum ? fmt(v) : String(v ?? "—")}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ScoreCard({ label, value, pct, sub }) {
  const color = pctColor(pct);
  return (
    <div style={{ background: "#111827", border: "1px solid #374151", borderRadius: 10, padding: "16px 20px", minWidth: 160 }}>
      <div style={{ fontSize: 11, color: "#6b7280", fontWeight: 600, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700, color }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

export default function DataQualityPage() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchDataQuality()
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return (
    <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
      Running data quality checks…
    </div>
  );

  if (error) return (
    <div style={{ padding: 40, textAlign: "center", color: "#ef4444" }}>Error: {error}</div>
  );

  if (!data) return null;

  // Compute summary scores
  const priceScores = data.missing_price || [];
  const avgPriced = priceScores.length
    ? priceScores.reduce((s, r) => s + parseFloat(r.pct_priced || 0), 0) / priceScores.length
    : 0;
  const supplierScores = data.missing_supplier || [];
  const avgSupplier = supplierScores.length
    ? supplierScores.reduce((s, r) => s + parseFloat(r.pct_with_supplier || 0), 0) / supplierScores.length
    : 0;
  const shipdateScores = data.missing_shipdate || [];
  const avgShipdate = shipdateScores.length
    ? shipdateScores.reduce((s, r) => s + parseFloat(r.pct_complete || 0), 0) / shipdateScores.length
    : 0;
  const totalRows = (data.mart_counts || []).reduce((s, r) => s + (r.rows || 0), 0);
  const orphanTotal = (data.orphan_items || []).reduce((s, r) => s + (r.orphan_items || 0), 0);

  return (
    <div style={{ padding: "24px 32px", fontFamily: "inherit" }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: "#f9fafb" }}>Data Quality Report</h1>
        <p style={{ margin: "4px 0 0", color: "#9ca3af", fontSize: 13 }}>
          KU sign-off checklist · Source: MotherDuck siop_db · {new Date().toLocaleDateString()}
        </p>
      </div>

      {/* Summary scorecards */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
        <ScoreCard label="TOTAL ROWS LOADED" value={fmt(totalRows)} pct={100} sub="across all marts" />
        <ScoreCard label="ITEMS WITH PRICE" value={`${avgPriced.toFixed(0)}%`} pct={avgPriced} sub="avg across companies" />
        <ScoreCard label="ITEMS WITH SUPPLIER" value={`${avgSupplier.toFixed(0)}%`} pct={avgSupplier} sub="avg across companies" />
        <ScoreCard label="PO LINES WITH DATE" value={`${avgShipdate.toFixed(0)}%`} pct={avgShipdate} sub="open POs confirmed" />
        <ScoreCard label="ORPHAN ITEMS" value={fmt(orphanTotal)} pct={orphanTotal === 0 ? 100 : 50} sub="on-hand not in master" />
      </div>

      {/* Mart row counts */}
      <Card title="Mart Row Counts — Data Loaded into MotherDuck" accent="#2563eb33">
        <Table rows={data.mart_counts} cols={["mart", "rows", "companies"]} />
      </Card>

      {/* Company coverage */}
      <Card title="Company Coverage — On-Hand Inventory" accent="#7c3aed33">
        <Table rows={data.company_coverage} cols={["company", "items", "total_available", "total_on_hand"]} />
      </Card>

      {/* On-hand value */}
      <Card title="On-Hand Value by Company" accent="#059669333">
        <Table rows={data.onhand_value} cols={["company", "items_with_stock", "total_qty", "priced_lines", "unpriced_lines", "total_value_local"]} />
      </Card>

      {/* Missing price */}
      <Card title="Completeness — Items with Price" accent={avgPriced >= 90 ? "#22c55e33" : "#ef444433"}>
        <div style={{ marginBottom: 12, fontSize: 13, color: "#9ca3af" }}>
          Items missing a standard cost price cannot be valued in inventory reports.
        </div>
        <Table rows={data.missing_price} cols={["company", "total_items", "items_with_price", "missing_price", "pct_priced"]} />
      </Card>

      {/* Missing supplier */}
      <Card title="Completeness — Items with Primary Supplier" accent={avgSupplier >= 90 ? "#22c55e33" : "#f59e0b33"}>
        <div style={{ marginBottom: 12, fontSize: 13, color: "#9ca3af" }}>
          Items without a primary supplier cannot be automatically reordered or expedited.
        </div>
        <Table rows={data.missing_supplier} cols={["company", "total_items", "items_with_supplier", "missing_supplier", "pct_with_supplier"]} />
      </Card>

      {/* Missing ship date */}
      <Card title="Completeness — Open PO Lines with Confirmed Ship Date" accent={avgShipdate >= 70 ? "#22c55e33" : "#ef444433"}>
        <div style={{ marginBottom: 12, fontSize: 13, color: "#9ca3af" }}>
          Open PO lines without a confirmed ship date cannot be planned or expedited accurately.
        </div>
        <Table rows={data.missing_shipdate} cols={["company", "total_po_lines", "with_confirmed_date", "missing_date", "pct_complete"]} />
      </Card>

      {/* Orphan items */}
      <Card title="Consistency — On-Hand Items Not in Item Master" accent={orphanTotal === 0 ? "#22c55e33" : "#ef444433"}>
        <div style={{ marginBottom: 12, fontSize: 13, color: "#9ca3af" }}>
          Items with physical stock but no master data record — indicates D365 data integrity issues.
        </div>
        <Table rows={data.orphan_items} cols={["company", "onhand_items", "matched_in_master", "orphan_items"]} />
      </Card>

      {/* Freshness */}
      <Card title="Data Freshness — Date Range per Mart" accent="#374151">
        <div style={{ marginBottom: 12, fontSize: 13, color: "#9ca3af" }}>
          Days since latest record — should be within the last 7 days for operational data.
        </div>
        <Table rows={data.freshness} cols={["mart", "oldest_record", "newest_record", "days_since_latest"]} />
      </Card>
    </div>
  );
}
