import { useState, useEffect, useMemo } from "react";
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Cell, Legend,
} from "recharts";
import { fetchSpeedUp, fetchSpeedUpQuality, fetchCrossSite, fetchPurchasedNotUsed } from "../api";

// difot_status values from SQL: 'DIFOT' | 'Late' | 'Partial' | 'Past Due' | 'Open'
const STATUS_COLORS = {
  DIFOT: "#22c55e",
  Late: "#ef4444",
  Partial: "#f59e0b",
  "Past Due": "#f97316",
  Open: "#3b82f6",
};

const REGIONS = ['NAMER', 'SAMER', 'ECANA', 'SSAMESA', 'APAC'];
const REGION_MAP = {
  'Tucson': 'NAMER', 'US Consignment': 'NAMER', 'Evansville': 'NAMER',
  'Renca': 'SAMER', 'Lima': 'SAMER', 'La Negra': 'SAMER', 'CL (La Negra)': 'SAMER', 'PE (Lima)': 'SAMER',
  'AU (Welshpool)': 'APAC', 'Welshpool': 'APAC', 'ID (Surabaya)': 'APAC', 'Surabaya': 'APAC',
  'Pinkenba': 'APAC', 'Jakarta': 'APAC', 'Beresfield': 'APAC',
  'Arakonam': 'SSAMESA', 'Stormil': 'SSAMESA', 'Storm Mill': 'SSAMESA',
};
const LINE_COLORS = ['#3b82f6','#22c55e','#f59e0b','#ef4444','#a855f7','#ec4899','#14b8a6','#f97316','#06b6d4','#84cc16','#fb923c'];

function median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function getRegion(company) { return REGION_MAP[company] || 'Other'; }
function ltColor(v) { if (v == null || isNaN(v)) return '#6b7280'; if (v <= 30) return '#22c55e'; if (v <= 60) return '#f59e0b'; return '#ef4444'; }
function evColor(v) { if (v == null) return '#6b7280'; const a = Math.abs(v); if (a < 15) return '#22c55e'; if (a < 30) return '#f59e0b'; return '#ef4444'; }

function KpiMatrixPanel({ title, legend, byLoc, valueKey, colorFn, formatFn }) {
  const byRegion = {};
  REGIONS.forEach(r => { byRegion[r] = []; });
  byLoc.forEach(d => { const r = getRegion(d.loc); if (byRegion[r]) byRegion[r].push(d); });
  const maxRows = Math.max(...REGIONS.map(r => byRegion[r].length), 1);
  return (
    <div style={{ background: '#111827', border: '1px solid #374151', borderRadius: 10, padding: 14 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#d1d5db', marginBottom: 6 }}>{title}</div>
      <div style={{ display: 'flex', gap: 10, marginBottom: 8, flexWrap: 'wrap' }}>
        {legend.map(l => (
          <span key={l.label} style={{ fontSize: 9, color: '#9ca3af', display: 'flex', alignItems: 'center', gap: 3 }}>
            <span style={{ width: 7, height: 7, borderRadius: '50%', background: l.color, display: 'inline-block' }} />{l.label}
          </span>
        ))}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: `repeat(${REGIONS.length}, 1fr)`, gap: 4 }}>
        {REGIONS.map(r => (
          <div key={r} style={{ fontSize: 9, fontWeight: 700, color: '#6b7280', textAlign: 'center', borderBottom: '1px solid #374151', paddingBottom: 4, marginBottom: 2 }}>{r}</div>
        ))}
        {Array.from({ length: maxRows }, (_, i) =>
          REGIONS.map(r => {
            const d = byRegion[r][i];
            if (!d) return <div key={`${r}-${i}`} />;
            const v = d[valueKey];
            const c = colorFn(v);
            return (
              <div key={`${r}-${i}`} style={{ background: c + '25', border: `1px solid ${c}55`, borderRadius: 6, padding: '5px 4px', textAlign: 'center', marginBottom: 4 }}>
                <div style={{ fontSize: 9, color: '#d1d5db', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{d.loc}</div>
                <div style={{ fontSize: 14, fontWeight: 800, color: c }}>{formatFn(v)}</div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

function fmtMoney(n) {
  if (n == null || isNaN(n)) return "—";
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(1) + "B DKK";
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + "M DKK";
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(0) + "K DKK";
  return n.toFixed(0) + " DKK";
}

function pct(n, d, digits = 1) {
  if (!d) return "—";
  return ((n / d) * 100).toFixed(digits) + "%";
}

function difotColor(v) {
  const n = parseFloat(v);
  if (isNaN(n)) return "#6b7280";
  if (n >= 80) return "#22c55e";
  if (n >= 50) return "#f59e0b";
  return "#ef4444";
}

function KpiCard({ label, value, sub, color }) {
  return (
    <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: "20px 24px", flex: 1, minWidth: 150 }}>
      <div style={{ fontSize: 10, color: "#6b7280", fontWeight: 700, letterSpacing: 1, marginBottom: 6, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 32, fontWeight: 800, color: color || "#f9fafb", lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#6b7280", marginTop: 5 }}>{sub}</div>}
    </div>
  );
}

function QualityBar({ label, pctOk, total, missing }) {
  const c = pctOk >= 90 ? "#22c55e" : pctOk >= 70 ? "#f59e0b" : "#ef4444";
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
        <span style={{ fontSize: 12, color: "#d1d5db" }}>{label}</span>
        <span style={{ fontSize: 12, color: c, fontWeight: 600 }}>{pctOk.toFixed(1)}%</span>
      </div>
      <div style={{ background: "#374151", borderRadius: 4, height: 7, overflow: "hidden" }}>
        <div style={{ width: `${Math.min(pctOk, 100)}%`, height: "100%", background: c, borderRadius: 4 }} />
      </div>
      <div style={{ fontSize: 10, color: "#6b7280", marginTop: 2 }}>{missing} missing of {total} total</div>
    </div>
  );
}

function Th({ children, right }) {
  return (
    <th style={{ padding: "9px 12px", color: "#6b7280", fontWeight: 700, fontSize: 11, textAlign: right ? "right" : "left", borderBottom: "1px solid #374151", whiteSpace: "nowrap", background: "#111827" }}>
      {children}
    </th>
  );
}
function Td({ children, color, right, bold }) {
  return (
    <td style={{ padding: "8px 12px", color: color || "#d1d5db", textAlign: right ? "right" : "left", fontWeight: bold ? 700 : 400, fontSize: 13, whiteSpace: "nowrap" }}>
      {children}
    </td>
  );
}

export default function SpeedUpDashboard() {
  const [tab, setTab] = useState("scorecard");
  const [spSortCol, setSpSortCol] = useState("pastDueCount");
  const [spSortDir, setSpSortDir] = useState(-1);
  const [companyFilter, setCompanyFilter] = useState("all");
  const [erpFilter, setErpFilter] = useState("all");
  const [yearFilter, setYearFilter] = useState("all");
  const [gvOrderType, setGvOrderType] = useState("all");
  const [gvStockNonStock, setGvStockNonStock] = useState("all");
  const [gvProductType, setGvProductType] = useState("all");
  const [gvExecType, setGvExecType] = useState("all");
  const [gvBusinessLine, setGvBusinessLine] = useState("all");
  const [gvOfferingType, setGvOfferingType] = useState("all");
  const [gvProductLine, setGvProductLine] = useState("all");
  const [gvReference, setGvReference] = useState("all");
  const [gvCustomer, setGvCustomer] = useState("all");
  const [rows, setRows] = useState([]);
  const [quality, setQuality] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Cross-site state — lazy loaded when tab is first opened
  const [csRows, setCsRows] = useState([]);
  const [csLoading, setCsLoading] = useState(false);
  const [csERP, setCsERP] = useState("all");
  const [csCoverage, setCsCoverage] = useState("all");
  const [csStatus, setCsStatus] = useState("Past Due");
  const [csStockERP, setCsStockERP] = useState("all");
  const [csStockCountry, setCsStockCountry] = useState("all");
  const [csDemandSite, setCsDemandSite] = useState("all");
  const [csProductFamily, setCsProductFamily] = useState("all");
  const [csWarehouse, setCsWarehouse] = useState("all");
  const [csItemSearch, setCsItemSearch] = useState("");
  const [csMinValue, setCsMinValue] = useState("");
  const [csSortCol, setCsSortCol] = useState("value_dkk_k");
  const [csSortDir, setCsSortDir] = useState(-1);
  const [csPage, setCsPage] = useState(0);
  const CS_PAGE_SIZE = 50;

  // Purchased-but-not-used state — lazy loaded when tab is first opened
  const [pnuRows, setPnuRows] = useState([]);
  const [pnuLoading, setPnuLoading] = useState(false);
  const [pnuCompany, setPnuCompany] = useState("all");
  const [pnuReqGroup, setPnuReqGroup] = useState("all");
  const [pnuErpSource, setPnuErpSource] = useState("all");
  const [pnuItemSearch, setPnuItemSearch] = useState("");
  const [pnuMinValue, setPnuMinValue] = useState("");
  const [pnuSortCol, setPnuSortCol] = useState("stock_value_dkk");
  const [pnuSortDir, setPnuSortDir] = useState(-1);
  const [pnuPage, setPnuPage] = useState(0);
  const PNU_PAGE_SIZE = 50;

  useEffect(() => {
    setLoading(true);
    Promise.all([fetchSpeedUp(), fetchSpeedUpQuality()])
      .then(([d, q]) => { setRows(Array.isArray(d) ? d : []); setQuality(Array.isArray(q) ? q : []); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (tab === "cross-site" && csRows.length === 0 && !csLoading) {
      setCsLoading(true);
      fetchCrossSite()
        .then((d) => setCsRows(Array.isArray(d) ? d : []))
        .catch(console.error)
        .finally(() => setCsLoading(false));
    }
  }, [tab]);

  useEffect(() => {
    if (tab === "purchased-not-used" && pnuRows.length === 0 && !pnuLoading) {
      setPnuLoading(true);
      fetchPurchasedNotUsed()
        .then((d) => setPnuRows(Array.isArray(d) ? d : []))
        .catch(console.error)
        .finally(() => setPnuLoading(false));
    }
  }, [tab]);

  // Enrich: lead_time_days now comes from SQL; add computed flags
  const enriched = useMemo(() => rows.map((r) => ({
    ...r,
    is_difot: r.difot_status === "DIFOT",
    is_shipped: r.goods_issue_date != null,
    val: parseFloat(r.line_value_dkk ?? r.line_value) || 0,
    lt: parseFloat(r.lead_time_days),
    ev: (r.goods_issue_date && r.confirmed_ship_date)
      ? Math.round((new Date(r.goods_issue_date) - new Date(r.confirmed_ship_date)) / 86400000)
      : null,
  })), [rows]);

  const companies = useMemo(() => [...new Set(enriched.map((r) => r.company))].filter(Boolean).sort(), [enriched]);
  const erpSources = useMemo(() => [...new Set(enriched.map((r) => r.erp_source))].filter(Boolean).sort(), [enriched]);
  const years = useMemo(() => [...new Set(enriched.map((r) => r.created_date?.slice(0, 4)).filter(Boolean))].sort().reverse(), [enriched]);
  const filtered = useMemo(() => {
    let f = companyFilter === "all" ? enriched : enriched.filter((r) => r.company === companyFilter);
    if (erpFilter !== "all") f = f.filter((r) => r.erp_source === erpFilter);
    return f;
  }, [enriched, companyFilter, erpFilter]);
  const filteredGV = useMemo(() => {
    let f = yearFilter === "all" ? filtered : filtered.filter((r) => r.created_date?.startsWith(yearFilter));
    if (gvOrderType !== "all") f = f.filter((r) => r.sales_status === gvOrderType);
    if (gvStockNonStock !== "all") f = f.filter((r) => r.stock_non_stock === gvStockNonStock);
    if (gvProductType !== "all") f = f.filter((r) => r.product_typology === gvProductType);
    if (gvExecType !== "all") f = f.filter((r) => r.execution_type === gvExecType);
    if (gvBusinessLine !== "all") f = f.filter((r) => r.business_line === gvBusinessLine);
    if (gvOfferingType !== "all") f = f.filter((r) => r.offering_type === gvOfferingType);
    if (gvProductLine !== "all") f = f.filter((r) => r.product_line_name === gvProductLine);
    if (gvReference !== "all") f = f.filter((r) => r.reference === gvReference);
    if (gvCustomer !== "all") f = f.filter((r) => r.customer_name === gvCustomer);
    return f;
  }, [filtered, yearFilter, gvOrderType, gvStockNonStock, gvProductType, gvExecType, gvBusinessLine, gvOfferingType, gvProductLine, gvReference, gvCustomer]);

  const gvOrderTypes = useMemo(() => [...new Set(enriched.map((r) => r.sales_status).filter(Boolean))].sort(), [enriched]);
  const gvStockNonStocks = useMemo(() => [...new Set(enriched.map((r) => r.stock_non_stock).filter(Boolean))].sort(), [enriched]);
  const gvProductTypes = useMemo(() => [...new Set(enriched.map((r) => r.product_typology).filter(v => v && v !== 'Unknown'))].sort(), [enriched]);
  const gvExecTypes = useMemo(() => [...new Set(enriched.map((r) => r.execution_type).filter(Boolean))].sort(), [enriched]);
  const gvBusinessLines = useMemo(() => [...new Set(enriched.map((r) => r.business_line).filter(Boolean))].sort(), [enriched]);
  const gvOfferingTypes = useMemo(() => [...new Set(enriched.map((r) => r.offering_type).filter(Boolean))].sort(), [enriched]);
  const gvProductLines = useMemo(() => [...new Set(enriched.map((r) => r.product_line_name).filter(Boolean))].sort(), [enriched]);
  const gvReferences = useMemo(() => [...new Set(enriched.map((r) => r.reference).filter(Boolean))].sort(), [enriched]);
  const gvCustomers = useMemo(() => [...new Set(enriched.map((r) => r.customer_name).filter(Boolean))].sort(), [enriched]);

  // --- Global KPIs ---
  const kpi = useMemo(() => {
    const total = filtered.length;
    const difot = filtered.filter((r) => r.is_difot).length;
    const totalVal = filtered.reduce((s, r) => s + r.val, 0);
    const difotVal = filtered.filter((r) => r.is_difot).reduce((s, r) => s + r.val, 0);
    const withIssue = filtered.filter((r) => r.is_shipped).length;
    const ltRows = filtered.filter((r) => !isNaN(r.lt) && r.lt >= 0);
    const avgLt = ltRows.length ? ltRows.reduce((s, r) => s + r.lt, 0) / ltRows.length : null;
    const byLoc = {};
    filtered.forEach((r) => {
      if (!byLoc[r.company]) byLoc[r.company] = { total: 0, difot: 0 };
      byLoc[r.company].total++; if (r.is_difot) byLoc[r.company].difot++;
    });
    const locArr = Object.entries(byLoc).filter(([, v]) => v.total >= 50).map(([k, v]) => ({ loc: k, p: (v.difot / v.total) * 100 }));
    const best = [...locArr].sort((a, b) => b.p - a.p)[0];
    const worst = [...locArr].sort((a, b) => a.p - b.p)[0];
    return { total, difot, totalVal, difotVal, withIssue, avgLt, best, worst };
  }, [filtered]);

  // --- By location ---
  const byLocation = useMemo(() => {
    const map = {};
    filtered.forEach((r) => {
      const k = r.company || "Unknown";
      if (!map[k]) map[k] = { loc: k, total: 0, difot: 0, totalVal: 0, difotVal: 0, ltSum: 0, ltCnt: 0 };
      map[k].total++; map[k].totalVal += r.val;
      if (r.is_difot) { map[k].difot++; map[k].difotVal += r.val; }
      if (!isNaN(r.lt) && r.lt >= 0) { map[k].ltSum += r.lt; map[k].ltCnt++; }
    });
    return Object.values(map).map((d) => ({
      ...d,
      difot_pct: d.total ? (d.difot / d.total) * 100 : 0,
      difot_val_pct: d.totalVal ? (d.difotVal / d.totalVal) * 100 : 0,
      avg_lt: d.ltCnt ? d.ltSum / d.ltCnt : null,
    })).sort((a, b) => b.total - a.total);
  }, [filtered]);

  // --- Lead time buckets ---
  const ltBuckets = useMemo(() => {
    const total = filtered.length;
    return [
      { label: "≤7 days", min: 0, max: 7 },
      { label: "8–14 days", min: 8, max: 14 },
      { label: "15–21 days", min: 15, max: 21 },
      { label: ">21 days", min: 22, max: Infinity },
    ].map((b) => {
      const bRows = filtered.filter((r) => !isNaN(r.lt) && r.lt >= b.min && r.lt <= b.max);
      const difotRows = bRows.filter((r) => r.is_difot);
      const val = bRows.reduce((s, r) => s + r.val, 0);
      const avgDays = bRows.length ? bRows.reduce((s, r) => s + r.lt, 0) / bRows.length : null;
      return { ...b, cnt: bRows.length, mix: total ? (bRows.length / total) * 100 : 0, val, avgDays, difot_pct: bRows.length ? (difotRows.length / bRows.length) * 100 : 0 };
    });
  }, [filtered]);

  // --- Monthly trend ---
  const byMonth = useMemo(() => {
    const map = {};
    filtered.forEach((r) => {
      if (!r.created_date) return;
      const m = r.created_date.slice(0, 7);
      if (!map[m]) map[m] = { month: m, total: 0, difot: 0, val: 0, difotVal: 0 };
      map[m].total++; map[m].val += r.val;
      if (r.is_difot) { map[m].difot++; map[m].difotVal += r.val; }
    });
    return Object.values(map).sort((a, b) => a.month.localeCompare(b.month)).slice(-18).map((m) => ({
      ...m,
      difot_pct: m.total ? Math.round((m.difot / m.total) * 100) : 0,
      difot_val_pct: m.val ? Math.round((m.difotVal / m.val) * 100) : 0,
    }));
  }, [filtered]);

  // --- Auto observations ---
  const observations = useMemo(() => {
    const obs = [];
    const dp = kpi.total ? (kpi.difot / kpi.total) * 100 : 0;
    const dvp = kpi.totalVal ? (kpi.difotVal / kpi.totalVal) * 100 : 0;
    const flagged = byLocation.filter((l) => l.difot_pct < 25 && l.total >= 50).map((l) => `${l.loc} (${l.difot_pct.toFixed(1)}%)`).join(", ");
    obs.push(`DIFOT (LINE COUNT): ${dp.toFixed(1)}% globally. ${kpi.best ? `${kpi.best.loc} leads at ${kpi.best.p.toFixed(1)}%` : ""}${flagged ? `; ${flagged} flagged below 25% — immediate review needed` : ""}.`);
    if (Math.abs(dp - dvp) > 10) obs.push(`DIFOT (VALUE-WEIGHTED): ${dvp.toFixed(1)}% — ${dvp < dp ? "significantly lower" : "higher"} than line-count DIFOT. High-value lines are ${dvp < dp ? "underperforming" : "outperforming"} on on-time delivery.`);
    if (kpi.avgLt) {
      const slowest = [...byLocation].filter((l) => l.avg_lt).sort((a, b) => b.avg_lt - a.avg_lt)[0];
      obs.push(`LEAD TIME (Order-to-Ship): Global avg ${kpi.avgLt.toFixed(1)} days (CreatedDate → Goods Issue Date). ${slowest ? `${slowest.loc} is longest at ${slowest.avg_lt.toFixed(1)}d` : ""}.`);
    }
    const topVal = [...byLocation].sort((a, b) => b.totalVal - a.totalVal)[0];
    if (topVal) obs.push(`VALUE CONCENTRATION: ${topVal.loc} holds ${pct(topVal.totalVal, kpi.totalVal)} of total order value (${fmtMoney(topVal.totalVal)}). Any disruption here has outsized revenue impact.`);
    const noIssue = filtered.filter((r) => !r.is_shipped).length;
    if (noIssue > filtered.length * 0.02) obs.push(`DATA QUALITY: ${noIssue.toLocaleString()} lines (${pct(noIssue, filtered.length)}) have no packing slip / goods issue date — excluded from DIFOT & lead time calculations.`);
    return obs;
  }, [kpi, byLocation, filtered]);

  // --- Global View aggregations ---
  const globalViewData = useMemo(() => {
    const locMap = {};
    filteredGV.forEach((r) => {
      const loc = r.company || 'Unknown';
      if (!locMap[loc]) locMap[loc] = { loc, region: getRegion(loc), total: 0, difot: 0, ltArr: [], evArr: [] };
      locMap[loc].total++;
      if (r.is_difot) locMap[loc].difot++;
      if (!isNaN(r.lt) && r.lt >= 0) locMap[loc].ltArr.push(r.lt);
      if (r.ev != null) locMap[loc].evArr.push(r.ev);
    });
    const byLoc = Object.values(locMap).map((d) => ({
      ...d,
      difot_pct: d.total ? (d.difot / d.total) * 100 : 0,
      avg_lt: d.ltArr.length ? d.ltArr.reduce((s, v) => s + v, 0) / d.ltArr.length : null,
      med_ev: median(d.evArr),
    })).sort((a, b) => b.total - a.total);

    const monthLocMap = {};
    filteredGV.forEach((r) => {
      if (!r.created_date) return;
      const m = r.created_date.slice(0, 7);
      const loc = r.company || 'Unknown';
      const key = `${m}||${loc}`;
      if (!monthLocMap[key]) monthLocMap[key] = { month: m, loc, total: 0, difot: 0, ltArr: [], evArr: [] };
      monthLocMap[key].total++;
      if (r.is_difot) monthLocMap[key].difot++;
      if (!isNaN(r.lt) && r.lt >= 0) monthLocMap[key].ltArr.push(r.lt);
      if (r.ev != null) monthLocMap[key].evArr.push(r.ev);
    });
    const months = [...new Set(Object.values(monthLocMap).map((d) => d.month))].sort().slice(-6);
    const locs = byLoc.slice(0, 11).map((d) => d.loc);
    const makeLineData = (metric) => months.map((m) => {
      const pt = { month: m };
      locs.forEach((loc) => {
        const d = monthLocMap[`${m}||${loc}`];
        if (!d) return;
        if (metric === 'lt') pt[loc] = d.ltArr.length ? Math.round(d.ltArr.reduce((s, v) => s + v, 0) / d.ltArr.length) : null;
        if (metric === 'difot') pt[loc] = d.total ? Math.round((d.difot / d.total) * 100) : null;
        if (metric === 'ev') pt[loc] = d.evArr.length ? Math.round(median(d.evArr)) : null;
      });
      return pt;
    });
    return { byLoc, locs, lineData: { lt: makeLineData('lt'), difot: makeLineData('difot'), ev: makeLineData('ev') } };
  }, [filteredGV]);

  // --- Site Performance data ---
  const siteData = useMemo(() => {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const d30 = new Date(today); d30.setDate(d30.getDate() + 30);
    const d60 = new Date(today); d60.setDate(d60.getDate() + 60);

    // monthly DIFOT last 3 months per site
    const monthlyMap = {};
    enriched.forEach((r) => {
      if (!r.created_date) return;
      const m = r.created_date.slice(0, 7);
      const k = `${r.company}||${m}`;
      if (!monthlyMap[k]) monthlyMap[k] = { total: 0, difot: 0 };
      monthlyMap[k].total++;
      if (r.is_difot) monthlyMap[k].difot++;
    });
    const recentMonths = [...new Set(Object.keys(monthlyMap).map((k) => k.split('||')[1]))].sort().slice(-4);

    const map = {};
    enriched.forEach((r) => {
      const k = r.company || 'Unknown';
      if (!map[k]) map[k] = {
        site: k,
        region: getRegion(k),
        total: 0, difot: 0, ltArr: [], evArr: [],
        pastDueCount: 0, pastDueVal: 0,
        atRisk30: 0, atRisk30Val: 0,
        atRisk60: 0, atRisk60Val: 0,
      };
      const d = map[k];
      d.total++;
      if (r.is_difot) d.difot++;
      if (!isNaN(r.lt) && r.lt >= 0) d.ltArr.push(r.lt);
      if (r.ev != null) d.evArr.push(r.ev);
      if (r.difot_status === 'Past Due') { d.pastDueCount++; d.pastDueVal += r.val; }
      if (!r.goods_issue_date && r.confirmed_ship_date) {
        const csd = new Date(r.confirmed_ship_date);
        if (!isNaN(csd)) {
          if (csd >= today && csd <= d30) { d.atRisk30++; d.atRisk30Val += r.val; }
          if (csd >= today && csd <= d60) { d.atRisk60++; d.atRisk60Val += r.val; }
        }
      }
    });

    const bySite = Object.values(map).map((d) => {
      const difot_pct = d.total ? (d.difot / d.total) * 100 : 0;
      const avg_lt = d.ltArr.length ? d.ltArr.reduce((s, v) => s + v, 0) / d.ltArr.length : null;
      const med_ev = median(d.evArr);

      // MoM trend: compare last 2 of last 4 months
      const m1 = recentMonths[recentMonths.length - 1];
      const m2 = recentMonths[recentMonths.length - 2];
      const cur = monthlyMap[`${d.site}||${m1}`];
      const prev = monthlyMap[`${d.site}||${m2}`];
      const curPct = cur?.total ? (cur.difot / cur.total) * 100 : null;
      const prevPct = prev?.total ? (prev.difot / prev.total) * 100 : null;
      let trend = '→';
      if (curPct != null && prevPct != null) {
        if (curPct - prevPct > 3) trend = '↑';
        else if (prevPct - curPct > 3) trend = '↓';
      }
      const trendColor = trend === '↑' ? '#22c55e' : trend === '↓' ? '#ef4444' : '#6b7280';

      // 3-month declining check
      const [m_2, m_1, m0] = recentMonths.slice(-3);
      const p2 = monthlyMap[`${d.site}||${m_2}`];
      const p1 = monthlyMap[`${d.site}||${m_1}`];
      const p0 = monthlyMap[`${d.site}||${m0}`];
      const pp2 = p2?.total ? (p2.difot / p2.total) * 100 : null;
      const pp1 = p1?.total ? (p1.difot / p1.total) * 100 : null;
      const pp0 = p0?.total ? (p0.difot / p0.total) * 100 : null;
      const declining3m = pp2 != null && pp1 != null && pp0 != null && pp0 < pp1 && pp1 < pp2;

      return { ...d, difot_pct, avg_lt, med_ev, trend, trendColor, declining3m, curPct, prevPct };
    });

    return bySite;
  }, [enriched]);

  // --- Business Line comparison data ---
  const blData = useMemo(() => {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const d30 = new Date(today); d30.setDate(d30.getDate() + 30);

    // monthly DIFOT per BL for trend
    const monthlyBL = {};
    enriched.forEach((r) => {
      const bl = r.business_line || 'Unclassified';
      if (!r.created_date) return;
      const m = r.created_date.slice(0, 7);
      const k = `${bl}||${m}`;
      if (!monthlyBL[k]) monthlyBL[k] = { total: 0, difot: 0 };
      monthlyBL[k].total++;
      if (r.is_difot) monthlyBL[k].difot++;
    });
    const recentMonths = [...new Set(Object.keys(monthlyBL).map((k) => k.split('||')[1]))].sort().slice(-6);

    const map = {};
    enriched.forEach((r) => {
      const bl = r.business_line || 'Unclassified';
      if (!map[bl]) map[bl] = { bl, total: 0, difot: 0, ltArr: [], evArr: [], pastDueCount: 0, pastDueVal: 0, atRisk30: 0, atRisk30Val: 0, totalVal: 0, difotVal: 0 };
      const d = map[bl];
      d.total++; d.totalVal += r.val;
      if (r.is_difot) { d.difot++; d.difotVal += r.val; }
      if (!isNaN(r.lt) && r.lt >= 0) d.ltArr.push(r.lt);
      if (r.ev != null) d.evArr.push(r.ev);
      if (r.difot_status === 'Past Due') { d.pastDueCount++; d.pastDueVal += r.val; }
      if (!r.goods_issue_date && r.confirmed_ship_date) {
        const csd = new Date(r.confirmed_ship_date);
        if (!isNaN(csd) && csd >= today && csd <= d30) { d.atRisk30++; d.atRisk30Val += r.val; }
      }
    });

    // BL × Site matrix
    const blSiteMap = {};
    enriched.forEach((r) => {
      const bl = r.business_line || 'Unclassified';
      const site = r.company || 'Unknown';
      const k = `${bl}||${site}`;
      if (!blSiteMap[k]) blSiteMap[k] = { bl, site, total: 0, difot: 0 };
      blSiteMap[k].total++;
      if (r.is_difot) blSiteMap[k].difot++;
    });

    const byBL = Object.values(map).map((d) => {
      const difot_pct = d.total ? (d.difot / d.total) * 100 : 0;
      const avg_lt = d.ltArr.length ? d.ltArr.reduce((s, v) => s + v, 0) / d.ltArr.length : null;
      const med_ev = median(d.evArr);
      const difot_val_pct = d.totalVal ? (d.difotVal / d.totalVal) * 100 : 0;
      // trend sparkline data
      const trendData = recentMonths.map((m) => {
        const e = monthlyBL[`${d.bl}||${m}`];
        return { month: m.slice(5), pct: e?.total ? Math.round((e.difot / e.total) * 100) : null };
      });
      return { ...d, difot_pct, avg_lt, med_ev, difot_val_pct, trendData };
    }).sort((a, b) => b.total - a.total);

    const blSiteMatrix = Object.values(blSiteMap).map((d) => ({
      ...d, difot_pct: d.total ? (d.difot / d.total) * 100 : 0,
    }));
    const allBLs = byBL.map((d) => d.bl);
    const allSites = [...new Set(blSiteMatrix.map((d) => d.site))].sort();

    return { byBL, blSiteMatrix, allBLs, allSites, recentMonths };
  }, [enriched]);

  // --- Not Moving Items ---
  const notMovingData = useMemo(() => {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const sixMonthsAgo = new Date(today); sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);

    const items = enriched.filter((r) => {
      if (r.goods_issue_date) return false;
      if (!r.created_date) return false;
      const created = new Date(r.created_date);
      return !isNaN(created) && created <= sixMonthsAgo;
    }).map((r) => {
      const created = new Date(r.created_date);
      const daysOpen = Math.floor((today - created) / 86400000);
      let bucket;
      if (daysOpen < 270) bucket = "6–9 months";
      else if (daysOpen < 365) bucket = "9–12 months";
      else if (daysOpen < 540) bucket = "12–18 months";
      else bucket = "18+ months";
      return { ...r, daysOpen, bucket };
    });

    const buckets = ["6–9 months", "9–12 months", "12–18 months", "18+ months"].map((label) => {
      const rows = items.filter((r) => r.bucket === label);
      return { label, count: rows.length, value: rows.reduce((s, r) => s + r.val, 0) };
    });

    const bySite = {};
    items.forEach((r) => {
      const k = r.company || "Unknown";
      if (!bySite[k]) bySite[k] = { site: k, region: getRegion(k), count: 0, value: 0, maxDays: 0 };
      bySite[k].count++;
      bySite[k].value += r.val;
      if (r.daysOpen > bySite[k].maxDays) bySite[k].maxDays = r.daysOpen;
    });

    const byBL = {};
    items.forEach((r) => {
      const k = r.business_line || "Unclassified";
      if (!byBL[k]) byBL[k] = { bl: k, count: 0, value: 0 };
      byBL[k].count++;
      byBL[k].value += r.val;
    });

    const uniqueItems = new Set(items.map((r) => r.item_id).filter(Boolean)).size;
    const totalValue = items.reduce((s, r) => s + r.val, 0);

    return {
      items: items.sort((a, b) => b.daysOpen - a.daysOpen),
      buckets,
      bySite: Object.values(bySite).sort((a, b) => b.count - a.count),
      byBL: Object.values(byBL).sort((a, b) => b.count - a.count),
      totalCount: items.length,
      totalValue,
      uniqueItems,
      sitesAffected: Object.keys(bySite).length,
    };
  }, [enriched]);

  const [nmSortCol, setNmSortCol] = useState("daysOpen");
  const [nmSortDir, setNmSortDir] = useState(-1);
  const [nmPage, setNmPage] = useState(0);
  const NM_PAGE_SIZE = 50;

  const csFiltered = useMemo(() => {
    let f = csRows;
    if (csERP !== "all") f = f.filter((r) => r.demand_erp === csERP);
    if (csCoverage !== "all") f = f.filter((r) => r.coverage_type === csCoverage);
    if (csStatus !== "all") f = f.filter((r) => r.demand_status === csStatus);
    if (csStockERP !== "all") f = f.filter((r) => r.stock_erp === csStockERP);
    if (csStockCountry !== "all") f = f.filter((r) => r.stock_country === csStockCountry);
    if (csWarehouse !== "all") f = f.filter((r) => r.warehouse === csWarehouse);
    if (csDemandSite !== "all") f = f.filter((r) => r.demand_site === csDemandSite);
    if (csProductFamily !== "all") f = f.filter((r) => r.product_family === csProductFamily);
    if (csItemSearch.trim()) {
      const q = csItemSearch.trim().toUpperCase();
      f = f.filter((r) => (r.item_id || "").toUpperCase().includes(q));
    }
    if (csMinValue !== "") f = f.filter((r) => (parseFloat(r.value_dkk_k) || 0) >= parseFloat(csMinValue));
    return f;
  }, [csRows, csERP, csCoverage, csStatus, csStockERP, csStockCountry, csWarehouse, csDemandSite, csProductFamily, csItemSearch, csMinValue]);

  const csStats = useMemo(() => {
    const pdFull = csRows.filter((r) => r.demand_status === "Past Due" && r.coverage_type === "Full Cover");
    const totalVal = csRows.reduce((s, r) => s + (parseFloat(r.value_dkk_k) || 0), 0);
    const pdFullVal = pdFull.reduce((s, r) => s + (parseFloat(r.value_dkk_k) || 0), 0);
    return {
      totalRows: csRows.length,
      totalValM: totalVal / 1000,
      pdFullRows: pdFull.length,
      pdFullValM: pdFullVal / 1000,
      uniqueItems: new Set(csRows.map((r) => r.item_id)).size,
      uniqueDemandSites: new Set(csRows.map((r) => r.demand_site)).size,
    };
  }, [csRows]);

  const csErpChart = useMemo(() => {
    const map = {};
    csRows.forEach((r) => {
      const k = r.demand_erp || "Unknown";
      if (!map[k]) map[k] = { erp: k, "Past Due Full": 0, "Past Due Partial": 0, "Open Full": 0, "Open Partial": 0 };
      const val = parseFloat(r.value_dkk_k) || 0;
      if (r.demand_status === "Past Due" && r.coverage_type === "Full Cover") map[k]["Past Due Full"] += val;
      else if (r.demand_status === "Past Due") map[k]["Past Due Partial"] += val;
      else if (r.coverage_type === "Full Cover") map[k]["Open Full"] += val;
      else map[k]["Open Partial"] += val;
    });
    return Object.values(map);
  }, [csRows]);

  const pnuFiltered = useMemo(() => {
    let f = pnuRows;
    if (pnuCompany !== "all") f = f.filter((r) => r.company === pnuCompany);
    if (pnuReqGroup !== "all") f = f.filter((r) => r.req_group === pnuReqGroup);
    if (pnuErpSource !== "all") f = f.filter((r) => r.erp_source === pnuErpSource);
    if (pnuItemSearch.trim()) {
      const q = pnuItemSearch.trim().toUpperCase();
      f = f.filter((r) => (r.item_id || "").toUpperCase().includes(q) || (r.item_description || "").toUpperCase().includes(q));
    }
    if (pnuMinValue !== "") f = f.filter((r) => (parseFloat(r.stock_value_dkk) || 0) >= parseFloat(pnuMinValue));
    return f;
  }, [pnuRows, pnuCompany, pnuReqGroup, pnuErpSource, pnuItemSearch, pnuMinValue]);

  const pnuStats = useMemo(() => {
    const withStock = pnuRows.filter((r) => parseFloat(r.current_stock) > 0);
    const totalVal = pnuRows.reduce((s, r) => s + (parseFloat(r.stock_value_dkk) || 0), 0);
    const avgDays = pnuRows.length
      ? Math.round(pnuRows.reduce((s, r) => s + (parseFloat(r.days_since_receipt) || 0), 0) / pnuRows.length)
      : 0;
    return {
      totalValM: totalVal / 1e6,
      uniqueItems: new Set(pnuRows.map((r) => r.item_id)).size,
      companies: new Set(pnuRows.map((r) => r.company)).size,
      avgDays,
      withStockCount: withStock.length,
    };
  }, [pnuRows]);

  const pnuCompanyChart = useMemo(() => {
    const map = {};
    pnuRows.forEach((r) => {
      const k = r.company || "Unknown";
      if (!map[k]) map[k] = { company: k, value: 0, items: 0 };
      map[k].value += parseFloat(r.stock_value_dkk) || 0;
      map[k].items++;
    });
    return Object.values(map).sort((a, b) => b.value - a.value);
  }, [pnuRows]);

  const navStyle = (id) => ({
    padding: "8px 18px", borderRadius: 6, border: "none", cursor: "pointer", fontWeight: 600, fontSize: 13,
    background: tab === id ? "#2563eb" : "#1f2937", color: tab === id ? "#fff" : "#9ca3af",
  });

  if (loading) return <div style={{ padding: 60, textAlign: "center", color: "#9ca3af", fontSize: 15 }}>Loading Speed Up Dashboard…</div>;
  if (error) return <div style={{ padding: 60, textAlign: "center", color: "#ef4444" }}>Error: {error}</div>;

  return (
    <div style={{ padding: "24px 32px", fontFamily: "inherit" }}>
      {/* Header */}
      <div style={{ marginBottom: 20 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 800, color: "#f9fafb" }}>SIOP KPI Scorecard — Speed Up</h1>
        <p style={{ margin: "5px 0 0", color: "#6b7280", fontSize: 12, display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <span>{rows.length.toLocaleString()} SO lines</span>
          {erpSources.map((src) => {
            const cnt = enriched.filter((r) => r.erp_source === src).length;
            const color = src === "D365" ? "#3b82f6" : src.startsWith("ORACLE") ? "#a855f7" : "#f59e0b";
            const label = src === "ORACLE_ERP" || src === "ORACLE_CEN01" ? "Oracle CEN01" : src;
            return (
              <span key={src} style={{ background: color + "22", border: `1px solid ${color}55`, borderRadius: 4, padding: "2px 8px", fontSize: 11, color }}>
                {label}: {cnt.toLocaleString()}
              </span>
            );
          })}
        </p>
      </div>

      {/* Sub-nav */}
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        <button style={navStyle("global-view")} onClick={() => setTab("global-view")}>Global View</button>
        <button style={navStyle("site-performance")} onClick={() => setTab("site-performance")}>Site Performance</button>
        <button style={navStyle("business-lines")} onClick={() => setTab("business-lines")}>Business Lines</button>
        <button style={navStyle("bucket_not_moving_items")} onClick={() => { setTab("bucket_not_moving_items"); setNmPage(0); }}>Not Moving Items</button>
        <button style={navStyle("cross-site")} onClick={() => { setTab("cross-site"); setCsPage(0); }}>Cross-Site Opportunity</button>
        <button style={navStyle("purchased-not-used")} onClick={() => { setTab("purchased-not-used"); setPnuPage(0); }}>Purchased Not Used</button>
        <button style={navStyle("scorecard")} onClick={() => setTab("scorecard")}>KPI Scorecard</button>
        <button style={navStyle("trend")} onClick={() => setTab("trend")}>Monthly Trend</button>
        <button style={navStyle("quality")} onClick={() => setTab("quality")}>Data Quality</button>
        <button style={navStyle("definitions")} onClick={() => setTab("definitions")}>Definitions</button>
      </div>

      {/* Filter */}
      <div style={{ display: "flex", gap: 12, marginBottom: 24, alignItems: "flex-end", flexWrap: "wrap" }}>
        <div>
          <label style={{ display: "block", fontSize: 10, color: "#6b7280", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>Turn In Date (Year)</label>
          <select value={yearFilter} onChange={(e) => setYearFilter(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "7px 12px", fontSize: 13 }}>
            <option value="all">All Years</option>
            {years.map((y) => <option key={y} value={y}>{y}</option>)}
          </select>
        </div>
        <div>
          <label style={{ display: "block", fontSize: 10, color: "#6b7280", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>ERP Source</label>
          <select value={erpFilter} onChange={(e) => setErpFilter(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "7px 12px", fontSize: 13 }}>
            <option value="all">All ERP Systems</option>
            {erpSources.map((s) => (
              <option key={s} value={s}>{s === "ORACLE_CEN01" ? "Oracle CEN01" : s}</option>
            ))}
          </select>
        </div>
        <div>
          <label style={{ display: "block", fontSize: 10, color: "#6b7280", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>Location / Company</label>
          <select value={companyFilter} onChange={(e) => setCompanyFilter(e.target.value)}
            style={{ background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "7px 12px", fontSize: 13 }}>
            <option value="all">All Locations</option>
            {companies.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <div style={{ fontSize: 12, color: "#6b7280", alignSelf: "center" }}>
          {filtered.length.toLocaleString()} lines &nbsp;|&nbsp; {fmtMoney(kpi.totalVal)} total value
        </div>
      </div>

      {/* ── GLOBAL VIEW TAB ── */}
      {tab === "global-view" && (
        <>
          {/* Global View filter bar */}
          <div style={{ background: "#111827", border: "1px solid #374151", borderRadius: 10, padding: "12px 16px", marginBottom: 16, display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}>
            {[
              { label: "Turn In Date", value: yearFilter, set: setYearFilter, opts: years, allLabel: "All Years" },
              { label: "Order Type", value: gvOrderType, set: setGvOrderType, opts: gvOrderTypes, allLabel: "(Blank)" },
              { label: "Stock/Non Stock", value: gvStockNonStock, set: setGvStockNonStock, opts: gvStockNonStocks, allLabel: "All" },
              { label: "Product Type", value: gvProductType, set: setGvProductType, opts: gvProductTypes, allLabel: "All" },
              { label: "Execution Type", value: gvExecType, set: setGvExecType, opts: gvExecTypes, allLabel: "All" },
              { label: "Business Line", value: gvBusinessLine, set: setGvBusinessLine, opts: gvBusinessLines, allLabel: "All" },
              { label: "Offering Type", value: gvOfferingType, set: setGvOfferingType, opts: gvOfferingTypes, allLabel: "All" },
              { label: "Product Line", value: gvProductLine, set: setGvProductLine, opts: gvProductLines, allLabel: "All" },
              { label: "Reference", value: gvReference, set: setGvReference, opts: gvReferences, allLabel: "All" },
              { label: "Locations", value: companyFilter, set: setCompanyFilter, opts: companies, allLabel: "All" },
              { label: "Customer", value: gvCustomer, set: setGvCustomer, opts: gvCustomers, allLabel: "All" },
            ].map(({ label, value, set, opts, allLabel }) => (
              <div key={label} style={{ flex: 1, minWidth: 100 }}>
                <label style={{ display: "block", fontSize: 9, color: "#6b7280", marginBottom: 3, textTransform: "uppercase", letterSpacing: 1 }}>{label}</label>
                <select value={value} onChange={(e) => set(e.target.value)}
                  style={{ width: "100%", background: "#1f2937", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "5px 8px", fontSize: 12 }}>
                  <option value="all">{allLabel}</option>
                  {opts.map((o) => <option key={o} value={o}>{o}</option>)}
                </select>
              </div>
            ))}
            <div style={{ textAlign: "right", borderLeft: "1px solid #374151", paddingLeft: 16, minWidth: 70 }}>
              <div style={{ fontSize: 9, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 3 }}>Line Items</div>
              <div style={{ fontSize: 20, fontWeight: 800, color: "#f9fafb" }}>{filteredGV.length.toLocaleString()}</div>
            </div>
          </div>

          {/* KPI Matrix Panels */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 16 }}>
            <KpiMatrixPanel
              title="Order to Ship (Avg)"
              legend={[{ label: "<30", color: "#22c55e" }, { label: ">31 <60", color: "#f59e0b" }, { label: ">61", color: "#ef4444" }]}
              byLoc={globalViewData.byLoc} valueKey="avg_lt" colorFn={ltColor}
              formatFn={(v) => v != null ? v.toFixed(1) : "—"} />
            <KpiMatrixPanel
              title="DIFOT %"
              legend={[{ label: ">80", color: "#22c55e" }, { label: ">60 <79", color: "#f59e0b" }, { label: "<59", color: "#ef4444" }]}
              byLoc={globalViewData.byLoc} valueKey="difot_pct" colorFn={difotColor}
              formatFn={(v) => v != null ? v.toFixed(1) + "%" : "—"} />
            <KpiMatrixPanel
              title="Execution Variance (Median)"
              legend={[{ label: "<|15|", color: "#22c55e" }, { label: "<|30|", color: "#f59e0b" }, { label: ">|30|", color: "#ef4444" }]}
              byLoc={globalViewData.byLoc} valueKey="med_ev" colorFn={evColor}
              formatFn={(v) => v != null ? Math.round(v).toString() : "—"} />
          </div>

          {/* Bar Charts */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 16 }}>
            {[
              { title: "Order to Ship Avg (days)", dataKey: "avg_lt", colorFn: ltColor, fmt: (v) => v != null ? v.toFixed(1) : "" },
              { title: "DIFOT % by Location", dataKey: "difot_pct", colorFn: difotColor, fmt: (v) => v != null ? v.toFixed(0) + "%" : "" },
              { title: "Execution Variance — Median (days)", dataKey: "med_ev", colorFn: evColor, fmt: (v) => v != null ? Math.round(v) : "" },
            ].map(({ title, dataKey, colorFn, fmt }) => {
              const data = [...globalViewData.byLoc]
                .filter((d) => d[dataKey] != null)
                .sort((a, b) => (a[dataKey] || 0) - (b[dataKey] || 0));
              return (
                <div key={title} style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 16 }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 12, textTransform: "uppercase", letterSpacing: 1 }}>{title}</div>
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={data} margin={{ top: 10, right: 10, left: -10, bottom: 40 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                      <XAxis dataKey="loc" tick={{ fill: "#9ca3af", fontSize: 9 }} angle={-40} textAnchor="end" interval={0} />
                      <YAxis tick={{ fill: "#9ca3af", fontSize: 10 }} />
                      <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }}
                        formatter={(v) => [v != null ? (typeof v === 'number' ? v.toFixed(1) : v) : "—", title]} />
                      <Bar dataKey={dataKey} radius={[3, 3, 0, 0]} label={{ position: "top", fontSize: 9, fill: "#9ca3af", formatter: fmt }}>
                        {data.map((d, i) => <Cell key={i} fill={colorFn(d[dataKey])} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              );
            })}
          </div>

          {/* Line Charts — Monthly Trends */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
            {[
              { title: "Order to Ship — Monthly Avg", lineKey: "lt" },
              { title: "DIFOT % — Monthly Trend", lineKey: "difot" },
              { title: "Execution Variance — Monthly Median", lineKey: "ev" },
            ].map(({ title, lineKey }) => (
              <div key={title} style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 16 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 12, textTransform: "uppercase", letterSpacing: 1 }}>{title}</div>
                <ResponsiveContainer width="100%" height={240}>
                  <LineChart data={globalViewData.lineData[lineKey]} margin={{ top: 5, right: 5, left: -15, bottom: 40 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="month" tick={{ fill: "#9ca3af", fontSize: 9 }} angle={-40} textAnchor="end" height={50} />
                    <YAxis tick={{ fill: "#9ca3af", fontSize: 9 }} />
                    <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6, fontSize: 11 }} />
                    <Legend wrapperStyle={{ fontSize: 9, paddingTop: 4 }} />
                    {globalViewData.locs.map((loc, i) => (
                      <Line key={loc} type="monotone" dataKey={loc} stroke={LINE_COLORS[i % LINE_COLORS.length]}
                        dot={{ r: 3 }} strokeWidth={1.5} connectNulls />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ))}
          </div>
        </>
      )}

      {/* ── BUSINESS LINES TAB ── */}
      {tab === "business-lines" && (() => {
        const BL_COLORS = { PCV: "#3b82f6", Service: "#22c55e", Products: "#f59e0b", Unclassified: "#6b7280" };
        const blColor = (bl) => BL_COLORS[bl] || "#a855f7";

        return (
          <>
            {/* KPI Cards per Business Line */}
            <div style={{ display: "grid", gridTemplateColumns: `repeat(${blData.byBL.length}, 1fr)`, gap: 14, marginBottom: 20 }}>
              {blData.byBL.map((bl) => {
                const c = blColor(bl.bl);
                return (
                  <div key={bl.bl} style={{ background: "#1f2937", border: `2px solid ${c}55`, borderRadius: 12, padding: "20px 22px" }}>
                    <div style={{ fontSize: 13, fontWeight: 800, color: c, marginBottom: 16, textTransform: "uppercase", letterSpacing: 1 }}>{bl.bl}</div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                      {[
                        { label: "DIFOT %", value: bl.difot_pct.toFixed(1) + "%", color: difotColor(bl.difot_pct) },
                        { label: "# Orders", value: bl.total.toLocaleString(), color: "#f9fafb" },
                        { label: "Lead Time", value: bl.avg_lt != null ? bl.avg_lt.toFixed(1) + "d" : "—", color: ltColor(bl.avg_lt) },
                        { label: "Exec Var", value: bl.med_ev != null ? Math.round(bl.med_ev) + "d" : "—", color: evColor(bl.med_ev) },
                        { label: "Past Due", value: bl.pastDueCount > 0 ? bl.pastDueCount.toLocaleString() : "—", color: bl.pastDueCount > 0 ? "#f97316" : "#6b7280" },
                        { label: "At Risk 30d", value: bl.atRisk30 > 0 ? bl.atRisk30.toLocaleString() : "—", color: bl.atRisk30 > 0 ? "#f59e0b" : "#6b7280" },
                        { label: "Order Value", value: fmtMoney(bl.totalVal), color: "#d1d5db" },
                        { label: "DIFOT Value", value: fmtMoney(bl.difotVal), color: "#22c55e" },
                      ].map(({ label, value, color }) => (
                        <div key={label} style={{ background: "#111827", borderRadius: 7, padding: "10px 12px" }}>
                          <div style={{ fontSize: 9, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>{label}</div>
                          <div style={{ fontSize: 18, fontWeight: 800, color }}>{value}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>

            {/* DIFOT % bar comparison */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 20 }}>
              <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 16, textTransform: "uppercase", letterSpacing: 1 }}>DIFOT % by Business Line</div>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={blData.byBL} margin={{ top: 10, right: 10, left: -10, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="bl" tick={{ fill: "#9ca3af", fontSize: 12 }} />
                    <YAxis domain={[0, 100]} tick={{ fill: "#9ca3af", fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }}
                      formatter={(v) => [v.toFixed(1) + "%", "DIFOT %"]} />
                    <Bar dataKey="difot_pct" radius={[5, 5, 0, 0]} label={{ position: "top", fontSize: 12, fontWeight: 700, fill: "#f9fafb", formatter: (v) => v.toFixed(1) + "%" }}>
                      {blData.byBL.map((d) => <Cell key={d.bl} fill={blColor(d.bl)} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 16, textTransform: "uppercase", letterSpacing: 1 }}>Past Due & At Risk by Business Line</div>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={blData.byBL} margin={{ top: 10, right: 10, left: -10, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis dataKey="bl" tick={{ fill: "#9ca3af", fontSize: 12 }} />
                    <YAxis tick={{ fill: "#9ca3af", fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }} />
                    <Legend wrapperStyle={{ fontSize: 11 }} />
                    <Bar dataKey="pastDueCount" name="Past Due" fill="#f97316" radius={[3, 3, 0, 0]} />
                    <Bar dataKey="atRisk30" name="At Risk 30d" fill="#f59e0b" radius={[3, 3, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* DIFOT % Monthly Trend per BL */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20, marginBottom: 20 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 16, textTransform: "uppercase", letterSpacing: 1 }}>DIFOT % Monthly Trend — by Business Line</div>
              <ResponsiveContainer width="100%" height={240}>
                <LineChart margin={{ top: 5, right: 10, left: -10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="month" type="category" allowDuplicatedCategory={false}
                    data={blData.byBL[0]?.trendData || []}
                    tick={{ fill: "#9ca3af", fontSize: 11 }} />
                  <YAxis domain={[0, 100]} tick={{ fill: "#9ca3af", fontSize: 11 }} tickFormatter={(v) => v + "%"} />
                  <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6, fontSize: 12 }}
                    formatter={(v, name) => [v != null ? v + "%" : "—", name]} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  {blData.byBL.map((bl) => (
                    <Line key={bl.bl} type="monotone" dataKey="pct" data={bl.trendData} name={bl.bl}
                      stroke={blColor(bl.bl)} strokeWidth={2.5} dot={{ r: 4, fill: blColor(bl.bl) }} connectNulls />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* BL × Site matrix */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>DIFOT % — Site × Business Line</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 16 }}>Each cell = DIFOT% for that site and business line combination. Empty = no orders.</div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ borderCollapse: "collapse", fontSize: 12, minWidth: "100%" }}>
                  <thead>
                    <tr>
                      <th style={{ padding: "8px 14px", color: "#6b7280", fontWeight: 700, fontSize: 11, textAlign: "left", borderBottom: "1px solid #374151", background: "#111827", whiteSpace: "nowrap" }}>Site</th>
                      {blData.allBLs.map((bl) => (
                        <th key={bl} style={{ padding: "8px 14px", color: blColor(bl), fontWeight: 700, fontSize: 11, textAlign: "center", borderBottom: "1px solid #374151", background: "#111827", whiteSpace: "nowrap" }}>{bl}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {blData.allSites.map((site, i) => (
                      <tr key={site} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                        <td style={{ padding: "9px 14px", fontWeight: 700, color: "#d1d5db", whiteSpace: "nowrap" }}>{site}</td>
                        {blData.allBLs.map((bl) => {
                          const cell = blData.blSiteMatrix.find((d) => d.bl === bl && d.site === site);
                          if (!cell || cell.total < 3) return <td key={bl} style={{ padding: "9px 14px", textAlign: "center", color: "#374151" }}>—</td>;
                          const c = difotColor(cell.difot_pct);
                          return (
                            <td key={bl} style={{ padding: "9px 14px", textAlign: "center" }}>
                              <span style={{ fontWeight: 700, color: c, background: c + "18", padding: "3px 10px", borderRadius: 5, display: "inline-block" }}>
                                {cell.difot_pct.toFixed(1)}%
                              </span>
                              <span style={{ display: "block", fontSize: 10, color: "#6b7280", marginTop: 2 }}>{cell.total.toLocaleString()} orders</span>
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        );
      })()}

      {/* ── NOT MOVING ITEMS TAB ── */}
      {tab === "bucket_not_moving_items" && (() => {
        const BUCKET_COLORS = { "6–9 months": "#f59e0b", "9–12 months": "#f97316", "12–18 months": "#ef4444", "18+ months": "#7f1d1d" };

        const sortedItems = [...notMovingData.items].sort((a, b) => {
          const av = a[nmSortCol] ?? "";
          const bv = b[nmSortCol] ?? "";
          return typeof av === "string" ? av.localeCompare(bv) * nmSortDir : (av - bv) * nmSortDir;
        });
        const pageItems = sortedItems.slice(nmPage * NM_PAGE_SIZE, (nmPage + 1) * NM_PAGE_SIZE);
        const totalPages = Math.ceil(sortedItems.length / NM_PAGE_SIZE);

        const nmColH = (key, label, right) => {
          const active = nmSortCol === key;
          return (
            <th onClick={() => { if (active) setNmSortDir((d) => -d); else { setNmSortCol(key); setNmSortDir(-1); } }}
              style={{ padding: "9px 12px", color: active ? "#93c5fd" : "#6b7280", fontWeight: 700, fontSize: 11,
                textAlign: right ? "right" : "left", borderBottom: "1px solid #374151", whiteSpace: "nowrap",
                background: "#111827", cursor: "pointer", userSelect: "none" }}>
              {label}{active ? (nmSortDir === -1 ? " ↓" : " ↑") : ""}
            </th>
          );
        };

        return (
          <>
            {/* Hero KPIs */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, marginBottom: 20 }}>
              {[
                { label: "Not Moving Lines", value: notMovingData.totalCount.toLocaleString(), sub: "open orders, no shipment in 6+ months", color: "#ef4444", border: "#ef4444" },
                { label: "Value at Risk", value: fmtMoney(notMovingData.totalValue), sub: "total order value of stagnant lines", color: "#f97316", border: "#f97316" },
                { label: "Unique Items", value: notMovingData.uniqueItems.toLocaleString(), sub: "distinct item SKUs affected", color: "#f59e0b", border: "#f59e0b" },
                { label: "Sites Affected", value: notMovingData.sitesAffected.toLocaleString(), sub: "locations with not-moving orders", color: "#a855f7", border: "#a855f7" },
              ].map(({ label, value, sub, color, border }) => (
                <div key={label} style={{ background: "#1f2937", border: `1px solid ${border}44`, borderRadius: 12, padding: "22px 24px" }}>
                  <div style={{ fontSize: 10, color: "#6b7280", fontWeight: 700, letterSpacing: 1, textTransform: "uppercase", marginBottom: 8 }}>{label}</div>
                  <div style={{ fontSize: 36, fontWeight: 800, color, lineHeight: 1 }}>{value}</div>
                  <div style={{ fontSize: 11, color: "#6b7280", marginTop: 6 }}>{sub}</div>
                </div>
              ))}
            </div>

            {/* Aging Buckets */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, marginBottom: 20 }}>
              {notMovingData.buckets.map((b) => {
                const c = BUCKET_COLORS[b.label];
                const pct = notMovingData.totalCount ? ((b.count / notMovingData.totalCount) * 100).toFixed(1) : "0";
                return (
                  <div key={b.label} style={{ background: "#1f2937", border: `2px solid ${c}55`, borderRadius: 10, padding: "18px 20px" }}>
                    <div style={{ fontSize: 11, fontWeight: 800, color: c, marginBottom: 12, textTransform: "uppercase", letterSpacing: 1 }}>{b.label}</div>
                    <div style={{ fontSize: 30, fontWeight: 800, color: "#f9fafb", lineHeight: 1 }}>{b.count.toLocaleString()}</div>
                    <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4, marginBottom: 10 }}>{pct}% of total not-moving</div>
                    <div style={{ background: "#111827", borderRadius: 6, padding: "8px 12px" }}>
                      <div style={{ fontSize: 10, color: "#6b7280", marginBottom: 3 }}>Value</div>
                      <div style={{ fontSize: 16, fontWeight: 700, color: c }}>{fmtMoney(b.value)}</div>
                    </div>
                    <div style={{ background: c + "22", borderRadius: 4, height: 5, marginTop: 12, overflow: "hidden" }}>
                      <div style={{ width: `${pct}%`, height: "100%", background: c, borderRadius: 4 }} />
                    </div>
                  </div>
                );
              })}
            </div>

            {/* By Site + By Business Line */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 20 }}>
              <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 14, textTransform: "uppercase", letterSpacing: 1 }}>By Site</div>
                {notMovingData.bySite.map((s, i) => (
                  <div key={s.site} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10, background: "#111827", borderRadius: 6, padding: "8px 12px" }}>
                    <div>
                      <span style={{ fontWeight: 700, color: "#f9fafb", fontSize: 13 }}>{s.site}</span>
                      <span style={{ fontSize: 11, color: "#6b7280", marginLeft: 8 }}>{s.region}</span>
                      <span style={{ display: "block", fontSize: 10, color: "#ef4444", marginTop: 2 }}>oldest: {s.maxDays}d open</span>
                    </div>
                    <div style={{ textAlign: "right" }}>
                      <span style={{ fontWeight: 800, color: "#f97316", fontSize: 16 }}>{s.count.toLocaleString()}</span>
                      <span style={{ display: "block", fontSize: 11, color: "#9ca3af" }}>{fmtMoney(s.value)}</span>
                    </div>
                  </div>
                ))}
              </div>

              <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", marginBottom: 14, textTransform: "uppercase", letterSpacing: 1 }}>By Business Line</div>
                {notMovingData.byBL.map((b) => {
                  const blColors = { PCV: "#3b82f6", Service: "#22c55e", Products: "#f59e0b", Unclassified: "#6b7280" };
                  const c = blColors[b.bl] || "#a855f7";
                  const pct = notMovingData.totalCount ? (b.count / notMovingData.totalCount) * 100 : 0;
                  return (
                    <div key={b.bl} style={{ marginBottom: 14 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: c }}>{b.bl}</span>
                        <span style={{ fontSize: 13, color: "#d1d5db" }}>{b.count.toLocaleString()} lines — {fmtMoney(b.value)}</span>
                      </div>
                      <div style={{ background: "#374151", borderRadius: 4, height: 8, overflow: "hidden" }}>
                        <div style={{ width: `${pct}%`, height: "100%", background: c, borderRadius: 4 }} />
                      </div>
                      <div style={{ fontSize: 10, color: "#6b7280", marginTop: 2 }}>{pct.toFixed(1)}% of all not-moving</div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Detail Table */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
                <div>
                  <div style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd", textTransform: "uppercase", letterSpacing: 1 }}>Order Detail</div>
                  <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>Showing {nmPage * NM_PAGE_SIZE + 1}–{Math.min((nmPage + 1) * NM_PAGE_SIZE, sortedItems.length)} of {sortedItems.length.toLocaleString()} lines. Click headers to sort.</div>
                </div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button onClick={() => setNmPage((p) => Math.max(0, p - 1))} disabled={nmPage === 0}
                    style={{ background: nmPage === 0 ? "#374151" : "#2563eb", color: "#fff", border: "none", borderRadius: 6, padding: "6px 14px", cursor: nmPage === 0 ? "default" : "pointer", fontSize: 12 }}>← Prev</button>
                  <span style={{ fontSize: 12, color: "#6b7280", alignSelf: "center" }}>Page {nmPage + 1} / {totalPages}</span>
                  <button onClick={() => setNmPage((p) => Math.min(totalPages - 1, p + 1))} disabled={nmPage >= totalPages - 1}
                    style={{ background: nmPage >= totalPages - 1 ? "#374151" : "#2563eb", color: "#fff", border: "none", borderRadius: 6, padding: "6px 14px", cursor: nmPage >= totalPages - 1 ? "default" : "pointer", fontSize: 12 }}>Next →</button>
                </div>
              </div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr>
                      {nmColH("sales_order_id", "SO #", false)}
                      {nmColH("item_id", "Item", false)}
                      {nmColH("company", "Site", false)}
                      {nmColH("customer_name", "Customer", false)}
                      {nmColH("business_line", "Business Line", false)}
                      {nmColH("created_date", "Created", false)}
                      {nmColH("confirmed_ship_date", "Conf. Ship Date", false)}
                      {nmColH("daysOpen", "Days Open", true)}
                      {nmColH("ordered_qty", "Qty", true)}
                      {nmColH("val", "Value", true)}
                      <th style={{ padding: "9px 12px", color: "#6b7280", fontWeight: 700, fontSize: 11, textAlign: "center", borderBottom: "1px solid #374151", background: "#111827" }}>Age</th>
                    </tr>
                  </thead>
                  <tbody>
                    {pageItems.map((r, i) => {
                      const bc = BUCKET_COLORS[r.bucket];
                      return (
                        <tr key={r.line_id || i} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                          <Td><span style={{ fontFamily: "monospace", color: "#93c5fd", fontSize: 11 }}>{r.sales_order_id || "—"}</span></Td>
                          <Td><span style={{ fontFamily: "monospace", fontSize: 11 }}>{r.item_id || "—"}</span></Td>
                          <Td>{r.company || "—"}</Td>
                          <Td><span style={{ fontSize: 11 }}>{r.customer_name || "—"}</span></Td>
                          <Td><span style={{ fontSize: 11, color: "#9ca3af" }}>{r.business_line || "—"}</span></Td>
                          <Td><span style={{ fontSize: 11 }}>{r.created_date?.slice(0, 10) || "—"}</span></Td>
                          <Td><span style={{ fontSize: 11, color: r.confirmed_ship_date ? "#f97316" : "#6b7280" }}>{r.confirmed_ship_date?.slice(0, 10) || "—"}</span></Td>
                          <Td right><span style={{ fontWeight: 700, color: bc }}>{r.daysOpen}</span></Td>
                          <Td right>{r.ordered_qty ? parseFloat(r.ordered_qty).toLocaleString() : "—"}</Td>
                          <Td right>{r.val > 0 ? fmtMoney(r.val) : "—"}</Td>
                          <td style={{ padding: "8px 12px", textAlign: "center" }}>
                            <span style={{ fontSize: 10, fontWeight: 700, color: bc, background: bc + "22", padding: "2px 8px", borderRadius: 10, whiteSpace: "nowrap" }}>{r.bucket}</span>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        );
      })()}

      {/* ── SITE PERFORMANCE TAB ── */}
      {tab === "site-performance" && (() => {
        const sortedSites = [...siteData].sort((a, b) => {
          const av = a[spSortCol] ?? -Infinity;
          const bv = b[spSortCol] ?? -Infinity;
          return typeof av === 'string' ? av.localeCompare(bv) * spSortDir : (av - bv) * spSortDir;
        });
        const pastDueSites = [...siteData].filter((s) => s.pastDueCount > 0).sort((a, b) => b.pastDueCount - a.pastDueCount);
        const atRiskSites = [...siteData].filter((s) => s.atRisk30 > 0).sort((a, b) => b.atRisk30 - a.atRisk30);
        const decliningSites = siteData.filter((s) => s.declining3m);

        const colH = (key, label, right) => {
          const active = spSortCol === key;
          return (
            <th onClick={() => { if (active) setSpSortDir((d) => -d); else { setSpSortCol(key); setSpSortDir(-1); } }}
              style={{ padding: "9px 12px", color: active ? "#93c5fd" : "#6b7280", fontWeight: 700, fontSize: 11,
                textAlign: right ? "right" : "left", borderBottom: "1px solid #374151", whiteSpace: "nowrap",
                background: "#111827", cursor: "pointer", userSelect: "none" }}>
              {label}{active ? (spSortDir === -1 ? ' ↓' : ' ↑') : ''}
            </th>
          );
        };

        return (
          <>
            {/* --- SITE SCORECARD TABLE --- */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20, marginBottom: 20 }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: "#93c5fd", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>
                Site Scorecard — All KPIs
              </div>
              <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 14 }}>Click any column header to sort. Color = performance vs threshold.</div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr>
                      {colH("site", "Site", false)}
                      {colH("region", "Region", false)}
                      {colH("total", "# Orders", true)}
                      {colH("difot_pct", "DIFOT %", true)}
                      {colH("avg_lt", "Avg Lead Time (d)", true)}
                      {colH("med_ev", "Median Exec Var (d)", true)}
                      {colH("pastDueCount", "Past Due #", true)}
                      {colH("pastDueVal", "Past Due $", true)}
                      {colH("atRisk30", "At Risk ≤30d #", true)}
                      {colH("atRisk60", "At Risk ≤60d #", true)}
                      <th style={{ padding: "9px 12px", color: "#6b7280", fontWeight: 700, fontSize: 11, textAlign: "center", borderBottom: "1px solid #374151", whiteSpace: "nowrap", background: "#111827" }}>MoM</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedSites.map((s, i) => (
                      <tr key={s.site} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                        <td style={{ padding: "8px 12px", fontWeight: 700, fontSize: 13, color: "#d1d5db", whiteSpace: "nowrap" }}>{s.site}</td>
                        <td style={{ padding: "8px 12px", fontSize: 12, color: "#9ca3af", whiteSpace: "nowrap" }}>{s.region}</td>
                        <td style={{ padding: "8px 12px", textAlign: "right", fontSize: 13, color: "#d1d5db" }}>{s.total.toLocaleString()}</td>
                        <td style={{ padding: "8px 12px", textAlign: "right" }}>
                          <span style={{ fontWeight: 700, color: difotColor(s.difot_pct), background: difotColor(s.difot_pct) + '18', padding: "2px 8px", borderRadius: 4 }}>
                            {s.difot_pct.toFixed(1)}%
                          </span>
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "right", fontWeight: 600, color: ltColor(s.avg_lt) }}>
                          {s.avg_lt != null ? s.avg_lt.toFixed(1) : "—"}
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "right", fontWeight: 600, color: evColor(s.med_ev) }}>
                          {s.med_ev != null ? Math.round(s.med_ev) : "—"}
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "right", color: s.pastDueCount > 0 ? "#f97316" : "#6b7280", fontWeight: s.pastDueCount > 0 ? 700 : 400 }}>
                          {s.pastDueCount > 0 ? s.pastDueCount.toLocaleString() : "—"}
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "right", color: s.pastDueVal > 0 ? "#f97316" : "#6b7280", fontSize: 12 }}>
                          {s.pastDueVal > 0 ? fmtMoney(s.pastDueVal) : "—"}
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "right", color: s.atRisk30 > 0 ? "#f59e0b" : "#6b7280", fontWeight: s.atRisk30 > 0 ? 700 : 400 }}>
                          {s.atRisk30 > 0 ? s.atRisk30.toLocaleString() : "—"}
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "right", color: s.atRisk60 > 0 ? "#f59e0b" : "#6b7280" }}>
                          {s.atRisk60 > 0 ? s.atRisk60.toLocaleString() : "—"}
                        </td>
                        <td style={{ padding: "8px 12px", textAlign: "center", fontWeight: 800, fontSize: 16, color: s.trendColor }}>
                          {s.trend}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
                {[
                  { label: "DIFOT %", items: [{ c: "#22c55e", t: ">80%" }, { c: "#f59e0b", t: "60–80%" }, { c: "#ef4444", t: "<60%" }] },
                  { label: "Lead Time", items: [{ c: "#22c55e", t: "≤30d" }, { c: "#f59e0b", t: "31–60d" }, { c: "#ef4444", t: ">60d" }] },
                  { label: "Exec Variance", items: [{ c: "#22c55e", t: "|<15d|" }, { c: "#f59e0b", t: "|<30d|" }, { c: "#ef4444", t: "|>30d|" }] },
                  { label: "MoM", items: [{ c: "#22c55e", t: "↑ improving" }, { c: "#6b7280", t: "→ stable" }, { c: "#ef4444", t: "↓ declining" }] },
                ].map(({ label, items }) => (
                  <div key={label} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 10, color: "#6b7280" }}>
                    <span style={{ fontWeight: 700 }}>{label}:</span>
                    {items.map(({ c, t }) => (
                      <span key={t} style={{ display: "flex", alignItems: "center", gap: 3 }}>
                        <span style={{ width: 8, height: 8, borderRadius: "50%", background: c, display: "inline-block" }} />{t}
                      </span>
                    ))}
                  </div>
                ))}
              </div>
            </div>

            {/* --- PROACTIVE RISK VIEW --- */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
              {/* Past Due by site */}
              <div style={{ background: "#1f2937", border: "1px solid #f9731644", borderRadius: 10, padding: 20 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#fb923c", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>
                  Already Past Due — by Site
                </div>
                <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 12 }}>Orders where confirmed ship date has passed with no goods issue date</div>
                {pastDueSites.length === 0
                  ? <div style={{ color: "#22c55e", fontSize: 13, fontWeight: 700 }}>No past due orders — all sites clear!</div>
                  : pastDueSites.map((s) => (
                    <div key={s.site} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8, background: "#111827", borderRadius: 6, padding: "8px 12px" }}>
                      <div>
                        <span style={{ fontWeight: 700, color: "#f9fafb", fontSize: 13 }}>{s.site}</span>
                        <span style={{ fontSize: 11, color: "#6b7280", marginLeft: 8 }}>{s.region}</span>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <span style={{ fontWeight: 800, color: "#f97316", fontSize: 14 }}>{s.pastDueCount.toLocaleString()}</span>
                        <span style={{ fontSize: 11, color: "#9ca3af", marginLeft: 6 }}>orders</span>
                        <span style={{ display: "block", fontSize: 11, color: "#f59e0b" }}>{fmtMoney(s.pastDueVal)}</span>
                      </div>
                    </div>
                  ))
                }
              </div>

              {/* At Risk next 30 days */}
              <div style={{ background: "#1f2937", border: "1px solid #f59e0b44", borderRadius: 10, padding: 20 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#fbbf24", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>
                  At Risk — Next 30 Days
                </div>
                <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 12 }}>Open orders with confirmed ship date in the next 30 days (no goods issue yet)</div>
                {atRiskSites.length === 0
                  ? <div style={{ color: "#22c55e", fontSize: 13, fontWeight: 700 }}>No at-risk orders in the next 30 days.</div>
                  : atRiskSites.map((s) => (
                    <div key={s.site} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8, background: "#111827", borderRadius: 6, padding: "8px 12px" }}>
                      <div>
                        <span style={{ fontWeight: 700, color: "#f9fafb", fontSize: 13 }}>{s.site}</span>
                        <span style={{ fontSize: 11, color: "#6b7280", marginLeft: 8 }}>{s.region}</span>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <span style={{ fontWeight: 800, color: "#f59e0b", fontSize: 14 }}>{s.atRisk30.toLocaleString()}</span>
                        <span style={{ fontSize: 11, color: "#9ca3af", marginLeft: 6 }}>orders</span>
                        <span style={{ display: "block", fontSize: 11, color: "#fbbf24" }}>{fmtMoney(s.atRisk30Val)}</span>
                      </div>
                    </div>
                  ))
                }
              </div>
            </div>

            {/* Declining trend sites */}
            {decliningSites.length > 0 && (
              <div style={{ background: "#1f2937", border: "1px solid #ef444444", borderRadius: 10, padding: 20, marginBottom: 16 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#fca5a5", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>
                  DIFOT Declining — 3 Consecutive Months ↓
                </div>
                <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 12 }}>These sites have shown declining DIFOT for 3 months in a row — immediate SIOP review recommended</div>
                <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                  {decliningSites.map((s) => (
                    <div key={s.site} style={{ background: "#111827", border: "1px solid #ef444466", borderRadius: 8, padding: "12px 18px", minWidth: 180 }}>
                      <div style={{ fontWeight: 700, color: "#f9fafb", fontSize: 14, marginBottom: 4 }}>{s.site}</div>
                      <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 6 }}>{s.region}</div>
                      <div style={{ fontSize: 18, fontWeight: 800, color: "#ef4444" }}>{s.difot_pct.toFixed(1)}%</div>
                      <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>Current DIFOT</div>
                      <div style={{ fontSize: 11, color: "#ef4444", marginTop: 4 }}>↓ 3-month decline</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* At Risk 60d summary row */}
            <div style={{ background: "#111827", border: "1px solid #374151", borderRadius: 10, padding: 16, display: "flex", gap: 24, flexWrap: "wrap" }}>
              <div>
                <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Total Past Due Orders</div>
                <div style={{ fontSize: 24, fontWeight: 800, color: "#f97316" }}>{siteData.reduce((s, d) => s + d.pastDueCount, 0).toLocaleString()}</div>
              </div>
              <div>
                <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Past Due Value</div>
                <div style={{ fontSize: 24, fontWeight: 800, color: "#f97316" }}>{fmtMoney(siteData.reduce((s, d) => s + d.pastDueVal, 0))}</div>
              </div>
              <div>
                <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>At Risk Next 30d</div>
                <div style={{ fontSize: 24, fontWeight: 800, color: "#f59e0b" }}>{siteData.reduce((s, d) => s + d.atRisk30, 0).toLocaleString()}</div>
              </div>
              <div>
                <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>At Risk Next 60d</div>
                <div style={{ fontSize: 24, fontWeight: 800, color: "#fbbf24" }}>{siteData.reduce((s, d) => s + d.atRisk60, 0).toLocaleString()}</div>
              </div>
              <div>
                <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Sites Declining 3m</div>
                <div style={{ fontSize: 24, fontWeight: 800, color: decliningSites.length > 0 ? "#ef4444" : "#22c55e" }}>{decliningSites.length}</div>
              </div>
            </div>
          </>
        );
      })()}

      {/* ── SCORECARD TAB — EXECUTIVE SUMMARY ── */}
      {tab === "scorecard" && (() => {
        const totalPastDue = siteData.reduce((s, d) => s + d.pastDueCount, 0);
        const totalPastDueVal = siteData.reduce((s, d) => s + d.pastDueVal, 0);
        const totalAtRisk30 = siteData.reduce((s, d) => s + d.atRisk30, 0);
        const decliningSites = siteData.filter((s) => s.declining3m);
        const difotPct = kpi.total ? (kpi.difot / kpi.total) * 100 : 0;

        const ragStatus = (s) => {
          if (s.difot_pct < 60 || s.pastDueCount > 10) return { label: "Red", color: "#ef4444", bg: "#ef444415" };
          if (s.difot_pct < 80 || s.pastDueCount > 0 || s.atRisk30 > 10) return { label: "Amber", color: "#f59e0b", bg: "#f59e0b15" };
          return { label: "Green", color: "#22c55e", bg: "#22c55e15" };
        };

        const sitesRanked = [...siteData].sort((a, b) => {
          const order = { Red: 0, Amber: 1, Green: 2 };
          return order[ragStatus(a).label] - order[ragStatus(b).label] || b.pastDueCount - a.pastDueCount;
        });

        const alerts = [];
        const worst = [...siteData].sort((a, b) => a.difot_pct - b.difot_pct)[0];
        if (worst) alerts.push({ color: "#ef4444", text: `${worst.site} has the lowest DIFOT at ${worst.difot_pct.toFixed(1)}% — ${worst.pastDueCount} past due orders worth ${fmtMoney(worst.pastDueVal)}.` });
        if (totalPastDue > 0) alerts.push({ color: "#f97316", text: `${totalPastDue.toLocaleString()} orders are past their confirmed ship date across all sites, totalling ${fmtMoney(totalPastDueVal)} in value.` });
        if (totalAtRisk30 > 0) alerts.push({ color: "#f59e0b", text: `${totalAtRisk30.toLocaleString()} open orders are due to ship in the next 30 days with no goods issue date — potential future late deliveries.` });
        if (decliningSites.length > 0) alerts.push({ color: "#a855f7", text: `${decliningSites.map((s) => s.site).join(", ")} ${decliningSites.length === 1 ? "is" : "are"} showing declining DIFOT for 3 consecutive months.` });

        return (
          <>
            {/* Hero KPIs */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, marginBottom: 20 }}>
              {[
                { label: "Global DIFOT", value: pct(kpi.difot, kpi.total), sub: `${kpi.difot.toLocaleString()} of ${kpi.total.toLocaleString()} lines`, color: difotColor(difotPct), border: difotColor(difotPct) },
                { label: "Past Due Orders", value: totalPastDue.toLocaleString(), sub: fmtMoney(totalPastDueVal) + " at risk", color: totalPastDue > 0 ? "#f97316" : "#22c55e", border: totalPastDue > 0 ? "#f97316" : "#22c55e" },
                { label: "At Risk — Next 30d", value: totalAtRisk30.toLocaleString(), sub: "open orders due soon, not shipped", color: totalAtRisk30 > 0 ? "#f59e0b" : "#22c55e", border: totalAtRisk30 > 0 ? "#f59e0b" : "#22c55e" },
                { label: "Avg Lead Time", value: kpi.avgLt != null ? kpi.avgLt.toFixed(1) + "d" : "—", sub: "order creation → goods issue", color: ltColor(kpi.avgLt), border: ltColor(kpi.avgLt) },
              ].map(({ label, value, sub, color, border }) => (
                <div key={label} style={{ background: "#1f2937", border: `1px solid ${border}44`, borderRadius: 12, padding: "22px 24px" }}>
                  <div style={{ fontSize: 10, color: "#6b7280", fontWeight: 700, letterSpacing: 1, textTransform: "uppercase", marginBottom: 8 }}>{label}</div>
                  <div style={{ fontSize: 36, fontWeight: 800, color, lineHeight: 1 }}>{value}</div>
                  <div style={{ fontSize: 11, color: "#6b7280", marginTop: 6 }}>{sub}</div>
                </div>
              ))}
            </div>

            {/* Alerts */}
            {alerts.length > 0 && (
              <div style={{ background: "#111827", border: "1px solid #374151", borderRadius: 10, padding: "14px 20px", marginBottom: 20 }}>
                <div style={{ fontSize: 10, color: "#6b7280", fontWeight: 700, letterSpacing: 1, textTransform: "uppercase", marginBottom: 10 }}>Action Required</div>
                {alerts.map((a, i) => (
                  <div key={i} style={{ display: "flex", gap: 10, alignItems: "flex-start", marginBottom: i < alerts.length - 1 ? 10 : 0 }}>
                    <span style={{ width: 8, height: 8, borderRadius: "50%", background: a.color, flexShrink: 0, marginTop: 5 }} />
                    <span style={{ fontSize: 13, color: "#d1d5db", lineHeight: 1.6 }}>{a.text}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Site Status Table */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#d1d5db", marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>Site Status</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 14 }}>Sorted by risk. Red = DIFOT &lt;60% or &gt;10 past due. Amber = DIFOT 60–80% or any past due.</div>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                <thead>
                  <tr>
                    <Th></Th>
                    <Th>Site</Th>
                    <Th>Region</Th>
                    <Th right>DIFOT %</Th>
                    <Th right>Trend</Th>
                    <Th right>Past Due</Th>
                    <Th right>At Risk 30d</Th>
                    <Th right>Lead Time</Th>
                  </tr>
                </thead>
                <tbody>
                  {sitesRanked.map((s, i) => {
                    const rag = ragStatus(s);
                    return (
                      <tr key={s.site} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                        <td style={{ padding: "10px 8px 10px 14px", width: 12 }}>
                          <span style={{ display: "inline-block", width: 10, height: 10, borderRadius: "50%", background: rag.color }} />
                        </td>
                        <Td bold>{s.site}</Td>
                        <Td><span style={{ fontSize: 11, color: "#6b7280" }}>{s.region}</span></Td>
                        <Td right>
                          <span style={{ fontWeight: 700, color: difotColor(s.difot_pct), background: difotColor(s.difot_pct) + "18", padding: "2px 8px", borderRadius: 4 }}>
                            {s.difot_pct.toFixed(1)}%
                          </span>
                        </Td>
                        <Td right><span style={{ fontWeight: 800, fontSize: 16, color: s.trendColor }}>{s.trend}</span></Td>
                        <Td right>
                          <span style={{ color: s.pastDueCount > 0 ? "#f97316" : "#6b7280", fontWeight: s.pastDueCount > 0 ? 700 : 400 }}>
                            {s.pastDueCount > 0 ? s.pastDueCount.toLocaleString() : "—"}
                          </span>
                        </Td>
                        <Td right>
                          <span style={{ color: s.atRisk30 > 0 ? "#f59e0b" : "#6b7280", fontWeight: s.atRisk30 > 0 ? 700 : 400 }}>
                            {s.atRisk30 > 0 ? s.atRisk30.toLocaleString() : "—"}
                          </span>
                        </Td>
                        <Td right><span style={{ color: ltColor(s.avg_lt) }}>{s.avg_lt != null ? s.avg_lt.toFixed(1) + "d" : "—"}</span></Td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        );
      })()}

      {/* ── TREND TAB ── */}
      {tab === "trend" && (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 20 }}>
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
              <div style={{ fontSize: 12, color: "#93c5fd", fontWeight: 700, marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>DIFOT % Trend (Line Count)</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 14 }}>Monthly — goods issue date ≤ confirmed ship date, qty ≥ 95%</div>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={byMonth} margin={{ top: 0, right: 0, left: -10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="month" tick={{ fill: "#9ca3af", fontSize: 10 }} angle={-45} textAnchor="end" height={50} />
                  <YAxis domain={[0, 100]} tick={{ fill: "#9ca3af", fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }} formatter={(v) => [v + "%", "DIFOT %"]} />
                  <Bar dataKey="difot_pct" name="DIFOT % (Lines)" radius={[3, 3, 0, 0]}>
                    {byMonth.map((m, i) => <Cell key={i} fill={difotColor(m.difot_pct)} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
              <div style={{ fontSize: 12, color: "#93c5fd", fontWeight: 700, marginBottom: 4, textTransform: "uppercase", letterSpacing: 1 }}>DIFOT % Trend (Value-Weighted)</div>
              <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 14 }}>DIFOT value / total order value by month</div>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={byMonth} margin={{ top: 0, right: 0, left: -10, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="month" tick={{ fill: "#9ca3af", fontSize: 10 }} angle={-45} textAnchor="end" height={50} />
                  <YAxis domain={[0, 100]} tick={{ fill: "#9ca3af", fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }} formatter={(v) => [v + "%", "DIFOT % (Value)"]} />
                  <Bar dataKey="difot_val_pct" name="DIFOT % (Value)" radius={[3, 3, 0, 0]}>
                    {byMonth.map((m, i) => <Cell key={i} fill={difotColor(m.difot_val_pct)} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20 }}>
            <div style={{ fontSize: 12, color: "#93c5fd", fontWeight: 700, marginBottom: 14, textTransform: "uppercase", letterSpacing: 1 }}>Monthly Order Volume &amp; Revenue</div>
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={byMonth} margin={{ top: 0, right: 0, left: 20, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis dataKey="month" tick={{ fill: "#9ca3af", fontSize: 10 }} angle={-45} textAnchor="end" height={50} />
                <YAxis yAxisId="lines" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                <YAxis yAxisId="val" orientation="right" tick={{ fill: "#9ca3af", fontSize: 11 }} tickFormatter={(v) => fmtMoney(v)} />
                <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 6 }}
                  formatter={(v, n) => [n === "val" ? fmtMoney(v) : v.toLocaleString(), n === "val" ? "Order Value" : "Lines"]} />
                <Bar yAxisId="lines" dataKey="total" name="total" fill="#3b82f6" radius={[3, 3, 0, 0]} />
                <Bar yAxisId="val" dataKey="val" name="val" fill="#22c55e" radius={[3, 3, 0, 0]} opacity={0.7} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      {/* ── DATA QUALITY TAB ── */}
      {tab === "quality" && (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16, marginBottom: 20 }}>
            {[
              { key: "D365", label: "D365 / Dynamics 365", note: "MART_DYN_FO" },
              { key: "ORACLE_ERP", label: "Oracle CEN01", note: "FLS_PROD_DB.RAW_CEN01" },
              { key: "EPICOR", label: "Epicor", note: "VW_EPICOR_SALE_ORDER" },
            ].map((e) => {
              const cnt = enriched.filter((r) => r.erp_source === e.key).length;
              const live = cnt > 0;
              const color = live ? "#22c55e" : "#f59e0b";
              return (
                <div key={e.key} style={{ background: "#1f2937", border: `1px solid ${color}44`, borderRadius: 10, padding: 16 }}>
                  <div style={{ fontSize: 13, fontWeight: 700, color: "#f9fafb", marginBottom: 4 }}>{e.label}</div>
                  <div style={{ display: "inline-block", fontSize: 11, fontWeight: 600, color, background: color + "22", borderRadius: 4, padding: "2px 8px", marginBottom: 8 }}>
                    {live ? `Live — ${cnt.toLocaleString()} lines` : "Pending"}
                  </div>
                  <div style={{ fontSize: 11, color: "#6b7280" }}>{e.note}</div>
                </div>
              );
            })}
          </div>
          {quality.length > 0 ? quality.map((q, i) => (
            <div key={i} style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 20, marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 700, color: "#f9fafb", marginBottom: 14 }}>
                <span style={{ fontFamily: "monospace", color: "#93c5fd" }}>{q.company}</span>
                <span style={{ fontSize: 12, color: "#6b7280", fontWeight: 400, marginLeft: 10 }}>{parseInt(q.total_lines || 0).toLocaleString()} active lines</span>
              </div>
              <QualityBar label="Confirmed Ship Date (ConfirmedShippingDate)" pctOk={parseFloat(q.pct_with_confirmed_date || 0)} total={parseInt(q.total_lines || 0)} missing={parseInt(q.missing_confirmed_date || 0)} />
              <QualityBar label="Requested Ship Date (ShippingDateRequested)" pctOk={parseFloat(q.pct_with_requested_date || 0)} total={parseInt(q.total_lines || 0)} missing={parseInt(q.missing_requested_date || 0)} />
              <QualityBar label="Created Date" pctOk={parseFloat(q.total_lines) > 0 ? (1 - parseFloat(q.missing_created_date || 0) / parseFloat(q.total_lines)) * 100 : 100} total={parseInt(q.total_lines || 0)} missing={parseInt(q.missing_created_date || 0)} />
              <QualityBar label="Ordered Quantity" pctOk={parseFloat(q.total_lines) > 0 ? (1 - parseFloat(q.missing_qty || 0) / parseFloat(q.total_lines)) * 100 : 100} total={parseInt(q.total_lines || 0)} missing={parseInt(q.missing_qty || 0)} />
              <QualityBar label="Customer Name" pctOk={parseFloat(q.total_lines) > 0 ? (1 - parseFloat(q.missing_customer || 0) / parseFloat(q.total_lines)) * 100 : 100} total={parseInt(q.total_lines || 0)} missing={parseInt(q.missing_customer || 0)} />
            </div>
          )) : <div style={{ padding: 32, textAlign: "center", color: "#6b7280" }}>Run export to generate quality metrics.</div>}
        </>
      )}

      {/* ── DEFINITIONS TAB ── */}
      {tab === "definitions" && (
        <div style={{ maxWidth: 860 }}>
          {/* DIFOT */}
          <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 28, marginBottom: 20 }}>
            <div style={{ fontSize: 18, fontWeight: 800, color: "#f9fafb", marginBottom: 4 }}>DIFOT</div>
            <div style={{ fontSize: 13, color: "#9ca3af", marginBottom: 20 }}>Delivery In Full On Time</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 24 }}>
              {[
                { label: "Start", value: "ConfirmedShippingDate", sub: "SOL.SHIPPINGDATECONFIRMED", color: "#3b82f6" },
                { label: "End (actual)", value: "Goods Issue Date", sub: "CUSTOMER_PACKING_SLIP_LINES.DELIVERYDATE", color: "#22c55e" },
                { label: "In Full threshold", value: "≥ 95%", sub: "shipped_qty / ordered_qty", color: "#f59e0b" },
              ].map((d) => (
                <div key={d.label} style={{ background: "#111827", borderRadius: 8, padding: "14px 18px", border: `1px solid ${d.color}44` }}>
                  <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>{d.label}</div>
                  <div style={{ fontSize: 16, fontWeight: 700, color: d.color, marginBottom: 4 }}>{d.value}</div>
                  <div style={{ fontSize: 10, color: "#4b5563", fontFamily: "monospace" }}>{d.sub}</div>
                </div>
              ))}
            </div>
            <div style={{ background: "#111827", borderRadius: 8, padding: "16px 20px", borderLeft: "4px solid #22c55e", marginBottom: 16 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#22c55e", marginBottom: 8 }}>A line is DIFOT when ALL conditions are met:</div>
              <div style={{ fontSize: 13, color: "#d1d5db", lineHeight: 2 }}>
                1. A packing slip is posted — goods physically issued from warehouse<br />
                2. Goods Issue Date &nbsp;<span style={{ background: "#374151", padding: "2px 6px", borderRadius: 4, fontFamily: "monospace" }}>≤</span>&nbsp; ConfirmedShippingDate &nbsp;(delivered on time)<br />
                3. Shipped Qty &nbsp;<span style={{ background: "#374151", padding: "2px 6px", borderRadius: 4, fontFamily: "monospace" }}>≥ 95%</span>&nbsp; of Ordered Qty &nbsp;(delivered in full)
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr", gap: 8 }}>
              {[
                { s: "DIFOT", desc: "On time + in full", color: STATUS_COLORS.DIFOT },
                { s: "Late", desc: "Shipped, but after confirmed date", color: STATUS_COLORS.Late },
                { s: "Partial", desc: "Shipped < 95% of ordered", color: STATUS_COLORS.Partial },
                { s: "Past Due", desc: "Not shipped, confirmed date passed", color: STATUS_COLORS["Past Due"] },
                { s: "Open", desc: "Not shipped, not yet due", color: STATUS_COLORS.Open },
              ].map((d) => (
                <div key={d.s} style={{ background: "#111827", borderRadius: 6, padding: "10px 12px", border: `1px solid ${d.color}44` }}>
                  <div style={{ fontWeight: 700, color: d.color, fontSize: 13, marginBottom: 4 }}>{d.s}</div>
                  <div style={{ fontSize: 11, color: "#6b7280" }}>{d.desc}</div>
                </div>
              ))}
            </div>
          </div>

          {/* Order-to-Ship */}
          <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 28, marginBottom: 20 }}>
            <div style={{ fontSize: 18, fontWeight: 800, color: "#f9fafb", marginBottom: 4 }}>Order-to-Ship Lead Time</div>
            <div style={{ fontSize: 13, color: "#9ca3af", marginBottom: 20 }}>How long it actually takes from order creation to goods leaving the warehouse</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 20 }}>
              {[
                { label: "Start", value: "CreatedDate", sub: "SO.CREATEDDATETIME — order entered in D365", color: "#3b82f6" },
                { label: "End (actual)", value: "Goods Issue Date", sub: "CUSTOMER_PACKING_SLIP_LINES.DELIVERYDATE — packing slip posted", color: "#22c55e" },
              ].map((d) => (
                <div key={d.label} style={{ background: "#111827", borderRadius: 8, padding: "14px 18px", border: `1px solid ${d.color}44` }}>
                  <div style={{ fontSize: 10, color: "#6b7280", textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>{d.label}</div>
                  <div style={{ fontSize: 16, fontWeight: 700, color: d.color, marginBottom: 4 }}>{d.value}</div>
                  <div style={{ fontSize: 11, color: "#4b5563" }}>{d.sub}</div>
                </div>
              ))}
            </div>
            <div style={{ background: "#111827", borderRadius: 8, padding: "16px 20px", borderLeft: "4px solid #3b82f6" }}>
              <div style={{ fontSize: 13, color: "#d1d5db", lineHeight: 2 }}>
                <span style={{ fontFamily: "monospace", background: "#374151", padding: "2px 6px", borderRadius: 4 }}>Lead Time (days) = Goods Issue Date − CreatedDate</span><br />
                Only calculated for lines that have a packing slip posted (goods physically shipped).<br />
                Lines still open show no lead time — they are excluded from the average.
              </div>
            </div>
          </div>

          {/* ERP source fields */}
          {[
            {
              title: "D365 Source Fields (MART_DYN_FO)",
              fields: [
                ["CreatedDate", "SALES_ORDERS", "CREATEDDATETIME", "Order-to-Ship start"],
                ["ConfirmedShippingDate", "ORDER_LINES", "SHIPPINGDATECONFIRMED", "DIFOT start / on-time benchmark"],
                ["RequestedShippingDate", "ORDER_LINES", "SHIPPINGDATEREQUESTED", "Original customer request"],
                ["Goods Issue Date", "CUSTOMER_PACKING_SLIP_LINES", "DELIVERYDATE", "DIFOT end / Order-to-Ship end"],
                ["Shipped Qty", "CUSTOMER_PACKING_SLIP_LINES", "QTY", "In-full check (≥ 95%)"],
                ["Ordered Qty", "ORDER_LINES", "SALESQTY", "In-full denominator"],
                ["Order Value", "ORDER_LINES", "LINEAMOUNT", "Value-weighted DIFOT"],
              ],
            },
            {
              title: "Oracle CEN01 Source Fields (FLS_PROD_DB.RAW_CEN01)",
              fields: [
                ["CreatedDate", "ONT_OE_ORDER_HEADERS_ALL", "BOOKED_DATE", "Order-to-Ship start"],
                ["ConfirmedShippingDate", "ONT_OE_ORDER_LINES_ALL", "PROMISE_DATE", "DIFOT on-time benchmark"],
                ["RequestedShippingDate", "ONT_OE_ORDER_LINES_ALL", "REQUEST_DATE", "Original customer request"],
                ["Goods Issue Date", "VW_SIFOTDATE_TMS_EWB / EXPEDITING_DATA_EWB / RCV_TRANSACTIONS", "ACTUAL_RECEIPT_DATE / PACKSLIP_DATE / SHIPPED_DATE", "DIFOT end (COALESCE priority)"],
                ["Ordered Qty", "ONT_OE_ORDER_LINES_ALL", "ORDERED_QUANTITY", "In-full denominator"],
                ["Order Value", "ONT_OE_ORDER_LINES_ALL", "UNIT_SELLING_PRICE × ORDERED_QUANTITY", "Value-weighted DIFOT"],
              ],
            },
            {
              title: "Epicor Source Fields (VW_EPICOR_SALE_ORDER)",
              fields: [
                ["CreatedDate", "VW_EPICOR_SALE_ORDER", "SALEORDER_ORDERDATE", "Order-to-Ship start"],
                ["ConfirmedShippingDate", "VW_EPICOR_SALE_ORDER", "SO_PROMISE_DATE", "DIFOT on-time benchmark"],
                ["RequestedShippingDate", "VW_EPICOR_SALE_ORDER", "SO_NEEDBY_DATE", "Original customer request"],
                ["Goods Issue Date", "VW_EPICOR_SALE_ORDER", "POFULFILLMENT_DATE", "DIFOT end / Order-to-Ship end"],
                ["Ordered Qty", "VW_EPICOR_SALE_ORDER", "SO_ORDER_QTY", "In-full denominator"],
                ["Order Value", "VW_EPICOR_SALE_ORDER", "SO_UNIT_PRICE × SO_ORDER_QTY", "Value-weighted DIFOT"],
              ],
            },
          ].map((erp) => (
            <div key={erp.title} style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 28, marginBottom: 16 }}>
              <div style={{ fontSize: 14, fontWeight: 700, color: "#d1d5db", marginBottom: 16 }}>{erp.title}</div>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr><Th>Field</Th><Th>Table</Th><Th>Column</Th><Th>Used for</Th></tr>
                </thead>
                <tbody>
                  {erp.fields.map(([field, table, col, use], i) => (
                    <tr key={i} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                      <Td bold color="#d1d5db">{field}</Td>
                      <Td color="#9ca3af"><span style={{ fontFamily: "monospace", fontSize: 11 }}>{table}</span></Td>
                      <Td color="#93c5fd"><span style={{ fontFamily: "monospace", fontSize: 11 }}>{col}</span></Td>
                      <Td color="#6b7280">{use}</Td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
        </div>
      )}

      {/* ── PURCHASED BUT NOT USED TAB ── */}
      {tab === "purchased-not-used" && (() => {
        if (pnuLoading) return <div style={{ padding: 60, textAlign: "center", color: "#9ca3af" }}>Loading purchased-not-used data…</div>;
        if (!pnuRows.length) return <div style={{ padding: 60, textAlign: "center", color: "#6b7280" }}>No data available. Run export first.</div>;

        const companies = [...new Set(pnuRows.map((r) => r.company).filter(Boolean))].sort();
        const reqGroups = [...new Set(pnuRows.map((r) => r.req_group).filter(Boolean))].sort();
        const erpSources = [...new Set(pnuRows.map((r) => r.erp_source).filter(Boolean))].sort();

        const sorted = [...pnuFiltered].sort((a, b) => {
          const av = parseFloat(a[pnuSortCol]) || 0;
          const bv = parseFloat(b[pnuSortCol]) || 0;
          return pnuSortDir * (bv - av);
        });
        const totalPages = Math.ceil(sorted.length / PNU_PAGE_SIZE);
        const pageRows = sorted.slice(pnuPage * PNU_PAGE_SIZE, (pnuPage + 1) * PNU_PAGE_SIZE);
        const filteredValM = pnuFiltered.reduce((s, r) => s + (parseFloat(r.stock_value_dkk) || 0), 0) / 1e6;

        const selStyle = { background: "#111827", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 12, width: "100%" };
        const lblStyle = { display: "block", fontSize: 10, color: "#6b7280", marginBottom: 3, textTransform: "uppercase", letterSpacing: 1 };

        const SortTh = ({ col, label, right }) => (
          <th onClick={() => { if (pnuSortCol === col) setPnuSortDir(-pnuSortDir); else { setPnuSortCol(col); setPnuSortDir(-1); } setPnuPage(0); }}
            style={{ padding: "9px 12px", color: pnuSortCol === col ? "#f9fafb" : "#6b7280", fontWeight: 700, fontSize: 11,
              textAlign: right ? "right" : "left", borderBottom: "1px solid #374151", whiteSpace: "nowrap",
              background: "#111827", cursor: "pointer", userSelect: "none" }}>
            {label}{pnuSortCol === col ? (pnuSortDir === -1 ? " ↓" : " ↑") : ""}
          </th>
        );

        const resetFilters = () => {
          setPnuCompany("all"); setPnuReqGroup("all"); setPnuErpSource("all"); setPnuItemSearch(""); setPnuMinValue(""); setPnuPage(0);
        };

        return (
          <>
            {/* Hero cards */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 14, marginBottom: 20 }}>
              <KpiCard label="Total Stock Value" value={`${pnuStats.totalValM.toFixed(0)}M DKK`} sub={`${pnuStats.withStockCount.toLocaleString()} items with stock on hand`} color="#ef4444" />
              <KpiCard label="Unique Items" value={pnuStats.uniqueItems.toLocaleString()} sub="received in last 6 months, no demand" color="#f97316" />
              <KpiCard label="Companies" value={pnuStats.companies.toLocaleString()} sub="with unplanned inventory" color="#a855f7" />
              <KpiCard label="Avg Days Since Receipt" value={pnuStats.avgDays.toLocaleString()} sub="since last PO receipt" color="#f59e0b" />
            </div>

            {/* Filter bar */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 16, marginBottom: 16 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                <span style={{ fontSize: 12, fontWeight: 700, color: "#9ca3af" }}>Filters</span>
                <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                  <span style={{ fontSize: 12, color: "#6b7280" }}>
                    <span style={{ color: "#f9fafb", fontWeight: 700 }}>{pnuFiltered.length.toLocaleString()}</span> rows
                    &nbsp;·&nbsp;
                    <span style={{ color: "#f9fafb", fontWeight: 700 }}>{filteredValM.toFixed(0)}M DKK</span>
                  </span>
                  <button onClick={resetFilters}
                    style={{ padding: "4px 10px", background: "#374151", border: "none", borderRadius: 4, color: "#9ca3af", cursor: "pointer", fontSize: 11 }}>
                    Reset
                  </button>
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr 160px", gap: 10 }}>
                <div>
                  <label style={lblStyle}>Company</label>
                  <select style={selStyle} value={pnuCompany} onChange={(e) => { setPnuCompany(e.target.value); setPnuPage(0); }}>
                    <option value="all">All Companies</option>
                    {companies.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>ERP Source</label>
                  <select style={selStyle} value={pnuErpSource} onChange={(e) => { setPnuErpSource(e.target.value); setPnuPage(0); }}>
                    <option value="all">All ERPs</option>
                    {erpSources.map((e) => <option key={e} value={e}>{e}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Planning Group</label>
                  <select style={selStyle} value={pnuReqGroup} onChange={(e) => { setPnuReqGroup(e.target.value); setPnuPage(0); }}>
                    <option value="all">All Groups</option>
                    {reqGroups.map((g) => <option key={g} value={g}>{g}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Item Search</label>
                  <input value={pnuItemSearch} onChange={(e) => { setPnuItemSearch(e.target.value); setPnuPage(0); }}
                    placeholder="Item ID or description…"
                    style={{ ...selStyle, boxSizing: "border-box" }} />
                </div>
                <div>
                  <label style={lblStyle}>Min Value (DKK)</label>
                  <input type="number" value={pnuMinValue} onChange={(e) => { setPnuMinValue(e.target.value); setPnuPage(0); }}
                    placeholder="e.g. 50000"
                    style={{ ...selStyle, boxSizing: "border-box" }} />
                </div>
              </div>
            </div>

            {/* Chart */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 16, marginBottom: 16 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#9ca3af", marginBottom: 12 }}>Stock Value by Company (DKK) — all data</div>
              <ResponsiveContainer width="100%" height={160}>
                <BarChart data={pnuCompanyChart} margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="company" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} tickFormatter={(v) => v >= 1e6 ? `${(v/1e6).toFixed(0)}M` : v >= 1e3 ? `${(v/1e3).toFixed(0)}K` : v} />
                  <Tooltip contentStyle={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 6 }}
                    formatter={(v) => [v >= 1e6 ? `${(v/1e6).toFixed(1)}M DKK` : `${(v/1e3).toFixed(0)}K DKK`, "Stock Value"]} />
                  <Bar dataKey="value" fill="#f97316" radius={[3,3,0,0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Detail table */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, overflow: "hidden" }}>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr>
                      <Th>Company</Th>
                      <Th>ERP</Th>
                      <Th>Item</Th>
                      <Th>Description</Th>
                      <Th>Purchase Order</Th>
                      <SortTh col="po_created_date" label="PO Created" />
                      <SortTh col="last_receipt_date" label="Last Receipt" />
                      <SortTh col="days_since_receipt" label="Days Since" right />
                      <SortTh col="received_qty" label="Rcvd Qty" right />
                      <SortTh col="current_stock" label="On Hand" right />
                      <Th>Currency</Th>
                      <SortTh col="unit_price" label="Unit Price" right />
                      <SortTh col="stock_value_dkk" label="Value DKK" right />
                      <Th>Planning Group</Th>
                      <SortTh col="po_count" label="POs" right />
                    </tr>
                  </thead>
                  <tbody>
                    {pageRows.map((r, i) => {
                      const valDkk = parseFloat(r.stock_value_dkk) || 0;
                      const valColor = valDkk > 1e6 ? "#ef4444" : valDkk > 1e5 ? "#f97316" : "#d1d5db";
                      return (
                        <tr key={i} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                          <Td><span style={{ fontSize: 11, fontWeight: 700, color: "#93c5fd" }}>{r.company}</span></Td>
                          <Td><span style={{ fontSize: 10, background: r.erp_source === "D365" ? "#3b82f622" : "#a855f722", color: r.erp_source === "D365" ? "#93c5fd" : "#c4b5fd", border: `1px solid ${r.erp_source === "D365" ? "#3b82f655" : "#a855f755"}`, borderRadius: 3, padding: "2px 6px" }}>{r.erp_source}</span></Td>
                          <Td><span style={{ fontFamily: "monospace", color: "#93c5fd", fontSize: 11 }}>{r.item_id}</span></Td>
                          <Td><span style={{ fontSize: 11, color: "#9ca3af", maxWidth: 220, display: "inline-block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.item_description || "—"}</span></Td>
                          <Td><span style={{ fontFamily: "monospace", fontSize: 11 }}>{r.purchase_order || "—"}</span></Td>
                          <Td><span style={{ fontSize: 11, color: "#6b7280" }}>{r.po_created_date?.slice(0,10) || "—"}</span></Td>
                          <Td><span style={{ fontSize: 11, color: "#9ca3af" }}>{r.last_receipt_date?.slice(0,10) || "—"}</span></Td>
                          <Td right><span style={{ color: parseFloat(r.days_since_receipt) > 90 ? "#f59e0b" : "#d1d5db" }}>{r.days_since_receipt}</span></Td>
                          <Td right>{parseFloat(r.received_qty).toLocaleString()}</Td>
                          <Td right><span style={{ fontWeight: 700 }}>{parseFloat(r.current_stock).toLocaleString()}</span></Td>
                          <Td><span style={{ fontSize: 11, color: "#9ca3af" }}>{r.currency || "—"}</span></Td>
                          <Td right>{parseFloat(r.unit_price).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</Td>
                          <Td right bold><span style={{ color: valColor }}>{valDkk >= 1e6 ? `${(valDkk/1e6).toFixed(1)}M` : valDkk >= 1e3 ? `${(valDkk/1e3).toFixed(0)}K` : valDkk.toFixed(0)}</span></Td>
                          <Td><span style={{ fontSize: 10, background: "#374151", borderRadius: 3, padding: "2px 6px", color: "#9ca3af" }}>{r.req_group}</span></Td>
                          <Td right>{r.po_count}</Td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              {totalPages > 1 && (
                <div style={{ padding: "12px 16px", display: "flex", gap: 8, alignItems: "center", borderTop: "1px solid #374151" }}>
                  <button onClick={() => setPnuPage(Math.max(0, pnuPage - 1))} disabled={pnuPage === 0}
                    style={{ padding: "5px 12px", background: "#374151", border: "none", borderRadius: 4, color: "#d1d5db", cursor: pnuPage === 0 ? "default" : "pointer", opacity: pnuPage === 0 ? 0.4 : 1 }}>← Prev</button>
                  <span style={{ fontSize: 12, color: "#6b7280" }}>Page {pnuPage + 1} of {totalPages} ({sorted.length.toLocaleString()} rows)</span>
                  <button onClick={() => setPnuPage(Math.min(totalPages - 1, pnuPage + 1))} disabled={pnuPage === totalPages - 1}
                    style={{ padding: "5px 12px", background: "#374151", border: "none", borderRadius: 4, color: "#d1d5db", cursor: pnuPage === totalPages - 1 ? "default" : "pointer", opacity: pnuPage === totalPages - 1 ? 0.4 : 1 }}>Next →</button>
                </div>
              )}
            </div>
          </>
        );
      })()}

      {/* ── CROSS-SITE OPPORTUNITY TAB ── */}
      {tab === "cross-site" && (() => {
        if (csLoading) return <div style={{ padding: 60, textAlign: "center", color: "#9ca3af" }}>Loading cross-site data…</div>;
        if (!csRows.length) return <div style={{ padding: 60, textAlign: "center", color: "#6b7280" }}>No cross-site data available. Run export first.</div>;

        const csErps = [...new Set(csRows.map((r) => r.demand_erp).filter(Boolean))].sort();
        const coverageTypes = ["Full Cover", "Partial Cover"];
        const statusTypes = ["Past Due", "Open"];

        const sorted = [...csFiltered].sort((a, b) => {
          const av = parseFloat(a[csSortCol]) || 0;
          const bv = parseFloat(b[csSortCol]) || 0;
          return csSortDir * (bv - av);
        });
        const totalPages = Math.ceil(sorted.length / CS_PAGE_SIZE);
        const pageRows = sorted.slice(csPage * CS_PAGE_SIZE, (csPage + 1) * CS_PAGE_SIZE);

        const covColor = (c) => c === "Full Cover" ? "#22c55e" : "#f59e0b";
        const statusColor = (s) => s === "Past Due" ? "#ef4444" : "#3b82f6";

        const SortTh = ({ col, label, right }) => (
          <th onClick={() => { if (csSortCol === col) setCsSortDir(-csSortDir); else { setCsSortCol(col); setCsSortDir(-1); } setCsPage(0); }}
            style={{ padding: "9px 12px", color: csSortCol === col ? "#f9fafb" : "#6b7280", fontWeight: 700, fontSize: 11,
              textAlign: right ? "right" : "left", borderBottom: "1px solid #374151", whiteSpace: "nowrap",
              background: "#111827", cursor: "pointer", userSelect: "none" }}>
            {label}{csSortCol === col ? (csSortDir === -1 ? " ↓" : " ↑") : ""}
          </th>
        );

        const demandSites = [...new Set(csRows.map((r) => r.demand_site).filter(Boolean))].sort();
        const stockErps = [...new Set(csRows.map((r) => r.stock_erp).filter(Boolean))].sort();
        const stockCountries = [...new Set(csRows.map((r) => r.stock_country).filter(Boolean))].sort();
        const warehouses = [...new Set(csRows.map((r) => r.warehouse).filter(Boolean))].sort();
        const productFamilies = [...new Set(csRows.map((r) => r.product_family).filter(Boolean))].sort();
        const filteredValM = csFiltered.reduce((s, r) => s + (parseFloat(r.value_dkk_k) || 0), 0) / 1000;

        const selStyle = { background: "#111827", border: "1px solid #374151", color: "#f9fafb", borderRadius: 6, padding: "6px 10px", fontSize: 12, width: "100%" };
        const lblStyle = { display: "block", fontSize: 10, color: "#6b7280", marginBottom: 3, textTransform: "uppercase", letterSpacing: 1 };

        const resetFilters = () => {
          setCsERP("all"); setCsCoverage("all"); setCsStatus("Past Due");
          setCsStockERP("all"); setCsStockCountry("all"); setCsWarehouse("all"); setCsDemandSite("all");
          setCsProductFamily("all"); setCsItemSearch(""); setCsMinValue("");
          setCsPage(0);
        };

        return (
          <>
            {/* Hero cards */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 14, marginBottom: 20 }}>
              <KpiCard label="Past Due + Full Cover" value={`${csStats.pdFullValM.toFixed(0)}M DKK`} sub={`${csStats.pdFullRows.toLocaleString()} order lines`} color="#ef4444" />
              <KpiCard label="Total Cross-Site Value" value={`${csStats.totalValM.toFixed(0)}M DKK`} sub={`${csStats.totalRows.toLocaleString()} matches`} color="#3b82f6" />
              <KpiCard label="Unique Items" value={csStats.uniqueItems.toLocaleString()} sub="matched across sites" color="#a855f7" />
              <KpiCard label="Demand Sites" value={csStats.uniqueDemandSites.toLocaleString()} sub="with cross-site stock available" color="#f59e0b" />
            </div>

            {/* Filter bar */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 16, marginBottom: 16 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                <span style={{ fontSize: 12, fontWeight: 700, color: "#9ca3af" }}>Filters</span>
                <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                  <span style={{ fontSize: 12, color: "#6b7280" }}>
                    <span style={{ color: "#f9fafb", fontWeight: 700 }}>{csFiltered.length.toLocaleString()}</span> rows
                    &nbsp;·&nbsp;
                    <span style={{ color: "#f9fafb", fontWeight: 700 }}>{filteredValM.toFixed(0)}M DKK</span>
                  </span>
                  <button onClick={resetFilters}
                    style={{ padding: "4px 10px", background: "#374151", border: "none", borderRadius: 4, color: "#9ca3af", cursor: "pointer", fontSize: 11 }}>
                    Reset
                  </button>
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr) repeat(2,140px) 1fr 1fr", gap: 10, alignItems: "end" }}>
                <div>
                  <label style={lblStyle}>Demand ERP</label>
                  <select style={selStyle} value={csERP} onChange={(e) => { setCsERP(e.target.value); setCsPage(0); }}>
                    <option value="all">All ERPs</option>
                    {csErps.map((e) => <option key={e} value={e}>{e}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Demand Site</label>
                  <select style={selStyle} value={csDemandSite} onChange={(e) => { setCsDemandSite(e.target.value); setCsPage(0); }}>
                    <option value="all">All Sites</option>
                    {demandSites.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Stock ERP</label>
                  <select style={selStyle} value={csStockERP} onChange={(e) => { setCsStockERP(e.target.value); setCsPage(0); }}>
                    <option value="all">All Stock ERPs</option>
                    {stockErps.map((e) => <option key={e} value={e}>{e}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Stock Country</label>
                  <select style={selStyle} value={csStockCountry} onChange={(e) => { setCsStockCountry(e.target.value); setCsPage(0); }}>
                    <option value="all">All Countries</option>
                    {stockCountries.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Warehouse</label>
                  <select style={selStyle} value={csWarehouse} onChange={(e) => { setCsWarehouse(e.target.value); setCsPage(0); }}>
                    <option value="all">All Warehouses</option>
                    {warehouses.map((w) => <option key={w} value={w}>{w}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Coverage</label>
                  <select style={selStyle} value={csCoverage} onChange={(e) => { setCsCoverage(e.target.value); setCsPage(0); }}>
                    <option value="all">All</option>
                    {coverageTypes.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Status</label>
                  <select style={selStyle} value={csStatus} onChange={(e) => { setCsStatus(e.target.value); setCsPage(0); }}>
                    <option value="all">All</option>
                    {statusTypes.map((s) => <option key={s} value={s}>{s}</option>)}
                  </select>
                </div>
                <div>
                  <label style={lblStyle}>Product Family</label>
                  <select style={selStyle} value={csProductFamily} onChange={(e) => { setCsProductFamily(e.target.value); setCsPage(0); }}>
                    <option value="all">All Families</option>
                    {productFamilies.map((p) => <option key={p} value={p}>{p}</option>)}
                  </select>
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 180px", gap: 10, marginTop: 10 }}>
                <div>
                  <label style={lblStyle}>Item Search</label>
                  <input value={csItemSearch} onChange={(e) => { setCsItemSearch(e.target.value); setCsPage(0); }}
                    placeholder="Type item ID…"
                    style={{ ...selStyle, width: "100%", boxSizing: "border-box" }} />
                </div>
                <div>
                  <label style={lblStyle}>Min Value (DKK K)</label>
                  <input type="number" value={csMinValue} onChange={(e) => { setCsMinValue(e.target.value); setCsPage(0); }}
                    placeholder="e.g. 100"
                    style={{ ...selStyle, width: "100%", boxSizing: "border-box" }} />
                </div>
              </div>
            </div>

            {/* Chart */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, padding: 16, marginBottom: 16 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#9ca3af", marginBottom: 12 }}>Value by Demand ERP (DKK K) — filtered view</div>
              <ResponsiveContainer width="100%" height={160}>
                <BarChart data={csErpChart} margin={{ top: 0, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis dataKey="erp" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} tickFormatter={(v) => v >= 1000 ? `${(v/1000).toFixed(0)}M` : `${v}K`} />
                  <Tooltip contentStyle={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 6 }} formatter={(v) => [`${v.toFixed(0)} DKK K`, ""]} />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                  <Bar dataKey="Past Due Full" stackId="a" fill="#ef4444" />
                  <Bar dataKey="Past Due Partial" stackId="a" fill="#f97316" />
                  <Bar dataKey="Open Full" stackId="a" fill="#22c55e" />
                  <Bar dataKey="Open Partial" stackId="a" fill="#86efac" />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Detail table */}
            <div style={{ background: "#1f2937", border: "1px solid #374151", borderRadius: 10, overflow: "hidden" }}>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                  <thead>
                    <tr>
                      <Th>Item</Th><Th>Product Family</Th>
                      <Th>Demand ERP</Th><Th>Demand Site</Th><Th>Order #</Th>
                      <SortTh col="ordered_qty" label="Ord Qty" right />
                      <Th>Ship Date</Th>
                      <SortTh col="days_overdue" label="Days Overdue" right />
                      <SortTh col="value_dkk_k" label="Value DKK K" right />
                      <Th>Coverage</Th><Th>Status</Th>
                      <Th>Stock Site</Th><Th>Stock Country</Th><Th>Warehouse</Th><Th>Stock ERP</Th>
                      <SortTh col="onhand_qty" label="On Hand" right />
                      <SortTh col="qty_surplus" label="Surplus" right />
                    </tr>
                  </thead>
                  <tbody>
                    {pageRows.map((r, i) => (
                      <tr key={i} style={{ background: i % 2 === 0 ? "#1f2937" : "#111827", borderBottom: "1px solid #1f2937" }}>
                        <Td><span style={{ fontFamily: "monospace", color: "#93c5fd", fontSize: 11 }}>{r.item_id}</span></Td>
                        <Td><span style={{ fontSize: 11, color: "#9ca3af" }}>{r.product_family || "—"}</span></Td>
                        <Td><span style={{ fontSize: 11 }}>{r.demand_erp}</span></Td>
                        <Td><span style={{ fontSize: 11 }}>{r.demand_site}</span></Td>
                        <Td><span style={{ fontFamily: "monospace", fontSize: 11 }}>{r.order_number}</span></Td>
                        <Td right>{parseFloat(r.ordered_qty).toLocaleString()}</Td>
                        <Td><span style={{ fontSize: 11, color: "#9ca3af" }}>{r.confirmed_ship_date?.slice(0, 10) || "—"}</span></Td>
                        <Td right><span style={{ color: parseFloat(r.days_overdue) > 0 ? "#ef4444" : "#22c55e", fontWeight: 700 }}>
                          {parseFloat(r.days_overdue) > 0 ? r.days_overdue : `in ${Math.abs(parseFloat(r.days_overdue))}d`}
                        </span></Td>
                        <Td right bold>{parseFloat(r.value_dkk_k).toLocaleString(undefined, { maximumFractionDigits: 0 })}</Td>
                        <Td><span style={{ background: covColor(r.coverage_type) + "22", color: covColor(r.coverage_type), border: `1px solid ${covColor(r.coverage_type)}55`, borderRadius: 4, padding: "2px 7px", fontSize: 10, fontWeight: 700 }}>{r.coverage_type}</span></Td>
                        <Td><span style={{ background: statusColor(r.demand_status) + "22", color: statusColor(r.demand_status), border: `1px solid ${statusColor(r.demand_status)}55`, borderRadius: 4, padding: "2px 7px", fontSize: 10, fontWeight: 700 }}>{r.demand_status}</span></Td>
                        <Td><span style={{ fontSize: 11 }}>{r.stock_site}</span></Td>
                        <Td><span style={{ fontSize: 11, color: "#9ca3af" }}>{r.stock_country}</span></Td>
                        <Td><span style={{ fontFamily: "monospace", fontSize: 11, color: "#a78bfa" }}>{r.warehouse || "—"}</span></Td>
                        <Td><span style={{ fontSize: 11 }}>{r.stock_erp}</span></Td>
                        <Td right>{parseFloat(r.onhand_qty).toLocaleString()}</Td>
                        <Td right><span style={{ color: parseFloat(r.qty_surplus) >= 0 ? "#22c55e" : "#f59e0b" }}>
                          {parseFloat(r.qty_surplus) >= 0 ? "+" : ""}{parseFloat(r.qty_surplus).toLocaleString()}
                        </span></Td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {totalPages > 1 && (
                <div style={{ padding: "12px 16px", display: "flex", gap: 8, alignItems: "center", borderTop: "1px solid #374151" }}>
                  <button onClick={() => setCsPage(Math.max(0, csPage - 1))} disabled={csPage === 0}
                    style={{ padding: "5px 12px", background: "#374151", border: "none", borderRadius: 4, color: "#d1d5db", cursor: csPage === 0 ? "default" : "pointer", opacity: csPage === 0 ? 0.4 : 1 }}>← Prev</button>
                  <span style={{ fontSize: 12, color: "#6b7280" }}>Page {csPage + 1} of {totalPages} ({sorted.length.toLocaleString()} rows)</span>
                  <button onClick={() => setCsPage(Math.min(totalPages - 1, csPage + 1))} disabled={csPage === totalPages - 1}
                    style={{ padding: "5px 12px", background: "#374151", border: "none", borderRadius: 4, color: "#d1d5db", cursor: csPage === totalPages - 1 ? "default" : "pointer", opacity: csPage === totalPages - 1 ? 0.4 : 1 }}>Next →</button>
                </div>
              )}
            </div>
          </>
        );
      })()}
    </div>
  );
}
