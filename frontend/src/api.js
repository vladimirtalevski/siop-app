import axios from "axios";

const USE_STATIC = import.meta.env.VITE_USE_STATIC === "true";
const BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

const api = axios.create({ baseURL: BASE, timeout: 120000 }); // 2-min timeout for Snowflake queries

// Static JSON loaders (used in Vercel deployment)
const loadStatic = (name) =>
  fetch(`/data/${name}.json`).then((r) => r.json());

export const fetchInventorySummary = () =>
  USE_STATIC ? loadStatic("inventory_summary") : api.get("/api/inventory/summary").then((r) => r.data);

export const fetchInventory = () =>
  USE_STATIC ? loadStatic("inventory") : api.get("/api/inventory").then((r) => r.data);

export const fetchForecastByMonth = (params) =>
  USE_STATIC ? loadStatic("forecast_by_month") : api.get("/api/demand-forecast/by-month", { params }).then((r) => r.data);

export const fetchForecast = (params) =>
  USE_STATIC ? loadStatic("forecast") : api.get("/api/demand-forecast", { params }).then((r) => r.data);

export const fetchOpenPOs = () =>
  USE_STATIC ? loadStatic("purchase_orders") : api.get("/api/purchase-orders/open").then((r) => r.data);

export const fetchPOSummary = () =>
  USE_STATIC
    ? loadStatic("purchase_orders").then((rows) => {
        const map = {};
        rows.forEach((r) => {
          const c = r.company || r.COMPANY;
          const s = r.po_status || r.PO_STATUS || r.status;
          const key = `${c}-${s}`;
          if (!map[key]) map[key] = { company: c, status: s, po_count: 0, line_count: 0, open_value: 0 };
          map[key].line_count++;
          // new format: remaining_value or order_price; old format: line_amount
          const val =
            parseFloat(r.remaining_value || r.REMAINING_VALUE) ||
            parseFloat(r.order_price || r.ORDER_PRICE) ||
            parseFloat(r.line_amount || r.LINE_AMOUNT) ||
            (parseFloat(r.remaining_qty || r.REMAINING_QTY) * parseFloat(r.unit_price || r.UNIT_PRICE)) ||
            0;
          map[key].open_value += isFinite(val) ? val : 0;
        });
        return Object.values(map);
      })
    : api.get("/api/purchase-orders/summary").then((r) => r.data);

export const fetchSalesPipeline = () =>
  USE_STATIC ? loadStatic("sales_orders") : api.get("/api/sales-orders/pipeline").then((r) => r.data);

export const fetchSalesOrderLines = (params) =>
  USE_STATIC ? loadStatic("sales_orders") : api.get("/api/sales-orders/pipeline", { params }).then((r) => r.data);

export const fetchSupplyDemandGap = () =>
  USE_STATIC ? loadStatic("supply_demand_gap") : api.get("/api/supply-demand-gap").then((r) => r.data);

export const fetchCompanies = () =>
  USE_STATIC
    ? loadStatic("inventory_summary").then((rows) => [...new Set(rows.map((r) => r.company))].filter(Boolean).sort())
    : api.get("/api/meta/companies").then((r) => r.data);

export const fetchForecastModels = () =>
  USE_STATIC
    ? loadStatic("forecast_by_month").then((rows) => [...new Map(rows.map((r) => [`${r.company}-${r.model_id}`, r])).values()])
    : api.get("/api/meta/forecast-models").then((r) => r.data);

export const fetchBOMForecast = (params) =>
  USE_STATIC ? loadStatic("forecast_bom") : api.get("/api/forecast/bom", { params }).then((r) => r.data);

export const fetchMLForecast = (params) =>
  api.get("/api/ml/forecast", { params }).then((r) => r.data);

export const fetchSlowMoving = (params) =>
  USE_STATIC ? loadStatic("slow_moving") : api.get("/api/slow-moving", { params }).then((r) => r.data);
