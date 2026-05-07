import { useState } from "react";
import HomePage from "./pages/HomePage";
import Dashboard from "./pages/Dashboard";
import InventoryPage from "./pages/InventoryPage";
import ForecastPage from "./pages/ForecastPage";
import PurchaseOrdersPage from "./pages/PurchaseOrdersPage";
import SalesOrdersPage from "./pages/SalesOrdersPage";
import SupplyDemandPage from "./pages/SupplyDemandPage";
import MLForecastPage from "./pages/MLForecastPage";
import SlowMovingPage from "./pages/SlowMovingPage";
import ExpeditePage from "./pages/ExpeditePage";
import ChatPage from "./pages/ChatPage";
import DataQualityPage from "./pages/DataQualityPage";
import SpeedUpDashboard from "./pages/SpeedUpDashboard";
import "./App.css";

const GROUPS = [
  {
    label: "SIOP & Planning",
    color: "#0ea5e9",
    pages: [
      { id: "dashboard",     label: "Dashboard" },
      { id: "forecast",      label: "Demand Forecast" },
      { id: "ml-forecast",   label: "ML Forecast" },
      { id: "supply-demand", label: "Supply vs Demand" },
    ],
  },
  {
    label: "Procurement",
    color: "#f97316",
    pages: [
      { id: "purchase-orders", label: "Purchase Orders" },
      { id: "expedite",        label: "Expedite Report" },
    ],
  },
  {
    label: "Sales & Delivery",
    color: "#22c55e",
    pages: [
      { id: "sales-orders", label: "Sales Order Lines" },
      { id: "speed-up",     label: "Speed Up" },
    ],
  },
  {
    label: "Inventory",
    color: "#a855f7",
    pages: [
      { id: "inventory",   label: "Inventory" },
      { id: "slow-moving", label: "Slow-Moving Items" },
    ],
  },
  {
    label: "Intelligence",
    color: "#64748b",
    pages: [
      { id: "chat",         label: "AI Assistant" },
      { id: "data-quality", label: "Data Quality" },
    ],
  },
];

export default function App() {
  const [page, setPage] = useState("home");

  return (
    <div className="app">
      <nav className="sidebar">
        <div className="logo">
          <span className="logo-icon">✈</span>
          <span className="logo-text">SIOP Manager</span>
        </div>

        {/* Home / Control Tower link */}
        <ul style={{ marginBottom: 0 }}>
          <li
            className={page === "home" ? "active" : ""}
            onClick={() => setPage("home")}
            style={{ marginTop: 12 }}
          >
            🏠 Control Tower
          </li>
        </ul>

        {/* Grouped navigation */}
        {GROUPS.map((g) => (
          <div key={g.label}>
            <div
              className="sidebar-group-label"
              style={{ "--g-color": g.color }}
            >
              {g.label}
            </div>
            <ul style={{ marginTop: 0 }}>
              {g.pages.map((p) => (
                <li
                  key={p.id}
                  className={page === p.id ? "active" : ""}
                  onClick={() => setPage(p.id)}
                >
                  {p.label}
                </li>
              ))}
            </ul>
          </div>
        ))}

        <div className="sidebar-footer">FLSmidth · Supply &amp; Ops</div>
      </nav>

      <main className="content">
        {page === "home"           && <HomePage onNavigate={setPage} />}
        {page === "dashboard"      && <Dashboard onNavigate={setPage} />}
        {page === "inventory"      && <InventoryPage />}
        {page === "forecast"       && <ForecastPage />}
        {page === "purchase-orders"&& <PurchaseOrdersPage />}
        {page === "sales-orders"   && <SalesOrdersPage />}
        {page === "supply-demand"  && <SupplyDemandPage />}
        {page === "ml-forecast"    && <MLForecastPage />}
        {page === "slow-moving"    && <SlowMovingPage />}
        {page === "expedite"       && <ExpeditePage />}
        {page === "chat"           && <ChatPage />}
        {page === "data-quality"   && <DataQualityPage />}
        {page === "speed-up"       && <SpeedUpDashboard />}
      </main>
    </div>
  );
}
