import { useState } from "react";
import Dashboard from "./pages/Dashboard";
import InventoryPage from "./pages/InventoryPage";
import ForecastPage from "./pages/ForecastPage";
import PurchaseOrdersPage from "./pages/PurchaseOrdersPage";
import SalesOrdersPage from "./pages/SalesOrdersPage";
import SupplyDemandPage from "./pages/SupplyDemandPage";
import MLForecastPage from "./pages/MLForecastPage";
import SlowMovingPage from "./pages/SlowMovingPage";
import "./App.css";

const PAGES = [
  { id: "dashboard", label: "Dashboard" },
  { id: "forecast", label: "Demand Forecast" },
  { id: "ml-forecast", label: "ML Forecast" },
  { id: "inventory", label: "Inventory" },
  { id: "purchase-orders", label: "Purchase Orders" },
  { id: "sales-orders", label: "Sales Order Lines" },
  { id: "supply-demand", label: "Supply vs Demand" },
  { id: "slow-moving", label: "Slow-Moving Items" },
];

export default function App() {
  const [page, setPage] = useState("dashboard");

  return (
    <div className="app">
      <nav className="sidebar">
        <div className="logo">
          <span className="logo-icon">⬡</span>
          <span className="logo-text">SIOP Manager</span>
        </div>
        <ul>
          {PAGES.map((p) => (
            <li
              key={p.id}
              className={page === p.id ? "active" : ""}
              onClick={() => setPage(p.id)}
            >
              {p.label}
            </li>
          ))}
        </ul>
        <div className="sidebar-footer">FLSmidth · Supply &amp; Ops</div>
      </nav>
      <main className="content">
        {page === "dashboard" && <Dashboard onNavigate={setPage} />}
        {page === "inventory" && <InventoryPage />}
        {page === "forecast" && <ForecastPage />}
        {page === "purchase-orders" && <PurchaseOrdersPage />}
        {page === "sales-orders" && <SalesOrdersPage />}
        {page === "supply-demand" && <SupplyDemandPage />}
        {page === "ml-forecast" && <MLForecastPage />}
        {page === "slow-moving" && <SlowMovingPage />}
      </main>
    </div>
  );
}
