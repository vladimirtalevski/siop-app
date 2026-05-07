const TERMINALS = [
  {
    id: "siop",
    label: "SIOP & Planning",
    icon: "📊",
    color: "#0ea5e9",
    colorBg: "rgba(14,165,233,0.07)",
    description: "Inventory health, demand signals and supply gap analysis",
    pages: [
      { id: "dashboard",     label: "Dashboard",         desc: "KPI overview & alerts" },
      { id: "forecast",      label: "Demand Forecast",   desc: "Monthly demand by model" },
      { id: "ml-forecast",   label: "ML Forecast",       desc: "Machine learning predictions" },
      { id: "supply-demand", label: "Supply vs Demand",  desc: "Gap analysis by site" },
    ],
  },
  {
    id: "procurement",
    label: "Procurement",
    icon: "📦",
    color: "#f97316",
    colorBg: "rgba(249,115,22,0.07)",
    description: "Open POs, overdue lines and expedite priorities",
    pages: [
      { id: "purchase-orders", label: "Purchase Orders",  desc: "Open PO lines & value" },
      { id: "expedite",        label: "Expedite Report",  desc: "At-risk deliveries" },
    ],
  },
  {
    id: "sales",
    label: "Sales & Delivery",
    icon: "🚀",
    color: "#22c55e",
    colorBg: "rgba(34,197,94,0.07)",
    description: "Pipeline visibility, DIFOT performance and OTS tracking",
    pages: [
      { id: "sales-orders", label: "Sales Order Lines",    desc: "Open order pipeline" },
      { id: "speed-up",     label: "Speed Up Dashboard",   desc: "DIFOT, lead time & value buckets" },
    ],
  },
  {
    id: "inventory",
    label: "Inventory",
    icon: "🏭",
    color: "#a855f7",
    colorBg: "rgba(168,85,247,0.07)",
    description: "On-hand stock levels and slow-moving risk identification",
    pages: [
      { id: "inventory",    label: "Inventory",           desc: "On-hand by site & item" },
      { id: "slow-moving",  label: "Slow-Moving Items",   desc: "Excess & aging stock" },
    ],
  },
  {
    id: "intelligence",
    label: "Intelligence",
    icon: "🤖",
    color: "#64748b",
    colorBg: "rgba(100,116,139,0.07)",
    description: "AI-powered queries and data completeness checks",
    pages: [
      { id: "chat",         label: "AI Assistant",   desc: "Ask questions about your data" },
      { id: "data-quality", label: "Data Quality",   desc: "Completeness & freshness scores" },
    ],
  },
];

const ACCENT_COLORS = {
  "SIOP & Planning":   "#0ea5e9",
  "Procurement":       "#f97316",
  "Sales & Delivery":  "#22c55e",
  "Inventory":         "#a855f7",
  "Intelligence":      "#64748b",
};

export default function HomePage({ onNavigate }) {
  const now = new Date();
  const dateStr = now.toLocaleDateString("en-GB", {
    weekday: "long", year: "numeric", month: "long", day: "numeric",
  });

  return (
    <div className="home-page">
      {/* ── Header ── */}
      <div className="home-header">
        <div className="home-header-left">
          <div className="home-title">
            <span className="home-plane">✈</span>
            SIOP Control Tower
          </div>
          <div className="home-subtitle">FLSmidth · Supply &amp; Operations · {dateStr}</div>
        </div>
        <div className="home-tagline">Select your destination</div>
      </div>

      {/* ── Departures board ── */}
      <div className="terminal-grid">
        {TERMINALS.map((t) => (
          <div
            key={t.id}
            className="terminal-card"
            style={{ "--t-accent": t.color, "--t-bg": t.colorBg }}
          >
            {/* Card top accent line */}
            <div className="terminal-accent-bar" />

            <div className="terminal-top">
              <span className="terminal-icon">{t.icon}</span>
              <div>
                <div className="terminal-name">{t.label}</div>
                <div className="terminal-description">{t.description}</div>
              </div>
            </div>

            {/* Gate list */}
            <div className="terminal-gates">
              {t.pages.map((p) => (
                <button
                  key={p.id}
                  className="gate-btn"
                  onClick={() => onNavigate(p.id)}
                >
                  <div className="gate-info">
                    <span className="gate-label">{p.label}</span>
                    <span className="gate-desc">{p.desc}</span>
                  </div>
                  <span className="gate-arrow">→</span>
                </button>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* ── Bottom status bar (airport departures feel) ── */}
      <div className="home-status-bar">
        <span className="status-dot green" /> Live data · Snowflake
        <span className="status-sep">|</span>
        {TERMINALS.reduce((acc, t) => acc + t.pages.length, 0)} modules available
        <span className="status-sep">|</span>
        Last updated: {now.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })}
      </div>
    </div>
  );
}
