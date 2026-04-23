"""
Export live data from Railway backend to static JSON files for Vercel demo.
Run this once before a presentation to snapshot fresh data.

Usage:
    cd backend
    python export_static.py
"""
import json, os, time
import urllib.request
import urllib.error

BACKEND = "http://localhost:8000"
OUTPUT  = os.path.join(os.path.dirname(__file__), "..", "frontend", "public", "data")
os.makedirs(OUTPUT, exist_ok=True)

def fetch(path, label):
    t0 = time.time()
    print(f"  >> {label}... ", end="", flush=True)
    try:
        url = f"{BACKEND}{path}"
        with urllib.request.urlopen(url, timeout=120) as r:
            data = json.loads(r.read())
        print(f"{len(data) if isinstance(data, list) else 'ok'} rows ({time.time()-t0:.0f}s)")
        return data
    except Exception as e:
        print(f"FAILED — {e}")
        return []

def save(name, data):
    path = os.path.join(OUTPUT, f"{name}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

print("=" * 55)
print("  SIOP Manager — Static Data Export from Railway")
print(f"  Backend: {BACKEND}")
print("=" * 55)
print()

# Check backend is up
try:
    with urllib.request.urlopen(f"{BACKEND}/api/health", timeout=10) as r:
        print(f"OK Backend live: {r.read().decode()}\n")
except Exception as e:
    print(f"FAILED Backend not reachable: {e}")
    print("  Make sure Railway is running and try again.")
    exit(1)

# --- Inventory ---
inv_summary = fetch("/api/inventory/summary", "Inventory summary")
save("inventory_summary", inv_summary)

inv = fetch("/api/inventory?limit=500", "Inventory detail")
save("inventory", inv)

# --- Forecast ---
forecast_month = fetch("/api/demand-forecast/by-month", "Forecast by month")
save("forecast_by_month", forecast_month)

forecast = fetch("/api/demand-forecast?limit=500", "Demand forecast")
save("forecast", forecast)

forecast_bom = fetch("/api/forecast/bom?limit=500", "BOM forecast")
save("forecast_bom", forecast_bom)

# --- Purchase Orders ---
po = fetch("/api/purchase-orders/open?limit=500", "Purchase orders")
save("purchase_orders", po)

# --- Sales Orders ---
so = fetch("/api/sales-orders/pipeline?limit=500", "Sales orders")
save("sales_orders", so)

# --- Supply vs Demand ---
gap = fetch("/api/supply-demand-gap", "Supply vs demand gap")
save("supply_demand_gap", gap)

# --- Slow Moving ---
slow = fetch("/api/slow-moving?limit=500", "Slow-moving items")
save("slow_moving", slow)

# --- Expedite ---
expedite = fetch("/api/expedite", "Expedite report")
save("expedite", expedite)

# --- Data Quality ---
dq = fetch("/api/data-quality", "Data quality")
save("data_quality", dq)

print()
print("=" * 55)
print("  ✓ All files saved to frontend/public/data/")
print("=" * 55)
print()
print("Next: deploy static build to Vercel")
print("  cd ..")
print("  python backend/deploy_demo.py")
print()
