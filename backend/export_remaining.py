"""Resume export from step 5 (POs and supply/demand gap)."""
import json, os
from decimal import Decimal
from datetime import datetime, date
from db import get_connection

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "frontend", "src", "data")
os.makedirs(OUTPUT, exist_ok=True)

def to_serializable(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    if isinstance(obj, bytearray): return obj.hex()
    return str(obj)

def dump(name, data):
    path = os.path.join(OUTPUT, f"{name}.json")
    with open(path, "w") as f:
        json.dump(data, f, default=to_serializable, indent=2)
    print(f"  Saved {name}.json ({len(data)} rows)")

conn = get_connection()
cur = conn.cursor()

print("[5/6] Purchase orders...")
cur.execute("""
    SELECT
        DATAAREAID AS company, PURCHID AS po_number,
        ITEMID AS item_id, NAME AS item_name,
        VENDACCOUNT AS vendor_id, PURCHQTY AS ordered_qty,
        REMAINDER AS remaining_qty, PURCHPRICE AS unit_price,
        LINEAMOUNT AS line_amount, CURRENCYCODE AS currency,
        DELIVERYDATE AS delivery_date, PURCHSTATUS AS status
    FROM PURCHASE_ORDER_LINE
    WHERE PURCHSTATUS IN (1, 2, 3)
    ORDER BY DELIVERYDATE
    LIMIT 500
""")
cols = [d[0].lower() for d in cur.description]
dump("purchase_orders", [dict(zip(cols, r)) for r in cur.fetchall()])

print("[6/6] Supply vs demand gap...")
cur.execute("""
    SELECT
        inv.DATAAREAID AS company, inv.ITEMID AS item_id,
        inv.INVENTSITEID AS site,
        SUM(inv.AVAILPHYSICAL) AS avail_physical,
        SUM(inv.ONORDER) AS on_order,
        COALESCE(fc.forecast_qty, 0) AS forecast_demand,
        SUM(inv.AVAILPHYSICAL) + SUM(inv.ONORDER) - COALESCE(fc.forecast_qty, 0) AS gap
    FROM ONHAND_INVENTORY inv
    LEFT JOIN (
        SELECT DATAAREAID, ITEMID, SUM(SALESQTY) AS forecast_qty
        FROM DEMAND_FORECAST
        WHERE ACTIVE = 1
          AND TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7')
              BETWEEN CURRENT_DATE AND DATEADD('month', 3, CURRENT_DATE)
        GROUP BY DATAAREAID, ITEMID
    ) fc ON inv.DATAAREAID = fc.DATAAREAID AND inv.ITEMID = fc.ITEMID
    WHERE (inv.AVAILPHYSICAL != 0 OR inv.ONORDER != 0)
    GROUP BY inv.DATAAREAID, inv.ITEMID, inv.INVENTSITEID, fc.forecast_qty
    ORDER BY gap ASC
    LIMIT 300
""")
cols = [d[0].lower() for d in cur.description]
dump("supply_demand_gap", [dict(zip(cols, r)) for r in cur.fetchall()])

print(f"\nDone! All data in {OUTPUT}/")
