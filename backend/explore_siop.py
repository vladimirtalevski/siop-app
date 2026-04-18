from db import get_connection
import pandas as pd

conn = get_connection()
cur = conn.cursor()

key_tables = [
    "DEMAND_FORECAST",
    "ONHAND_INVENTORY",
    "NET_REQUIREMENTS",
    "PLANNED_ORDER",
    "INVENTORY_TRANSACTIONS",
    "SALES_ORDERS",
    "PURCHASE_ORDER_LINE",
    "ITEMS",
]

for table in key_tables:
    print(f"\n=== {table} ===")
    try:
        cur.execute(f"SELECT * FROM {table} LIMIT 2")
        cols = [d[0] for d in cur.description]
        print("COLUMNS:", cols)
        rows = cur.fetchall()
        for r in rows:
            print(r)
    except Exception as e:
        print(f"ERROR: {e}")

cur.close()
conn.close()
