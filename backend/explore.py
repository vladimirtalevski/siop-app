from db import get_connection
import pandas as pd

conn = get_connection()
cur = conn.cursor()

print("=== TABLES IN MART_DYN_FO ===")
cur.execute("SHOW TABLES IN SCHEMA FLS_PROD_DB.MART_DYN_FO")
tables = cur.fetchall()
for t in tables:
    print(t[1])

print("\n=== VIEWS IN MART_DYN_FO ===")
cur.execute("SHOW VIEWS IN SCHEMA FLS_PROD_DB.MART_DYN_FO")
views = cur.fetchall()
for v in views:
    print(v[1])

cur.close()
conn.close()
