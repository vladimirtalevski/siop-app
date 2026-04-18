from db import get_connection

conn = get_connection()
cur = conn.cursor()

print("=== ORDER_LINES columns ===")
cur.execute("SELECT * FROM ORDER_LINES LIMIT 2")
cols = [d[0] for d in cur.description]
print(cols)
for r in cur.fetchall():
    print(dict(zip(cols, r)))

print("\n=== FACT_SALES_ORDER_LINE_LTZ columns ===")
cur.execute("SELECT * FROM FACT_SALES_ORDER_LINE_LTZ LIMIT 2")
cols2 = [d[0] for d in cur.description]
print(cols2)

cur.close()
conn.close()
