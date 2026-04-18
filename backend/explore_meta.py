from db import get_connection
conn = get_connection()
cur = conn.cursor()

print("=== EDW_META access check ===")
cur.execute("SELECT COUNT(*) FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT LIMIT 1")
print("EDW_META rows:", cur.fetchone())

print("\n=== VENDOR_PRODUCT_RECEIPT_LINES columns ===")
cur.execute("SELECT * FROM FLS_PROD_DB.MART_DYN_FO.VENDOR_PRODUCT_RECEIPT_LINES LIMIT 1")
print([d[0] for d in cur.description])

print("\n=== SITE columns ===")
cur.execute("SELECT DATAAREAID, SITEID, NAME FROM FLS_PROD_DB.MART_DYN_FO.SITE LIMIT 5")
for r in cur.fetchall(): print(r)

cur.close()
conn.close()
