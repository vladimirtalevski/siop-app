"""
Snowflake → MotherDuck Sync
============================
Pulls all SIOP marts from Snowflake and loads them into MotherDuck.
Run this once to set up, then weekly to refresh data.

Usage:
    python sync_motherduck.py
"""

import os
import time
import duckdb
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── Tables to sync ────────────────────────────────────────────────────────────
# Format: (snowflake_db, snowflake_schema, table_name, md_schema, row_filter)
# Set to True to skip tables that already exist in MotherDuck
SKIP_EXISTING = True

TABLES = [
    # Core inventory & items
    ("FLS_PROD_DB", "MART_DYN_FO", "ONHAND_INVENTORY",           "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "ITEMS",                       "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "PRODUCT_TRANSLATIONS",        "MART_DYN_FO", "WHERE LANGUAGEID = 'en-US'"),
    ("FLS_PROD_DB", "MART_DYN_FO", "INVENTORY_DIMENSIONS",        "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "PRICE",                       "MART_DYN_FO", "WHERE PRICETYPE = '0'"),
    ("FLS_PROD_DB", "MART_DYN_FO", "ITEM_COVERAGE",               "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "WAREHOUSES",                  "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "BUYER_GROUPS",                "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "SUPPLY_TYPE_SETUP",           "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "ITEM_COVERAGE_GROUPS",        "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "ITEM_PURCHASE_ORDER_SETTINGS","MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "ENOVIA_ATTRIBUTES",           "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "DIMENSION_CODE_SET",          "MART_DYN_FO", ""),

    # Purchase orders
    ("FLS_PROD_DB", "MART_DYN_FO", "PURCHASE_ORDERS",             "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "PURCHASE_ORDER_LINE",         "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "VENDOR_PRODUCT_RECEIPT_LINES","MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "VENDORS",                     "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "GLOBAL_ADDRESS_BOOK",         "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "DOCUMENT_REFERENCES",         "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "WORKER",                      "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "PERSON_NAME",                 "MART_DYN_FO", ""),

    # Sales orders
    ("FLS_PROD_DB", "MART_DYN_FO", "SALES_ORDERS",                "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "ORDER_LINES",                 "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "CUSTOMER_PACKING_SLIP_LINES", "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "CUSTOMER_INVOICE_LINES",      "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "CUSTOMERS",                   "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "ADDRESSES",                   "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "SITE",                        "MART_DYN_FO", ""),

    # Inventory transactions (filtered to last 2 years — can be large)
    ("FLS_PROD_DB", "MART_DYN_FO", "INVENTORY_TRANSACTIONS",      "MART_DYN_FO",
     "WHERE DATEPHYSICAL >= DATEADD('year', -2, CURRENT_DATE())"),

    # Demand forecast
    ("FLS_PROD_DB", "MART_DYN_FO", "DEMAND_FORECAST",             "MART_DYN_FO", "WHERE ACTIVE = 1"),

    # BOM
    ("FLS_PROD_DB", "MART_DYN_FO", "BOM_VERSIONS",                "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "BOM_LINES",                   "MART_DYN_FO", ""),

    # Expedite report tables
    ("FLS_PROD_DB", "MART_DYN_FO", "NET_REQUIREMENTS",            "MART_DYN_FO",
     "WHERE UPPER(DATAAREAID) IN ('US2','ZA4','ZA3','DK1','GH1')"),
    ("FLS_PROD_DB", "MART_DYN_FO", "PRODUCTION_ORDERS",           "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "PRODUCTION_BOM",              "MART_DYN_FO", ""),
    ("FLS_PROD_DB", "MART_DYN_FO", "INVENTORY_TRANSACTIONS_ORIGINATOR", "MART_DYN_FO", ""),

    # FX rates
    ("FLS_SELFSERVICE_PROD_DB", "PROCUREMENT_DATA", "TBL_MASTER_FX_RATES", "PUBLIC", ""),

    # MOQ settings
    ("FLS_PROD_DB", "EDW_RV", "INVENTITEMPURCHSETUP_DYNFO_SAT", "PUBLIC", ""),
]


def connect_snowflake():
    print("Connecting to Snowflake (SSO login may open in browser)...")
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        client_store_temporary_credential=True,
    )
    print("✓ Snowflake connected")
    return conn


def connect_motherduck():
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN not set in .env")
    print("Connecting to MotherDuck...")
    conn = duckdb.connect(f"md:?motherduck_token={token}")
    conn.execute("CREATE DATABASE IF NOT EXISTS siop_db")
    conn.execute("USE siop_db")
    print("✓ MotherDuck connected")
    return conn


def sync_table(sf_conn, md_conn, sf_db, sf_schema, table, md_schema, row_filter):
    # Skip if already loaded successfully
    if SKIP_EXISTING:
        try:
            count = md_conn.execute(f"SELECT COUNT(*) FROM {md_schema}.{table}").fetchone()[0]
            if count > 0:
                print(f"  Skipping {table} — already loaded ({count:,} rows)")
                return count
        except Exception:
            pass

    sql = f"SELECT * FROM {sf_db}.{sf_schema}.{table} {row_filter}"
    print(f"  Fetching {sf_db}.{sf_schema}.{table}...", end=" ", flush=True)
    t0 = time.time()

    try:
        cur = sf_conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()

        from decimal import Decimal
        import datetime

        df = pd.DataFrame(rows, columns=cols)

        # Cast all Decimal → float to avoid DuckDB DECIMAL overflow
        for col in df.columns:
            if len(df) > 0 and isinstance(df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None, Decimal):
                df[col] = df[col].apply(lambda x: float(x) if x is not None else None)

        elapsed = time.time() - t0
        print(f"{len(df):,} rows ({elapsed:.1f}s)", end=" → ", flush=True)

        # Create schema if needed
        md_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {md_schema}")

        # Write to MotherDuck
        md_conn.execute(f"CREATE OR REPLACE TABLE {md_schema}.{table} AS SELECT * FROM df")
        print("✓ loaded")
        return len(df)

    except Exception as e:
        print(f"✗ FAILED: {e!r}")
        return 0


def main():
    print("=" * 60)
    print("SIOP MotherDuck Sync")
    print("=" * 60)

    sf_conn = connect_snowflake()
    md_conn = connect_motherduck()

    total_rows = 0
    failed = []

    for sf_db, sf_schema, table, md_schema, row_filter in TABLES:
        n = sync_table(sf_conn, md_conn, sf_db, sf_schema, table, md_schema, row_filter)
        if n == 0:
            failed.append(table)
        total_rows += n

    sf_conn.close()
    md_conn.close()

    print("\n" + "=" * 60)
    print(f"✓ Sync complete — {total_rows:,} total rows loaded")
    if failed:
        print(f"✗ Failed tables: {', '.join(failed)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
