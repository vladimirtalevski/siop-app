import os
import re
import threading
import duckdb
from dotenv import load_dotenv

load_dotenv()

USE_MOTHERDUCK = bool(os.getenv("MOTHERDUCK_TOKEN"))

_conn = None
_lock = threading.Lock()

# SQL prefix rewriter — maps Snowflake fully-qualified names to MotherDuck schemas
# Using fully-qualified siop_db.SCHEMA.TABLE paths to avoid relying on USE siop_db
_REPLACEMENTS = [
    # Order matters — more specific first
    (r'"FLS_PROD_DB"\."MART_DYN_FO"\.', "siop_db.MART_DYN_FO."),
    (r'"FLS_DEV_DB"\."MART_DYN_FO"\.', "siop_db.MART_DYN_FO."),
    (r'FLS_PROD_DB\.MART_DYN_FO\.', "siop_db.MART_DYN_FO."),
    (r'FLS_DEV_DB\.MART_DYN_FO\.', "siop_db.MART_DYN_FO."),
    (r'FLS_SELFSERVICE_PROD_DB\.PROCUREMENT_DATA\.', "siop_db.PUBLIC."),
    (r'FLS_PROD_DB\.EDW_RV\.', "siop_db.PUBLIC."),
    (r'FLS_DEV_DB\.EDW_META\.', "siop_db.PUBLIC."),
    (r'FLS_PROD_DB\.RAW_SHAREPOINT_CI\.', "siop_db.PUBLIC."),
]

# Bare table names used in some queries (without schema prefix)
_BARE_TABLES = [
    "ONHAND_INVENTORY", "DEMAND_FORECAST", "ITEMS", "ORDER_LINES",
    "PURCHASE_ORDERS", "PURCHASE_ORDER_LINE", "INVENTORY_TRANSACTIONS",
    "INVENTORY_DIMENSIONS", "PRICE", "PRODUCT_TRANSLATIONS", "BOM_VERSIONS",
    "BOM_LINES", "NET_REQUIREMENTS", "SALES_ORDERS", "WAREHOUSES",
    "ITEM_COVERAGE", "ENOVIA_ATTRIBUTES", "DIMENSION_CODE_SET",
    "BUYER_GROUPS", "SUPPLY_TYPE_SETUP", "ITEM_COVERAGE_GROUPS",
    "ITEM_PURCHASE_ORDER_SETTINGS", "VENDORS", "SITE", "CUSTOMERS",
    "VENDOR_PRODUCT_RECEIPT_LINES", "CUSTOMER_PACKING_SLIP_LINES",
    "CUSTOMER_INVOICE_LINES", "ADDRESSES", "GLOBAL_ADDRESS_BOOK",
    "DOCUMENT_REFERENCES", "WORKER", "PERSON_NAME", "PRODUCTION_ORDERS",
    "PRODUCTION_BOM", "INVENTORY_TRANSACTIONS_ORIGINATOR",
]


def rewrite_sql(sql: str) -> str:
    if not USE_MOTHERDUCK:
        return sql
    for pattern, replacement in _REPLACEMENTS:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    # Prefix bare table names not already qualified
    for table in _BARE_TABLES:
        # Only match bare table names: preceded by FROM/JOIN/INTO and not already prefixed with a dot
        sql = re.sub(
            rf'(?<!\.)(\b(?:FROM|JOIN|INTO)\s+)({table})\b(?!\s*\.)',
            lambda m: m.group(1) + "siop_db.MART_DYN_FO." + m.group(2),
            sql, flags=re.IGNORECASE
        )
    # DuckDB compatibility fixes
    sql = re.sub(r'\bCURRENT_DATE\(\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCURRENT_TIMESTAMP\(\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGETDATE\(\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    # TRY_TO_DATE(col, format) → TRY_CAST(col AS DATE)
    sql = re.sub(r'\bTRY_TO_DATE\(([^,)]+)(?:,\s*\'[^\']*\')?\)', r'TRY_CAST(\1 AS DATE)', sql, flags=re.IGNORECASE)
    # NVL → COALESCE
    sql = re.sub(r'\bNVL\(', 'COALESCE(', sql, flags=re.IGNORECASE)
    # TO_DECIMAL(x, p, s) → CAST(x AS DOUBLE)
    sql = re.sub(r'\bTO_DECIMAL\(([^,)]+),\s*\d+,\s*\d+\)', r'CAST(\1 AS DOUBLE)', sql, flags=re.IGNORECASE)
    # TO_VARCHAR(x) → CAST(x AS VARCHAR)
    sql = re.sub(r'\bTO_VARCHAR\(([^)]+)\)', r'CAST(\1 AS VARCHAR)', sql, flags=re.IGNORECASE)
    # CONTAINS(col, val) → contains(col, val)  — DuckDB is lowercase
    sql = re.sub(r'\bCONTAINS\(', 'contains(', sql, flags=re.IGNORECASE)
    # DATE type comparisons: cast VARCHAR date columns to DATE (handle optional table prefix)
    sql = re.sub(r'(\w+\.)?(ACTIVATIONDATE|CREATEDDATETIME|DATEPHYSICAL|CONFIRMEDSHIPDATE)\s*(<=|>=|<|>|=)\s*(CURRENT_DATE\b)',
                 lambda m: f'TRY_CAST({m.group(1) or ""}{m.group(2)} AS DATE) {m.group(3)} {m.group(4)}', sql, flags=re.IGNORECASE)
    # DATEDIFF(DAY, ...) → DATEDIFF('day', ...) — DuckDB requires quoted interval
    sql = re.sub(r'\bDATEDIFF\(\s*DAY\s*,', "DATEDIFF('day',", sql, flags=re.IGNORECASE)
    return sql


def get_connection():
    global _conn
    if USE_MOTHERDUCK:
        return _get_motherduck()
    else:
        return _get_snowflake()


def _get_motherduck():
    global _conn
    with _lock:
        if _conn is not None:
            try:
                _conn.execute("SELECT 1")
                return _conn
            except Exception:
                _conn = None

        token = os.getenv("MOTHERDUCK_TOKEN")
        _conn = duckdb.connect(f"md:siop_db?motherduck_token={token}")
        # Print available schemas for diagnostics
        try:
            schemas = _conn.execute("SELECT schema_name FROM information_schema.schemata").fetchdf()
            print(f"✓ MotherDuck connected — schemas: {schemas['schema_name'].tolist()}")
        except Exception as e:
            print(f"✓ MotherDuck connected (schema check failed: {e})")
        return _conn


def _get_snowflake():
    global _conn
    import snowflake.connector
    with _lock:
        try:
            if _conn and not _conn.is_closed():
                return _conn
        except Exception:
            _conn = None

        params = dict(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            client_store_temporary_credential=True,
            network_timeout=120,
            login_timeout=60,
        )
        password = os.getenv("SNOWFLAKE_PASSWORD")
        if password:
            params["password"] = password
        else:
            params["authenticator"] = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")

        _conn = snowflake.connector.connect(**params)
        return _conn
