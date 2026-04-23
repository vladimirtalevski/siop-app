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


def rewrite_sql(sql: str) -> str:
    if not USE_MOTHERDUCK:
        return sql
    for pattern, replacement in _REPLACEMENTS:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    # DuckDB compatibility fixes
    sql = re.sub(r'\bCURRENT_DATE\(\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bCURRENT_TIMESTAMP\(\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bGETDATE\(\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bYEAR\(CURRENT_DATE\)', 'YEAR(CURRENT_DATE)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bMONTH\(CURRENT_DATE\)', 'MONTH(CURRENT_DATE)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bDATEADD\(\'year\'', "DATEADD('year'", sql, flags=re.IGNORECASE)
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
