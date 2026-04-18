import os
import threading
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

_conn = None
_lock = threading.Lock()


def get_connection():
    global _conn
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
            # Cache the SSO token locally so re-auth isn't needed on next start
            client_store_temporary_credential=True,
        )
        password = os.getenv("SNOWFLAKE_PASSWORD")
        if password:
            params["password"] = password
        else:
            params["authenticator"] = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")

        _conn = snowflake.connector.connect(**params)
        return _conn
