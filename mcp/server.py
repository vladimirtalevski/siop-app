"""
SIOP Snowflake MCP Server
=========================
Exposes Snowflake Marts to Claude as governed MCP tools.

Tools:
  - list_available_data   : show what marts exist + their purpose
  - describe_mart         : schema + sample data for a specific mart
  - query                 : execute a SQL SELECT (governed, limited)
  - get_entity_map        : show legal entity → company/currency/ERP mapping

Governance:
  - Only SELECT allowed
  - LIMIT enforced
  - Role-based entity and mart access
  - Sensitive column masking
"""

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load env from backend/.env
load_dotenv(Path(__file__).parent.parent / "backend" / ".env")

import snowflake.connector
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, CallToolResult

# ── Config ──────────────────────────────────────────────────────────────────

BASE = Path(__file__).parent
with open(BASE / "marts_metadata.json") as f:
    METADATA = json.load(f)
with open(BASE / "governance.json") as f:
    GOVERNANCE = json.load(f)

SCHEMA = METADATA["schema"]
ROLE = os.getenv("SIOP_MCP_ROLE", "GLOBAL_SUPPLY_CHAIN")

# ── Snowflake connection ─────────────────────────────────────────────────────

_conn = None

def get_conn():
    global _conn
    try:
        if _conn and not _conn.is_closed():
            return _conn
    except Exception:
        _conn = None

    _conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        client_store_temporary_credential=True,
    )
    return _conn


def run_sql(sql: str, limit: int = 1000) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = []
    for row in cur.fetchmany(limit):
        rows.append(dict(zip(cols, row)))
    cur.close()
    return rows


# ── Governance helpers ───────────────────────────────────────────────────────

def role_config() -> dict:
    return GOVERNANCE["roles"].get(ROLE, GOVERNANCE["roles"]["READ_ONLY"])


def allowed_entities() -> list[str]:
    cfg = role_config()
    if cfg["allowed_entities"] == ["*"]:
        return list(METADATA["entity_map"].keys())
    return cfg["allowed_entities"]


def allowed_marts() -> list[str]:
    cfg = role_config()
    if cfg["allowed_marts"] == ["*"]:
        return list(METADATA["marts"].keys())
    return cfg["allowed_marts"]


def max_rows() -> int:
    return min(role_config()["row_limit"], GOVERNANCE["max_query_rows"])


def is_safe_sql(sql: str) -> tuple[bool, str]:
    upper = sql.upper().strip()
    if not upper.startswith("SELECT"):
        return False, "Only SELECT statements are allowed."
    for op in GOVERNANCE["blocked_operations"]:
        pattern = r'\b' + op + r'\b'
        if re.search(pattern, upper):
            return False, f"Operation '{op}' is not permitted."
    return True, ""


def mask_sensitive(rows: list[dict]) -> list[dict]:
    if role_config()["sensitive_columns"] == "visible":
        return rows
    sensitive = [c.lower() for c in GOVERNANCE["sensitive_columns"]]
    masked = []
    for row in rows:
        r = dict(row)
        for col in sensitive:
            if col in r and r[col] is not None:
                try:
                    v = float(r[col])
                    if v < 1000:
                        r[col] = "< 1K"
                    elif v < 10000:
                        r[col] = "1K–10K"
                    elif v < 100000:
                        r[col] = "10K–100K"
                    else:
                        r[col] = "> 100K"
                except (TypeError, ValueError):
                    r[col] = "***"
        masked.append(r)
    return masked


def entity_filter_sql(alias: str = "") -> str:
    entities = allowed_entities()
    col = f"{alias}.DATAAREAID" if alias else "DATAAREAID"
    quoted = ", ".join(f"'{e.upper()}'" for e in entities)
    return f"UPPER({col}) IN ({quoted})"


# ── MCP Server ───────────────────────────────────────────────────────────────

server = Server("siop-snowflake")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="list_available_data",
            description=(
                "List all available Snowflake Marts and their business purpose. "
                "Call this first to understand what data is available before querying."
            ),
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="describe_mart",
            description=(
                "Get the schema, column descriptions, and sample data for a specific mart. "
                "Call this before writing a query to understand column names and data types."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "mart_name": {
                        "type": "string",
                        "description": "Name of the mart (e.g. ONHAND_INVENTORY, PURCHASE_ORDER_LINE)",
                    },
                    "sample_rows": {
                        "type": "integer",
                        "description": "Number of sample rows to return (default 3, max 10)",
                        "default": 3,
                    },
                },
                "required": ["mart_name"],
            },
        ),
        Tool(
            name="query",
            description=(
                "Execute a SQL SELECT query against Snowflake Marts. "
                "Always use fully qualified table names: FLS_PROD_DB.MART_DYN_FO.TABLE_NAME. "
                "Only SELECT is allowed. Results are automatically limited. "
                "Your role is: " + ROLE + ". "
                "Authorized entities: " + ", ".join(allowed_entities()) + ". "
                "Always filter by DATAAREAID using the authorized entities."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL SELECT query to execute",
                    },
                    "description": {
                        "type": "string",
                        "description": "Brief description of what this query answers (for audit log)",
                    },
                },
                "required": ["sql", "description"],
            },
        ),
        Tool(
            name="get_entity_map",
            description="Get the mapping of legal entity codes to company names, currencies, and ERP source.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:

    # ── list_available_data ──────────────────────────────────────────────────
    if name == "list_available_data":
        permitted = allowed_marts()
        lines = [
            f"## Available Snowflake Marts (Role: {ROLE})",
            f"Schema: `{SCHEMA}`",
            f"Authorized entities: {', '.join(allowed_entities())}",
            "",
        ]
        for mart_name, info in METADATA["marts"].items():
            status = "✓" if mart_name in permitted else "✗ (not authorized)"
            lines.append(f"### {mart_name} {status}")
            lines.append(f"{info['description']}")
            if "grain" in info:
                lines.append(f"*Grain: {info['grain']}*")
            if "common_questions" in info:
                lines.append("Common questions: " + " · ".join(info["common_questions"]))
            lines.append("")
        return [TextContent(type="text", text="\n".join(lines))]

    # ── describe_mart ────────────────────────────────────────────────────────
    elif name == "describe_mart":
        mart = arguments["mart_name"].upper()
        n_sample = min(int(arguments.get("sample_rows", 3)), 10)

        if mart not in allowed_marts():
            return [TextContent(type="text", text=f"❌ Access denied: '{mart}' is not in your authorized marts for role {ROLE}.")]

        meta = METADATA["marts"].get(mart, {})
        lines = [
            f"## {mart}",
            meta.get("description", ""),
            "",
            "### Column descriptions",
        ]
        for col, desc in meta.get("key_columns", {}).items():
            lines.append(f"- **{col}**: {desc}")
        lines.append("")

        # Get actual schema from Snowflake
        try:
            schema_rows = run_sql(
                f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                f"FROM FLS_PROD_DB.INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA = 'MART_DYN_FO' AND TABLE_NAME = '{mart}' "
                f"ORDER BY ORDINAL_POSITION"
            )
            lines.append(f"### Schema ({len(schema_rows)} columns)")
            for r in schema_rows[:30]:
                nullable = "" if r.get("is_nullable") == "YES" else " NOT NULL"
                lines.append(f"  `{r['column_name']}` {r['data_type']}{nullable}")
            lines.append("")

            # Sample rows
            entities = allowed_entities()
            quoted = ", ".join(f"'{e.upper()}'" for e in entities)
            sample_sql = (
                f"SELECT * FROM {SCHEMA}.{mart} "
                f"WHERE UPPER(DATAAREAID) IN ({quoted}) "
                f"LIMIT {n_sample}"
            )
            sample = run_sql(sample_sql, n_sample)
            sample = mask_sensitive(sample)
            lines.append(f"### Sample data ({len(sample)} rows)")
            lines.append("```json")
            lines.append(json.dumps(sample, indent=2, default=str))
            lines.append("```")
        except Exception as e:
            lines.append(f"⚠️  Could not fetch schema/sample: {e}")

        return [TextContent(type="text", text="\n".join(lines))]

    # ── query ────────────────────────────────────────────────────────────────
    elif name == "query":
        sql = arguments["sql"].strip()
        desc = arguments.get("description", "")

        # Safety check
        safe, reason = is_safe_sql(sql)
        if not safe:
            return [TextContent(type="text", text=f"❌ Query blocked: {reason}")]

        # Entity access check — warn if query doesn't filter by entity
        entities = allowed_entities()
        entity_check = any(e.upper() in sql.upper() for e in entities)
        if not entity_check and "DATAAREAID" not in sql.upper():
            note = (
                f"⚠️  Warning: query does not filter by DATAAREAID. "
                f"Results may include entities outside your authorized scope ({', '.join(entities)}). "
                f"Adding entity filter automatically.\n\n"
            )
            # Inject entity filter if there's a WHERE clause
            if "WHERE" in sql.upper():
                sql = re.sub(
                    r'\bWHERE\b',
                    f"WHERE UPPER(DATAAREAID) IN ({', '.join(repr(e.upper()) for e in entities)}) AND",
                    sql, count=1, flags=re.IGNORECASE
                )
            else:
                # Add before ORDER BY / LIMIT or at end
                insert_before = re.search(r'\b(ORDER BY|LIMIT|GROUP BY)\b', sql, re.IGNORECASE)
                if insert_before:
                    pos = insert_before.start()
                    sql = sql[:pos] + f"WHERE UPPER(DATAAREAID) IN ({', '.join(repr(e.upper()) for e in entities)}) " + sql[pos:]
                else:
                    sql += f"\nWHERE UPPER(DATAAREAID) IN ({', '.join(repr(e.upper()) for e in entities)})"
        else:
            note = ""

        # Enforce LIMIT
        limit = max_rows()
        if "LIMIT" not in sql.upper():
            sql += f"\nLIMIT {limit}"

        # Log query (audit trail)
        print(f"[AUDIT] Role={ROLE} | Query: {desc} | SQL: {sql[:200]}", file=sys.stderr)

        try:
            rows = run_sql(sql, limit)
            rows = mask_sensitive(rows)
            result = {
                "row_count": len(rows),
                "role": ROLE,
                "authorized_entities": entities,
                "data": rows,
            }
            return [TextContent(
                type="text",
                text=note + f"**{len(rows)} rows returned**\n\n```json\n{json.dumps(result, indent=2, default=str)}\n```"
            )]
        except Exception as e:
            return [TextContent(type="text", text=f"❌ Query error: {e}")]

    # ── get_entity_map ───────────────────────────────────────────────────────
    elif name == "get_entity_map":
        permitted = allowed_entities()
        lines = [f"## Legal Entity Map (Role: {ROLE})", ""]
        for code, info in METADATA["entity_map"].items():
            access = "✓" if code in permitted else "✗"
            lines.append(
                f"{access} **{code.upper()}** — {info['name']} | "
                f"Currency: {info['currency']} | ERP: {info['erp']}"
            )
        lines.append("")
        lines.append("*Add more ERPs (Oracle, Epicor) to `marts_metadata.json` → entity_map*")
        return [TextContent(type="text", text="\n".join(lines))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ── Entry point ──────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
