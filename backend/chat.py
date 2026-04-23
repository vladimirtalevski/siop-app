"""
AI Supply Chain Assistant
Wraps Anthropic tool-use so Claude can query Snowflake and answer
natural-language questions about the SIOP data.
"""

import json
import os
import re
from pathlib import Path

import anthropic

BASE = Path(__file__).parent.parent / "mcp"

with open(BASE / "marts_metadata.json") as f:
    METADATA = json.load(f)
with open(BASE / "governance.json") as f:
    GOVERNANCE = json.load(f)

SCHEMA = METADATA["schema"]
BLOCKED = GOVERNANCE["blocked_operations"]

SYSTEM_PROMPT = """You are a supply chain data analyst assistant for FLSmidth.
You have access to Snowflake data marts via the `execute_sql` tool.
Always use fully qualified table names: {schema}.TABLE_NAME.

## Available Marts
{marts}

## Legal Entities (DATAAREAID)
{entities}

## Rules
- Only write SELECT queries. Never INSERT, UPDATE, DELETE, DROP, etc.
- Always filter by DATAAREAID using the authorized entities for the user's company.
- Keep queries efficient — use LIMIT (max 500 rows unless user asks for more).
- When showing numbers: format currencies with 2 decimals, quantities with commas.
- For dates use YYYY-MM-DD format.
- If a question is ambiguous, make a reasonable assumption and state it.
- Respond concisely. Lead with the key insight, then show the data.
- Use markdown for formatting (bold key numbers, bullet points for lists).
- If you run a query and get no rows, say so clearly — don't invent data.
""".format(
    schema=SCHEMA,
    marts="\n".join(
        f"- **{name}**: {info['description']} | Grain: {info.get('grain', 'N/A')}"
        for name, info in METADATA["marts"].items()
    ),
    entities="\n".join(
        f"- {code.upper()}: {info['name']} ({info['currency']})"
        for code, info in METADATA["entity_map"].items()
    ),
)

TOOLS = [
    {
        "name": "execute_sql",
        "description": (
            "Execute a SELECT query against Snowflake and return results as JSON rows. "
            "Use fully qualified table names: FLS_PROD_DB.MART_DYN_FO.TABLE_NAME. "
            "Results are capped at 500 rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL SELECT query to execute",
                },
                "purpose": {
                    "type": "string",
                    "description": "One-sentence description of what this query answers",
                },
            },
            "required": ["sql", "purpose"],
        },
    },
    {
        "name": "list_marts",
        "description": "List all available data marts with their descriptions and key columns.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
]


def _is_safe(sql: str) -> tuple[bool, str]:
    upper = sql.upper().strip()
    if not upper.startswith("SELECT"):
        return False, "Only SELECT statements are allowed."
    for op in BLOCKED:
        if re.search(r"\b" + op + r"\b", upper):
            return False, f"Operation '{op}' is not permitted."
    return True, ""


def _run_sql(sql: str, run_query_fn) -> list[dict]:
    safe, reason = _is_safe(sql)
    if not safe:
        raise ValueError(reason)
    if "LIMIT" not in sql.upper():
        sql = sql.rstrip(";") + "\nLIMIT 500"
    return run_query_fn(sql)


def _handle_tool(name: str, inputs: dict, run_query_fn) -> str:
    if name == "list_marts":
        lines = []
        for mart, info in METADATA["marts"].items():
            lines.append(f"### {mart}")
            lines.append(info["description"])
            cols = info.get("key_columns", {})
            for col, desc in list(cols.items())[:6]:
                lines.append(f"  - {col}: {desc}")
        return "\n".join(lines)

    if name == "execute_sql":
        sql = inputs["sql"]
        purpose = inputs.get("purpose", "")
        print(f"[CHAT QUERY] {purpose} | SQL: {sql[:120]}")
        try:
            rows = _run_sql(sql, run_query_fn)
            return json.dumps({"row_count": len(rows), "rows": rows}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return json.dumps({"error": f"Unknown tool: {name}"})


def run_chat(messages: list[dict], run_query_fn, company: str = "US2") -> dict:
    """
    Run a multi-turn conversation with Claude using tool use.
    Returns {"answer": str, "queries": list[str], "data": list[dict] | None}
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    system = SYSTEM_PROMPT + f"\n\nThe user's primary company is **{company.upper()}**. Default DATAAREAID filter to this company unless they ask about others."

    # Convert frontend message format to Anthropic format
    api_messages = []
    for m in messages:
        api_messages.append({"role": m["role"], "content": m["content"]})

    queries_run = []
    last_data = None

    # Agentic loop — Claude may call tools multiple times
    for _ in range(6):  # max 6 tool rounds
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=system,
            tools=TOOLS,
            messages=api_messages,
        )

        if response.stop_reason == "end_turn":
            # Extract final text
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text += block.text
            return {"answer": text, "queries": queries_run, "data": last_data}

        if response.stop_reason == "tool_use":
            # Add assistant turn (may contain text + tool_use blocks)
            api_messages.append({"role": "assistant", "content": response.content})

            # Execute all tool calls and collect results
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = _handle_tool(block.name, block.input, run_query_fn)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
                    if block.name == "execute_sql":
                        queries_run.append(block.input.get("sql", ""))
                        try:
                            parsed = json.loads(result)
                            if "rows" in parsed and parsed["rows"]:
                                last_data = parsed["rows"]
                        except Exception:
                            pass

            api_messages.append({"role": "user", "content": tool_results})
            continue

        # Unexpected stop reason
        break

    return {"answer": "I was unable to complete the analysis. Please try rephrasing your question.", "queries": queries_run, "data": last_data}
