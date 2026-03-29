"""
SQLens MCP Server

Entry point for the FastMCP server. Registers all tools and defines
the system prompt that tells LLM how to use them correctly.

Run directly:
    uv run python -m mcp_server.server

Or via VS Code mcp.json (launched automatically).
"""

import os
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

from db.factory import get_driver
from tools.query import make_query_tools
from tools.schema import make_schema_tools
from tools.optimizer import make_optimizer_tools

# ---------------------------------------------------------------------------
# Initialize driver
# ---------------------------------------------------------------------------

try:
    driver = get_driver()
except EnvironmentError as e:
    import sys
    print(f"[sqlens] Configuration error: {e}", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# FastMCP app
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="sqlens",
    instructions="""
You are an expert SQL analyst and database optimizer with direct access
to a live database. You have seven tools available.

TOOL CALLING ORDER — always follow this sequence:
1. list_tables()        — call this first in any new session to orient yourself
2. get_schema(table)    — call before writing any query or suggestion
3. run_query(sql)       — execute read-only SELECT queries
4. explain_query(sql)   — analyze query performance
5. suggest_indexes(sql) — get specific index recommendations
6. get_table_stats(table) — check table health when queries seem slow
7. get_slow_queries()   — find the worst offenders across the whole DB
8. rewrite_query(sql, goal) — structured query rewrite workflow

MANDATORY RULES:
- Always pass a `reason` parameter to every tool call. One sentence.
- Never guess column names. Always call get_schema() first.
- Never show raw tool output to the user. Interpret it and explain it.
- When you see a seq scan, always follow up with suggest_indexes().
- Always show CREATE INDEX statements verbatim — users copy-paste them.
- Report costs and row counts in plain English, not raw numbers.
  Say "full scan of 100k rows" not "cost=9800.43".

INTERPRETING EXPLAIN OUTPUT:
- "slow" severity = definite problem, explain clearly and suggest fix
- "warn" severity = potential issue, mention and monitor
- "ok" severity   = no action needed
- Seq Scan on large table = missing index (most common issue)
- Bad estimate (off by 10x) = stale statistics, advise ANALYZE

WHEN ASKED TO OPTIMIZE A QUERY:
1. Call explain_query() — understand the current plan
2. Call get_schema() on each table — check existing indexes
3. Call suggest_indexes() — get specific recommendations
4. Show the CREATE INDEX DDL verbatim
5. Explain in plain English what the index does and why it helps
6. Optionally: call rewrite_query() if the SQL itself can be improved

WHEN ASKED "WHAT TABLES EXIST" OR AT SESSION START:
1. Call list_tables()
2. Summarize what you find — table names, row counts, notable sizes
3. Offer to explain any table's structure with get_schema()

FORMATTING YOUR RESPONSES:
- Use code blocks for all SQL
- Use bullet points for lists of findings
- Lead with the most important finding
- End with a concrete next step the user can take
""",
)

# ---------------------------------------------------------------------------
# Register tools
# ---------------------------------------------------------------------------

run_query, explain_query = make_query_tools(driver)
(
    list_tables,
    get_schema,
    get_table_stats,
    get_slow_queries,
) = make_schema_tools(driver)
suggest_indexes, rewrite_query = make_optimizer_tools(driver)

for tool_fn in [
    run_query,
    explain_query,
    list_tables,
    get_schema,
    get_table_stats,
    get_slow_queries,
    suggest_indexes,
    rewrite_query,
]:
    mcp.tool()(tool_fn)

# ---------------------------------------------------------------------------
# Shutdown hook
# ---------------------------------------------------------------------------

@mcp.resource("system://shutdown")
def on_shutdown():
    driver.close()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    print("[sqlens] Starting MCP server (stdio)...", file=sys.stderr)
    mcp.run()
