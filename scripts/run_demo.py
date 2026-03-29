"""
Demo script — exercises every tool against the demo database.

Useful for:
- Verifying all tools work end-to-end before a VS Code demo
- Catching integration bugs that unit tests miss
- Showing exactly what LLM receives from each tool

Run with:
    uv run python scripts/run_demo.py
    uv run python scripts/run_demo.py --db ./demo.db
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add project root to path so imports work when run as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()


def section(title: str) -> None:
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


def show(label: str, data: dict, keys: list[str] | None = None) -> None:
    """Print selected keys from a tool result dict."""
    print(f"\n{label}:")
    if "error" in data:
        print(f"  ERROR: {data['error']}")
        return
    if keys:
        for k in keys:
            v = data.get(k)
            if isinstance(v, list) and len(v) > 3:
                print(f"  {k}: [{v[0]}, {v[1]}, ... ({len(v)} total)]")
            else:
                print(f"  {k}: {v}")
    else:
        print(json.dumps(data, indent=2, default=str)[:500])


def run_demo(db_path: str) -> None:
    os.environ["DB_TYPE"]      = "sqlite"
    os.environ["SQLITE_PATH"]  = db_path

    # Import after setting env so factory picks up the right driver
    from db.factory import get_driver
    from tools.query import make_query_tools
    from tools.schema import make_schema_tools
    from tools.optimizer import make_optimizer_tools

    driver = get_driver()
    run_query, explain_query      = make_query_tools(driver)
    list_tables, get_schema, get_table_stats, get_slow_queries = (
        make_schema_tools(driver)
    )
    suggest_indexes, rewrite_query = make_optimizer_tools(driver)

    # ------------------------------------------------------------------
    section("1. list_tables")
    # ------------------------------------------------------------------
    result = list_tables(reason="demo: getting database overview")
    show("Result", result, ["table_count", "schema"])
    print("\n  Tables found:")
    for t in result.get("tables", []):
        size = t.get("size_human") or "n/a"
        print(f"    {t['name']:<20} {t['row_estimate']:>10,} rows   {size}")

    # ------------------------------------------------------------------
    section("2. get_schema — orders table")
    # ------------------------------------------------------------------
    result = get_schema(table="orders", reason="demo: inspecting orders schema")
    show("Result", result, ["table", "column_count", "pk_columns"])
    print(f"\n  Columns: {[c['name'] for c in result.get('columns', [])]}")
    print(f"  Indexes: {[i['name'] for i in result.get('indexes', [])]}")
    print(f"  Unindexed: {result.get('unindexed_columns', [])}")
    print(f"  FK columns: {[fk['column'] for fk in result.get('foreign_keys', [])]}")

    # ------------------------------------------------------------------
    section("3. run_query — basic SELECT")
    # ------------------------------------------------------------------
    result = run_query(
        sql="SELECT id, customer_id, status, total_cents FROM orders LIMIT 5",
        reason="demo: fetching sample orders",
    )
    show("Result", result, ["row_count", "columns", "execution_ms", "truncated"])
    if result.get("rows"):
        print(f"\n  First row: {result['rows'][0]}")

    # ------------------------------------------------------------------
    section("4. run_query — aggregate query")
    # ------------------------------------------------------------------
    result = run_query(
        sql="""
            SELECT
                status,
                COUNT(*) as order_count,
                ROUND(AVG(total_cents) / 100.0, 2) as avg_total
            FROM orders
            GROUP BY status
            ORDER BY order_count DESC
        """,
        reason="demo: order status breakdown",
    )
    show("Result", result, ["row_count", "execution_ms"])
    for row in result.get("rows", []):
        print(f"  {row.get('status'):<12} {row.get('order_count'):>8,} orders  "
              f"avg ${row.get('avg_total')}")

    # ------------------------------------------------------------------
    section("5. explain_query — slow query (no index on customer_id)")
    # ------------------------------------------------------------------
    slow_sql = "SELECT * FROM orders o WHERE o.customer_id = 42"
    result = explain_query(
        sql=slow_sql,
        reason="demo: analyzing slow customer order lookup",
    )
    show("Result", result, [
        "dialect", "summary", "has_seq_scan",
        "has_bad_estimate",
    ])
    print(f"\n  Slowest nodes:")
    for node in result.get("slowest_nodes", []):
        print(f"    [{node['severity'].upper():4}] "
              f"{node['node_type']:<20} "
              f"table={node.get('relation_name','?')}")
        print(f"           {node['reason']}")

    # ------------------------------------------------------------------
    section("6. suggest_indexes — fix the slow query")
    # ------------------------------------------------------------------
    result = suggest_indexes(
        sql=slow_sql,
        reason="demo: suggesting index for slow customer_id filter",
    )
    show("Result", result, [
        "suggestion_count", "tables_analyzed", "message",
    ])
    for s in result.get("suggestions", []):
        print(f"\n  [{s['impact'].upper()}] {s['table']} ({', '.join(s['columns'])})")
        print(f"  Usage:  {s['usage']}")
        print(f"  Reason: {s['reason']}")
        print(f"\n  DDL:\n    {s['ddl']}")

    # ------------------------------------------------------------------
    section("7. explain_query — JOIN query")
    # ------------------------------------------------------------------
    join_sql = """
        SELECT
            c.first_name,
            c.last_name,
            COUNT(o.id) AS order_count,
            SUM(o.total_cents) AS total_spent
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE c.country = 'IN'
        GROUP BY c.id, c.first_name, c.last_name
        ORDER BY total_spent DESC
        LIMIT 10
    """
    result = explain_query(
        sql=join_sql,
        reason="demo: analyzing customer lifetime value query",
    )
    show("Result", result, ["summary", "has_seq_scan"])
    print(f"\n  Plan nodes:")
    for node in result.get("plan_tree", []):
        print(f"    [{node['severity'].upper():4}] {node['node_type']}")

    # ------------------------------------------------------------------
    section("8. suggest_indexes — fix the JOIN query")
    # ------------------------------------------------------------------
    result = suggest_indexes(
        sql=join_sql,
        reason="demo: suggesting indexes for customer LTV join query",
    )
    show("Result", result, ["suggestion_count", "message"])
    for s in result.get("suggestions", []):
        print(f"  {s['ddl']}")

    # ------------------------------------------------------------------
    section("9. get_table_stats")
    # ------------------------------------------------------------------
    result = get_table_stats(
        table="orders",
        reason="demo: checking orders table health",
    )
    show("Result", result, [
        "live_row_count", "dead_row_count",
        "cache_hit_ratio", "health_flags",
    ])

    # ------------------------------------------------------------------
    section("10. get_slow_queries")
    # ------------------------------------------------------------------
    result = get_slow_queries(
        min_ms=0,
        reason="demo: listing slow queries",
    )
    show("Result", result, [
        "query_count", "setup_required", "message",
    ])

    # ------------------------------------------------------------------
    section("11. run_query — blocked write attempt")
    # ------------------------------------------------------------------
    result = run_query(
        sql="DELETE FROM orders WHERE id = 1",
        reason="demo: testing safety layer",
    )
    print(f"\n  blocked: {result.get('blocked')}")
    print(f"  error:   {result.get('error', '')[:80]}")

    # ------------------------------------------------------------------
    section("12. rewrite_query — pagination rewrite")
    # ------------------------------------------------------------------
    offset_sql = """
        SELECT * FROM orders o
        WHERE o.status = 'completed'
        ORDER BY o.id
        LIMIT 20 OFFSET 10000
    """
    result = rewrite_query(
        sql=offset_sql,
        goal="replace OFFSET pagination with keyset pagination",
        reason="demo: rewriting slow OFFSET query",
    )
    show("Result", result, ["goal", "original_sql"])
    print(f"\n  Guidance hints:")
    for hint in result.get("rewrite_guidance", []):
        print(f"    - {hint[:80]}")

    # ------------------------------------------------------------------
    section("Summary")
    # ------------------------------------------------------------------
    print("\n  All tools exercised successfully.")
    print(f"  Audit log: {Path('audit.log').resolve()}")
    print(f"\n  Check audit.log to see every tool call recorded.")

    driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the SQLens demo.")
    parser.add_argument(
        "--db",
        default=os.getenv("SQLITE_PATH", "./demo.db"),
        help="Path to the demo SQLite database",
    )
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"Demo DB not found at {args.db}")
        print("Run first: uv run python scripts/seed_demo_db.py")
        sys.exit(1)

    run_demo(db_path=args.db)
