from db.base import DBDriver
from db.plan_parser import parse_explain, plan_to_dict
from middleware.safety import sanitize, UnsafeSQLError
from middleware.audit import audit_tool


def make_query_tools(driver: DBDriver):
    """
    Factory function that returns run_query and explain_query
    bound to the given driver instance.

    Called once at server startup in mcp_server/server.py:
        run_query, explain_query = make_query_tools(driver)
        mcp.tool()(run_query)
        mcp.tool()(explain_query)
    """

    @audit_tool
    def run_query(sql: str, limit: int = 100, reason: str = "") -> dict:
        """
        Execute a read-only SQL SELECT query against the database.

        Always call get_schema() first to verify column names before
        writing a query. Never guess column names.

        Parameters
        ----------
        sql:
            A SELECT statement. Write operations are blocked and will
            raise an error. Do not include LIMIT in your query —
            the limit parameter handles that.
        limit:
            Maximum number of rows to return. Default 100.
            Use a lower value when you only need to verify structure.
            Use a higher value (max 1000) when you need full data.
        reason:
            One sentence explaining why you are running this query.
            Required for the audit log. Example: "Checking row count
            on orders table to understand data volume before optimizing."

        Returns
        -------
        dict with keys:
            columns:      list of column name strings
            rows:         list of dicts, each mapping column → value
            row_count:    number of rows returned
            execution_ms: query execution time in milliseconds
            truncated:    True if results were capped at limit

        Example usage
        -------------
        When asked "how many orders does customer 42 have?":
            run_query(
                sql="SELECT COUNT(*) as order_count FROM orders
                     WHERE customer_id = 42",
                reason="Counting orders for customer 42 as requested."
            )
        """
        try:
            clean_sql = sanitize(sql)
        except UnsafeSQLError as e:
            return {
                "error": str(e),
                "blocked": True,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "execution_ms": 0.0,
                "truncated": False,
            }

        try:
            result = driver.execute(clean_sql, limit=limit)
        except RuntimeError as e:
            return {
                "error": str(e),
                "blocked": False,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "execution_ms": 0.0,
                "truncated": False,
            }

        return {
            "columns":      result.columns,
            "rows":         result.rows,
            "row_count":    result.row_count,
            "execution_ms": result.execution_ms,
            "truncated":    result.row_count >= limit,
        }

    @audit_tool
    def explain_query(
        sql: str,
        analyze: bool = False,
        reason: str = "",
    ) -> dict:
        """
        Run EXPLAIN on a query and return an annotated plan tree.

        Use this whenever a query seems slow or the user asks why
        a query is performing a certain way. Always interpret the
        returned plan — do not just forward it to the user raw.

        The plan tree is annotated with severity scores:
            "slow" — definite performance problem, explain it clearly
            "warn" — potential issue, mention it
            "ok"   — no action needed

        Parameters
        ----------
        sql:
            The SELECT query to explain. Same safety rules as run_query.
        analyze:
            If True, run EXPLAIN ANALYZE — this actually executes the
            query and returns real row counts and timing.
            Only use analyze=True when:
            - The query is fast enough to run (< a few seconds)
            - You need actual vs estimated row counts
            - The user specifically asks for real timing
            Default False runs EXPLAIN only (no execution).
        reason:
            One sentence explaining why you are explaining this query.

        Returns
        -------
        dict with keys:
            dialect:          "postgres" or "sqlite"
            summary:          plain English one-sentence summary
            total_cost:       top-level cost estimate (Postgres only)
            has_seq_scan:     True if any sequential scan is present
            has_bad_estimate: True if any planner estimate is off by 10x
            slowest_nodes:    list of top 3 worst nodes with reasons
            plan_tree:        full annotated tree (for deep inspection)

        How to interpret this for the user
        -----------------------------------
        1. Read summary first — it tells you the main issue.
        2. Check slowest_nodes — each has a `reason` field explaining
           the specific problem.
        3. If has_seq_scan is True, the most impactful fix is usually
           adding an index. Call suggest_indexes() next.
        4. If has_bad_estimate is True, advise running ANALYZE on the table.
        5. Report findings in plain English, not raw cost numbers.
           Say "full table scan on orders (50k rows)" not "cost=980.0".
        """
        try:
            clean_sql = sanitize(sql)
        except UnsafeSQLError as e:
            return {
                "error": str(e),
                "blocked": True,
            }

        try:
            raw_plan = driver.explain(clean_sql, analyze=analyze)
        except RuntimeError as e:
            return {
                "error": str(e),
                "blocked": False,
            }

        parsed = parse_explain(raw_plan)
        return plan_to_dict(parsed)

    return run_query, explain_query
