from __future__ import annotations

import sqlglot
from sqlglot import exp

from db.base import DBDriver
from db.plan_parser import parse_explain, plan_to_dict
from middleware.audit import audit_tool
from middleware.safety import sanitize, UnsafeSQLError


def make_optimizer_tools(driver: DBDriver):
    """
    Factory returning suggest_indexes and rewrite_query
    bound to the given driver instance.

    Called once at server startup in mcp_server/server.py:
        suggest_indexes, rewrite_query = make_optimizer_tools(driver)
    """

    @audit_tool
    def suggest_indexes(sql: str, reason: str = "") -> dict:
        """
        Analyze a SQL query's AST and suggest specific indexes to add.

        This tool does real static analysis — it walks the parsed query
        tree to find columns used in WHERE conditions, JOIN predicates,
        and ORDER BY clauses, then cross-references existing indexes to
        avoid suggesting duplicates.

        Call this after explain_query() identifies a slow query.
        The typical workflow is:
            1. explain_query() → sees Seq Scan on orders
            2. get_schema("orders") → checks existing indexes
            3. suggest_indexes(sql) → returns CREATE INDEX statements

        Parameters
        ----------
        sql:
            The SELECT query to analyze. Must be a valid SELECT —
            write operations are rejected. Does not execute the query.
        reason:
            One sentence explaining why you are suggesting indexes.
            Example: "explain_query showed a seq scan on orders.customer_id
            with 50k rows — suggesting an index to eliminate it."

        Returns
        -------
        dict with keys:
            suggestions:      list of index suggestion objects (see below)
            suggestion_count: number of suggestions generated
            tables_analyzed:  list of table names that were inspected
            skipped_columns:  columns seen in query but skipped because
                              an index already covers them
            message:          plain-English summary of findings

        Each suggestion object contains:
            table:      table name to create index on
            columns:    list of column names in recommended order
                        (order matters — put equality columns first,
                        range columns second, sort columns last)
            ddl:        the exact CREATE INDEX CONCURRENTLY statement
                        to run — copy-paste ready
            reason:     plain-English explanation of why this index
                        helps this specific query
            usage:      which part of the query uses these columns
                        e.g. "WHERE clause", "JOIN predicate", "ORDER BY"
            impact:     "high" / "medium" / "low" estimate based on
                        whether the column is in WHERE (high),
                        JOIN (high), or ORDER BY only (medium)

        How to present suggestions to the user
        ----------------------------------------
        1. Always show the DDL — users want copy-paste ready statements.
        2. Explain the reason in plain English — "This index lets Postgres
           find all orders for a given customer without scanning all 50k rows."
        3. Mention CONCURRENTLY — it means the index builds without locking
           the table, safe to run on production.
        4. If suggestion_count is 0, explain why — either the query already
           has good indexes (check skipped_columns) or the query structure
           does not benefit from indexes (e.g. no WHERE clause at all).
        5. For composite indexes, explain column order — equality filters
           first, range filters second, sort columns last.
        """
        # Validate SQL first
        try:
            clean_sql = sanitize(sql)
        except UnsafeSQLError as e:
            return {
                "error":            str(e),
                "suggestions":      [],
                "suggestion_count": 0,
                "tables_analyzed":  [],
                "skipped_columns":  [],
                "message":          f"SQL rejected by safety layer: {e}",
            }

        # Parse the AST
        try:
            statement = sqlglot.parse_one(clean_sql, error_level=None)
        except Exception as e:
            return {
                "error":            str(e),
                "suggestions":      [],
                "suggestion_count": 0,
                "tables_analyzed":  [],
                "skipped_columns":  [],
                "message":          f"Could not parse SQL: {e}",
            }

        if statement is None:
            return {
                "error":            "Parse returned no statement.",
                "suggestions":      [],
                "suggestion_count": 0,
                "tables_analyzed":  [],
                "skipped_columns":  [],
                "message":          "Could not parse SQL.",
            }

        # Extract column usage from AST
        column_usage = _extract_column_usage(statement)

        if not column_usage:
            return {
                "suggestions":      [],
                "suggestion_count": 0,
                "tables_analyzed":  [],
                "skipped_columns":  [],
                "message": (
                    "No indexable column usage found. "
                    "The query may have no WHERE clause, JOIN predicate, "
                    "or ORDER BY — or columns are not qualified with table names."
                ),
            }

        # Get existing indexes for each table
        tables = list({table for table, _, _ in column_usage})
        existing_indexes: dict[str, list[list[str]]] = {}

        for table in tables:
            try:
                schema_info = driver.get_schema(table)
                existing_indexes[table] = [
                    idx.columns for idx in schema_info.indexes
                ]
            except (ValueError, RuntimeError):
                # Table not found or schema unavailable — skip it
                existing_indexes[table] = []

        # Build suggestions
        suggestions, skipped = _build_suggestions(
            column_usage, existing_indexes
        )

        tables_analyzed = [t for t in tables if t in existing_indexes]

        if suggestions:
            message = (
                f"Found {len(suggestions)} index suggestion(s) for "
                f"{len(tables_analyzed)} table(s). "
                f"All suggestions use CREATE INDEX CONCURRENTLY — "
                f"safe to run without locking your table."
            )
        elif skipped:
            message = (
                "No new indexes needed. "
                "The relevant columns are already covered by existing indexes."
            )
        else:
            message = (
                "No index suggestions generated. "
                "Qualify column names with table aliases "
                "(e.g. o.customer_id instead of customer_id) "
                "so the analyzer can map columns to tables."
            )

        return {
            "suggestions":      suggestions,
            "suggestion_count": len(suggestions),
            "tables_analyzed":  tables_analyzed,
            "skipped_columns":  skipped,
            "message":          message,
        }

    @audit_tool
    def rewrite_query(
        sql: str,
        goal: str,
        reason: str = "",
    ) -> dict:
        """
        Analyze a query and produce an optimized rewrite for a stated goal.

        This tool gathers context — the EXPLAIN plan, the schema for
        each involved table, and any index suggestions — then returns
        all of that context alongside a structured rewrite request.
        LLM produces the actual rewritten SQL based on this context.

        Common goals:
            "make this faster"
            "add keyset pagination instead of OFFSET"
            "reduce the number of rows scanned"
            "make this readable and add comments"
            "avoid the N+1 pattern"
            "push the filter earlier in the query"

        Parameters
        ----------
        sql:
            The SELECT query to rewrite.
        goal:
            Plain-English description of what the rewrite should achieve.
            Be specific — "make this faster" is fine, but
            "eliminate the seq scan on orders" is more actionable.
        reason:
            One sentence explaining why you are rewriting this query.

        Returns
        -------
        dict with keys:
            original_sql:      the input query (normalized)
            goal:              the stated rewrite goal
            explain_plan:      annotated EXPLAIN plan dict (same as
                               explain_query() output) — use this to
                               understand what is currently slow
            schema_context:    dict mapping table name → schema info
                               for every table in the query
            index_suggestions: output of suggest_indexes() — index
                               changes that would help this query
            rewrite_guidance:  structured hints derived from the plan
                               and schema to guide your rewrite
            instructions:      explicit instructions for the LLM on how
                               to produce the rewrite response

        How to use this tool
        ---------------------
        After calling rewrite_query(), you have everything you need:
        1. Read explain_plan.summary to understand the main problem.
        2. Read rewrite_guidance for specific technique suggestions.
        3. Produce the rewritten SQL addressing the goal.
        4. Show both the original and rewritten SQL.
        5. Run explain_query() on the rewrite to show the improvement.
        6. Explain in plain English what changed and why it helps.

        Never just return the rewrite_query() dict to the user raw —
        it is context for you, not output for them.
        """
        # Validate SQL
        try:
            clean_sql = sanitize(sql)
        except UnsafeSQLError as e:
            return {
                "error":        str(e),
                "original_sql": sql,
                "goal":         goal,
            }

        # Get EXPLAIN plan
        try:
            raw_plan = driver.explain(clean_sql, analyze=False)
            parsed   = parse_explain(raw_plan)
            plan_dict = plan_to_dict(parsed)
        except Exception as e:
            plan_dict = {"error": str(e)}
            parsed    = None

        # Extract table names from query
        tables = _extract_tables(clean_sql)

        # Get schema for each table
        schema_context: dict[str, dict] = {}
        for table in tables:
            try:
                info = driver.get_schema(table)
                schema_context[table] = {
                    "columns": [
                        {
                            "name":        c.name,
                            "data_type":   c.data_type,
                            "nullable":    c.nullable,
                            "primary_key": c.primary_key,
                        }
                        for c in info.columns
                    ],
                    "indexes": [
                        {
                            "name":    i.name,
                            "columns": i.columns,
                            "unique":  i.unique,
                        }
                        for i in info.indexes
                    ],
                    "foreign_keys": [
                        {
                            "column":           fk.column,
                            "references_table": fk.references_table,
                        }
                        for fk in info.foreign_keys
                    ],
                }
            except Exception:
                schema_context[table] = {}

        # Get index suggestions
        try:
            index_suggestions = suggest_indexes(
                sql=clean_sql,
                reason="gathering context for rewrite",
            )
        except Exception:
            index_suggestions = {"suggestions": [], "suggestion_count": 0}

        # Derive rewrite guidance from plan analysis
        guidance = _derive_rewrite_guidance(
            goal=goal,
            plan_dict=plan_dict,
            parsed_plan=parsed,
            schema_context=schema_context,
            index_suggestions=index_suggestions,
        )

        return {
            "original_sql":      clean_sql,
            "goal":              goal,
            "explain_plan":      plan_dict,
            "schema_context":    schema_context,
            "index_suggestions": index_suggestions,
            "rewrite_guidance":  guidance,
            "instructions": (
                "Using the context above, produce a rewritten version of "
                "original_sql that achieves the stated goal. "
                "Your response must include: "
                "(1) the rewritten SQL, clearly formatted; "
                "(2) a plain-English explanation of every change made; "
                "(3) the specific reason each change helps; "
                "(4) any caveats or trade-offs the user should know. "
                "After showing the rewrite, call explain_query() on it "
                "to demonstrate the plan improvement."
            ),
        }

    return suggest_indexes, rewrite_query


# ---------------------------------------------------------------------------
# AST analysis helpers
# ---------------------------------------------------------------------------

# Each entry is (table_name, column_name, usage_type)
# usage_type is one of: "where", "join", "order_by"
ColumnUsage = list[tuple[str, str, str]]


def _extract_column_usage(statement: exp.Expression) -> ColumnUsage:
    """
    Walk the parsed AST and extract all (table, column, usage) tuples.

    Only extracts columns that are qualified with a table name or alias
    (e.g. o.customer_id, orders.customer_id). Unqualified columns like
    bare `customer_id` are skipped because we cannot reliably determine
    which table they belong to without a full resolver.

    Returns a deduplicated list of (table, column, usage_type) tuples.
    """
    usage: list[tuple[str, str, str]] = []

    # Build alias → real table name map from FROM and JOIN clauses
    alias_map = _build_alias_map(statement)

    # --- WHERE clause ---
    where = statement.find(exp.Where)
    if where:
        for col in where.find_all(exp.Column):
            table, column = _resolve_column(col, alias_map)
            if table and column:
                usage.append((table, column, "where"))

    # --- JOIN ON conditions ---
    for join in statement.find_all(exp.Join):
        on_clause = join.args.get("on")
        if on_clause:
            for col in on_clause.find_all(exp.Column):
                table, column = _resolve_column(col, alias_map)
                if table and column:
                    usage.append((table, column, "join"))

    # --- ORDER BY ---
    order = statement.find(exp.Order)
    if order:
        for ordered in order.find_all(exp.Ordered):
            for col in ordered.find_all(exp.Column):
                table, column = _resolve_column(col, alias_map)
                if table and column:
                    usage.append((table, column, "order_by"))

    # Deduplicate while preserving first-seen order
    seen: set[tuple[str, str, str]] = set()
    result: ColumnUsage = []
    for entry in usage:
        if entry not in seen:
            seen.add(entry)
            result.append(entry)

    return result


def _build_alias_map(statement: exp.Expression) -> dict[str, str]:
    """
    Build a map from alias → real table name.

    Handles:
        FROM orders o          → {"o": "orders"}
        FROM orders            → {"orders": "orders"}
        JOIN customers c ON .. → {"c": "customers"}
    """
    alias_map: dict[str, str] = {}

    for table_expr in statement.find_all(exp.Table):
        real_name  = table_expr.name
        alias      = table_expr.alias

        if not real_name:
            continue

        # Map the real name to itself (handles unaliased tables)
        alias_map[real_name.lower()] = real_name.lower()

        # Map alias to real name
        if alias:
            alias_map[alias.lower()] = real_name.lower()

    return alias_map


def _resolve_column(
    col: exp.Column,
    alias_map: dict[str, str],
) -> tuple[str | None, str | None]:
    """
    Resolve a Column node to (real_table_name, column_name).
    Returns (None, None) if the column has no table qualifier
    or the qualifier cannot be resolved.
    """
    column_name = col.name
    table_ref   = col.table

    if not column_name or not table_ref:
        return None, None

    real_table = alias_map.get(table_ref.lower())
    if not real_table:
        return None, None

    return real_table, column_name.lower()


def _build_suggestions(
    column_usage: ColumnUsage,
    existing_indexes: dict[str, list[list[str]]],
) -> tuple[list[dict], list[dict]]:
    """
    Convert extracted column usage into index suggestions.

    Strategy:
    - Group columns by table and usage type
    - For each table, build a composite index with:
        equality WHERE/JOIN columns first (highest selectivity)
        ORDER BY columns last (enables index scan for sort)
    - Skip any combination already covered by an existing index
    - Assign impact based on usage type

    Returns (suggestions, skipped) where skipped contains columns
    that already have index coverage.
    """
    from collections import defaultdict

    # Group by table
    by_table: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: {"where": [], "join": [], "order_by": []}
    )

    for table, column, usage in column_usage:
        if column not in by_table[table][usage]:
            by_table[table][usage].append(column)

    suggestions: list[dict] = []
    skipped:     list[dict] = []

    for table, usage_groups in by_table.items():
        where_cols    = usage_groups["where"]
        join_cols     = usage_groups["join"]
        order_by_cols = usage_groups["order_by"]

        # Build composite column list in priority order:
        # equality filters first, then join columns, then sort columns
        composite: list[str] = []
        seen_composite: set[str] = set()

        for col in where_cols + join_cols + order_by_cols:
            if col not in seen_composite:
                composite.append(col)
                seen_composite.add(col)

        if not composite:
            continue

        table_existing = existing_indexes.get(table, [])

        # Check if an existing index already covers the first N columns
        # An index on (a, b, c) covers queries on (a), (a,b), (a,b,c)
        # but NOT on (b) or (c) alone
        already_covered = _is_covered_by_existing(composite, table_existing)

        if already_covered:
            skipped.append({
                "table":   table,
                "columns": composite,
                "reason":  f"Already covered by existing index.",
            })
            continue

        # Determine impact
        if where_cols or join_cols:
            impact = "high"
        elif order_by_cols:
            impact = "medium"
        else:
            impact = "low"

        # Determine usage description
        usage_parts = []
        if where_cols:
            usage_parts.append(f"WHERE ({', '.join(where_cols)})")
        if join_cols:
            usage_parts.append(f"JOIN ({', '.join(join_cols)})")
        if order_by_cols:
            usage_parts.append(f"ORDER BY ({', '.join(order_by_cols)})")
        usage_str = ", ".join(usage_parts)

        # Build DDL
        col_list   = ", ".join(composite)
        index_name = f"idx_{table}_{'_'.join(composite)}"
        # Truncate long index names to stay under 63-char Postgres limit
        if len(index_name) > 63:
            index_name = index_name[:63]

        ddl = (
            f"CREATE INDEX CONCURRENTLY {index_name} "
            f"ON {table} ({col_list});"
        )

        # Build reason
        reason_parts = []
        if where_cols:
            reason_parts.append(
                f"Eliminates full table scan when filtering "
                f"on {', '.join(where_cols)}."
            )
        if join_cols:
            reason_parts.append(
                f"Speeds up JOIN lookup on {', '.join(join_cols)}."
            )
        if order_by_cols and not where_cols and not join_cols:
            reason_parts.append(
                f"Allows index scan for ORDER BY {', '.join(order_by_cols)} "
                f"avoiding an explicit sort step."
            )
        if len(composite) > 1:
            reason_parts.append(
                f"Composite order: equality columns ({', '.join(where_cols or join_cols)}) "
                f"first for maximum selectivity."
            )

        suggestions.append({
            "table":   table,
            "columns": composite,
            "ddl":     ddl,
            "reason":  " ".join(reason_parts),
            "usage":   usage_str,
            "impact":  impact,
        })

    # Sort by impact: high first, then medium, then low
    impact_order = {"high": 0, "medium": 1, "low": 2}
    suggestions.sort(key=lambda s: impact_order.get(s["impact"], 3))

    return suggestions, skipped


def _is_covered_by_existing(
    columns: list[str],
    existing: list[list[str]],
) -> bool:
    """
    Check if an existing index already covers the leading columns
    of the proposed index.

    An index on (a, b, c) covers:
        queries on (a)       ← leading prefix match
        queries on (a, b)    ← leading prefix match
        queries on (a, b, c) ← exact match
    But NOT:
        queries on (b)       ← not a leading column
        queries on (b, c)    ← not a leading prefix

    We check if any existing index has a leading prefix that
    matches all the columns we want to index.
    """
    proposed_set = set(columns)

    for existing_index_cols in existing:
        existing_set = set(existing_index_cols)

        # Exact match or existing covers everything we need
        if proposed_set.issubset(existing_set):
            # Also verify leading column order matches for prefix benefit
            min_len = min(len(columns), len(existing_index_cols))
            if existing_index_cols[:min_len] == columns[:min_len]:
                return True

        # Existing index is exactly what we'd suggest — skip
        if existing_index_cols == columns:
            return True

    return False


def _extract_tables(sql: str) -> list[str]:
    """
    Extract all real table names referenced in a SQL query.
    Returns lowercase table names, deduplicated.
    Used by rewrite_query to know which schemas to fetch.
    """
    try:
        statement = sqlglot.parse_one(sql, error_level=None)
        if statement is None:
            return []
        tables = []
        seen: set[str] = set()
        for table_expr in statement.find_all(exp.Table):
            name = table_expr.name
            if name and name.lower() not in seen:
                tables.append(name.lower())
                seen.add(name.lower())
        return tables
    except Exception:
        return []


def _derive_rewrite_guidance(
    goal: str,
    plan_dict: dict,
    parsed_plan,
    schema_context: dict,
    index_suggestions: dict,
) -> list[str]:
    """
    Derive specific rewrite technique hints from the plan and goal.

    These are structured hints the LLM uses when producing the rewrite.
    Each hint names a concrete technique relevant to this specific query.
    """
    hints: list[str] = []
    goal_lower = goal.lower()

    # --- Plan-based hints ---
    if plan_dict.get("has_seq_scan"):
        hints.append(
            "SEEK_INDEX: query has a full table scan — "
            "rewrite to use indexed columns in WHERE or JOIN."
        )

    if plan_dict.get("has_bad_estimate"):
        hints.append(
            "RUN_ANALYZE: planner estimates are off — "
            "advise running ANALYZE on affected tables."
        )

    slowest = plan_dict.get("slowest_nodes", [])
    for node in slowest:
        if node.get("node_type") == "Sort":
            hints.append(
                "ELIMINATE_SORT: explicit Sort node present — "
                "an index on ORDER BY columns would remove it."
            )
        if node.get("node_type") == "Nested Loop":
            hints.append(
                "CHECK_NESTED_LOOP: Nested Loop join present — "
                "verify JOIN columns are indexed on both sides."
            )
        if node.get("node_type") in ("Hash Join", "Merge Join"):
            hints.append(
                "CHECK_JOIN_INDEXES: expensive join present — "
                "ensure both sides of the JOIN have indexes."
            )

    # --- Goal-based hints ---
    if "pagination" in goal_lower or "offset" in goal_lower:
        hints.append(
            "USE_KEYSET_PAGINATION: replace LIMIT/OFFSET with keyset "
            "pagination using WHERE id > :last_seen_id ORDER BY id "
            "for O(1) page navigation instead of O(n) OFFSET scan."
        )

    if "n+1" in goal_lower or "n+1" in goal_lower:
        hints.append(
            "ELIMINATE_N_PLUS_ONE: replace per-row subquery or loop "
            "with a single JOIN or lateral join."
        )

    if "readable" in goal_lower or "comment" in goal_lower:
        hints.append(
            "ADD_COMMENTS: add inline SQL comments explaining "
            "each CTE, JOIN condition, and WHERE filter."
        )

    if "faster" in goal_lower or "performance" in goal_lower or "slow" in goal_lower:
        if index_suggestions.get("suggestion_count", 0) > 0:
            hints.append(
                "ADD_INDEXES: index suggestions are available — "
                "show the CREATE INDEX statements alongside the rewrite."
            )
        hints.append(
            "PUSH_FILTERS_EARLY: move the most selective WHERE filters "
            "as early as possible in the query — inside CTEs or subqueries "
            "rather than in the outer query."
        )

    if "count" in goal_lower or "aggregate" in goal_lower:
        hints.append(
            "COVER_AGGREGATES: for COUNT/SUM on filtered rows, a covering "
            "index including both filter and aggregate columns allows "
            "index-only scans."
        )

    # --- Schema-based hints ---
    for table, schema in schema_context.items():
        fks = schema.get("foreign_keys", [])
        indexes = schema.get("indexes", [])
        indexed_cols = {col for idx in indexes for col in idx.get("columns", [])}
        for fk in fks:
            if fk.get("column") not in indexed_cols:
                hints.append(
                    f"MISSING_FK_INDEX: {table}.{fk['column']} is a foreign key "
                    f"with no index — JOIN on this column will scan {table}."
                )

    if not hints:
        hints.append(
            "GENERAL_REWRITE: no specific issues detected in plan — "
            "focus on achieving the stated goal: " + goal
        )

    return hints
