from db.base import DBDriver
from middleware.audit import audit_tool


def make_schema_tools(driver: DBDriver):
    """
    Factory returning list_tables, get_schema,
    get_table_stats, get_slow_queries —
    all bound to the given driver instance.

    Called once at server startup in mcp_server/server.py:
        list_tables, get_schema, get_table_stats, get_slow_queries = (
            make_schema_tools(driver)
        )
    """

    @audit_tool
    def list_tables(schema: str = "public", reason: str = "") -> dict:
        """
        List all tables in the database with row estimates and sizes.

        Call this first at the start of any session before writing
        queries or making optimization suggestions. It gives you the
        full map of what exists in the database.

        Parameters
        ----------
        schema:
            The schema to list tables from.
            For Postgres: defaults to "public". Change if the user
            mentions a different schema (e.g. "analytics", "reporting").
            For SQLite: this parameter is ignored — SQLite has no schemas.
            For MySQL: this maps to the database name.
        reason:
            One sentence explaining why you are listing tables.
            Example: "Getting overview of database structure before
            writing optimization suggestions."

        Returns
        -------
        dict with keys:
            tables:       list of table summary objects
            table_count:  total number of tables found
            schema:       the schema that was queried

        Each table summary object contains:
            name:         table name
            schema:       schema name
            row_estimate: approximate row count from DB statistics
                          (may be stale if ANALYZE has not been run recently)
            size_bytes:   total size including indexes, or None if unavailable
            size_human:   human-readable size string e.g. "12.4 MB", or None

        How to use this
        ---------------
        After calling list_tables():
        1. Identify the tables relevant to the user's question.
        2. Call get_schema() on each relevant table before writing any query.
        3. If a table has a very high row_estimate (> 1M), mention it —
           queries on large tables need careful index consideration.
        4. Never guess column names from the table name alone.
           Always verify with get_schema() first.
        """
        try:
            tables = driver.list_tables(schema=schema)
        except RuntimeError as e:
            return {"error": str(e), "tables": [], "table_count": 0, "schema": schema}

        table_list = []
        for t in tables:
            size_human = _format_bytes(t.size_bytes) if t.size_bytes else None
            table_list.append({
                "name":         t.name,
                "schema":       t.schema,
                "row_estimate": t.row_estimate,
                "size_bytes":   t.size_bytes,
                "size_human":   size_human,
            })

        return {
            "tables":      table_list,
            "table_count": len(table_list),
            "schema":      schema,
        }

    @audit_tool
    def get_schema(table: str, reason: str = "") -> dict:
        """
        Get full structural information for a single table.

        Call this before writing any query that touches a table.
        Never assume column names, types, or relationships — always
        verify with this tool first.

        Parameters
        ----------
        table:
            The table name to inspect. Must be an exact match —
            check list_tables() output for correct spelling.
        reason:
            One sentence explaining why you need this schema.
            Example: "Checking orders table column names and indexes
            before writing a join query."

        Returns
        -------
        dict with keys:
            table:        the table name
            columns:      list of column objects (see below)
            indexes:      list of index objects (see below)
            foreign_keys: list of foreign key objects (see below)
            row_count:    live row count if available, else None
            column_count: total number of columns

        Column object fields:
            name:        column name
            data_type:   database type string e.g. "integer", "text", "varchar(255)"
            nullable:    True if NULL values are allowed
            default:     default value expression, or None
            primary_key: True if this column is part of the primary key

        Index object fields:
            name:        index name
            columns:     list of column names in index order
                         (order matters — a composite index on [a, b]
                         helps queries filtering on a or (a AND b),
                         but NOT queries filtering on b alone)
            unique:      True if this is a unique index
            index_type:  storage type e.g. "btree", "hash", "gin", "gist"

        Foreign key object fields:
            column:            the column in this table
            references_table:  the table being referenced
            references_column: the column being referenced

        How to use this for query optimization
        ----------------------------------------
        1. Check indexes before suggesting CREATE INDEX — avoid duplicates.
        2. Check column types before writing WHERE clauses —
           comparing integer columns with string literals causes type coercion
           and can prevent index use.
        3. Check foreign keys to understand JOIN relationships.
        4. If row_count is very high (> 500k), be especially careful about
           queries that might trigger full table scans.
        5. A table with no indexes other than the primary key is a red flag —
           nearly any filter query will do a full scan.
        """
        try:
            info = driver.get_schema(table)
        except ValueError as e:
            return {"error": str(e), "table": table}
        except RuntimeError as e:
            return {"error": str(e), "table": table}

        columns = [
            {
                "name":        c.name,
                "data_type":   c.data_type,
                "nullable":    c.nullable,
                "default":     c.default,
                "primary_key": c.primary_key,
            }
            for c in info.columns
        ]

        indexes = [
            {
                "name":       i.name,
                "columns":    i.columns,
                "unique":     i.unique,
                "index_type": i.index_type,
            }
            for i in info.indexes
        ]

        foreign_keys = [
            {
                "column":            fk.column,
                "references_table":  fk.references_table,
                "references_column": fk.references_column,
            }
            for fk in info.foreign_keys
        ]

        # Derive useful flags LLM can act on directly
        pk_columns = [c["name"] for c in columns if c["primary_key"]]
        indexed_columns = [col for idx in indexes for col in idx["columns"]]
        unindexed_non_pk = [
            c["name"] for c in columns
            if c["name"] not in indexed_columns
            and not c["primary_key"]
        ]

        return {
            "table":            info.table,
            "columns":          columns,
            "indexes":          indexes,
            "foreign_keys":     foreign_keys,
            "row_count":        info.row_count,
            "column_count":     len(columns),
            "pk_columns":       pk_columns,
            "unindexed_columns": unindexed_non_pk,
        }

    @audit_tool
    def get_table_stats(table: str, reason: str = "") -> dict:
        """
        Get performance-relevant health statistics for a table.

        Call this when:
        - A query on this table is unexpectedly slow
        - The user asks about table performance or health
        - You suspect statistics are stale (bad query plans)
        - You want to check if a table needs VACUUM or ANALYZE

        Parameters
        ----------
        table:
            The table name to get stats for.
        reason:
            One sentence explaining why you need these stats.
            Example: "Checking if orders table statistics are stale
            after user reported slow query performance."

        Returns
        -------
        dict with keys:
            table:               table name
            live_row_count:      current live row count, or None
            dead_row_count:      dead/bloated rows from updates/deletes
                                 (Postgres MVCC only), or None
            last_vacuum:         ISO datetime of last VACUUM, or None
            last_analyze:        ISO datetime of last ANALYZE, or None
            cache_hit_ratio:     fraction of reads served from cache (0.0-1.0)
                                 A ratio below 0.95 on a busy system is a concern.
                                 or None if unavailable
            bloat_estimate_bytes: estimated wasted space, or None
            health_flags:        list of plain-English warnings derived
                                 from the stats (see below)

        Health flags are automatically derived:
            "statistics_stale"     — last_analyze is None or > 7 days ago
            "needs_vacuum"         — dead_row_count > 20% of live_row_count
            "low_cache_hit_ratio"  — cache_hit_ratio < 0.95
            "never_analyzed"       — last_analyze is None
            "never_vacuumed"       — last_vacuum is None (Postgres only)

        How to interpret and report this
        ----------------------------------
        1. Always mention health_flags if any are present — they are
           the actionable items, not the raw numbers.
        2. For "statistics_stale": advise running ANALYZE <table>.
        3. For "needs_vacuum": advise running VACUUM <table>.
        4. For "low_cache_hit_ratio": the working set may not fit in
           shared_buffers — suggest increasing it if consistently low.
        5. SQLite returns None for most fields — report what is available
           and note that SQLite does not expose detailed stats.
        """
        try:
            stats = driver.get_table_stats(table)
        except ValueError as e:
            return {"error": str(e), "table": table}
        except RuntimeError as e:
            return {"error": str(e), "table": table}

        health_flags = _derive_health_flags(stats)

        return {
            "table":                table,
            "live_row_count":       stats.live_row_count,
            "dead_row_count":       stats.dead_row_count,
            "last_vacuum":          stats.last_vacuum,
            "last_analyze":         stats.last_analyze,
            "cache_hit_ratio":      stats.cache_hit_ratio,
            "bloat_estimate_bytes": stats.bloat_estimate_bytes,
            "health_flags":         health_flags,
        }

    @audit_tool
    def get_slow_queries(min_ms: int = 100, reason: str = "") -> dict:
        """
        Return the slowest queries recorded by the database.

        For Postgres: reads from pg_stat_statements extension.
        Requires the extension to be installed and loaded:
            CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
        If the extension is not installed, returns an empty list
        with a setup_required flag rather than raising an error.

        For SQLite and MySQL: always returns an empty list —
        these databases do not expose a slow query log through
        a standard programmatic interface.

        Call this when:
        - The user asks "what are the slowest queries?"
        - You want to find optimization opportunities proactively
        - You need to prioritize which queries to investigate first

        Parameters
        ----------
        min_ms:
            Only return queries with mean execution time >= min_ms.
            Default 100ms. Use lower values (e.g. 10) to see more
            queries. Use higher values (e.g. 1000) to focus only
            on severely slow queries.
        reason:
            One sentence explaining why you are checking slow queries.

        Returns
        -------
        dict with keys:
            queries:        list of slow query objects (see below)
            query_count:    number of queries returned
            min_ms:         the threshold used
            setup_required: True if pg_stat_statements is not installed
            message:        plain-English explanation of the results

        Slow query object fields:
            query:              the SQL text (may be truncated by Postgres)
            mean_execution_ms:  average execution time in milliseconds
            total_calls:        how many times this query has been executed
            total_execution_ms: cumulative time spent on this query

        How to use these results
        -------------------------
        1. Sort mentally by mean_execution_ms * total_calls to find
           queries with the highest total impact, not just the slowest
           single execution.
        2. For each slow query, call explain_query() to understand why
           it is slow.
        3. Then call suggest_indexes() to get specific fix recommendations.
        4. Report the top 3-5 findings rather than listing all queries.
        """
        try:
            slow = driver.get_slow_queries(min_ms=min_ms)
        except Exception as e:
            return {
                "error":          str(e),
                "queries":        [],
                "query_count":    0,
                "min_ms":         min_ms,
                "setup_required": False,
                "message":        f"Failed to retrieve slow queries: {e}",
            }

        if not slow:
            # Distinguish between "no slow queries" and "feature unavailable"
            is_sqlite_or_mysql = hasattr(driver, 'path') or hasattr(driver, 'config')
            if is_sqlite_or_mysql:
                message = (
                    "Slow query log is not available for this database type. "
                    "Use explain_query() to analyze specific queries manually."
                )
                setup_required = False
            else:
                message = (
                    "No queries found exceeding the threshold. "
                    "If you expected results, ensure pg_stat_statements is "
                    "installed: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
                )
                setup_required = True
        else:
            total_impact = sum(
                q.mean_execution_ms * q.total_calls for q in slow
            )
            message = (
                f"Found {len(slow)} slow queries above {min_ms}ms threshold. "
                f"Estimated total wasted time: "
                f"{_format_ms(total_impact)}."
            )
            setup_required = False

        queries = [
            {
                "query":              q.query,
                "mean_execution_ms":  round(q.mean_execution_ms, 2),
                "total_calls":        q.total_calls,
                "total_execution_ms": round(q.total_execution_ms, 2),
                "total_impact_ms":    round(
                    q.mean_execution_ms * q.total_calls, 2
                ),
            }
            for q in slow
        ]

        # Sort by total impact descending — highest priority first
        queries.sort(key=lambda q: q["total_impact_ms"], reverse=True)

        return {
            "queries":        queries,
            "query_count":    len(queries),
            "min_ms":         min_ms,
            "setup_required": setup_required,
            "message":        message,
        }

    return list_tables, get_schema, get_table_stats, get_slow_queries


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _format_bytes(n: int | None) -> str | None:
    """Convert bytes to human-readable string."""
    if n is None:
        return None
    if n < 1024:
        return f"{n} B"
    elif n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    elif n < 1024 ** 3:
        return f"{n / 1024 ** 2:.1f} MB"
    else:
        return f"{n / 1024 ** 3:.1f} GB"


def _format_ms(ms: float) -> str:
    """Convert milliseconds to human-readable string."""
    if ms < 1000:
        return f"{ms:.0f}ms"
    elif ms < 60_000:
        return f"{ms / 1000:.1f}s"
    else:
        return f"{ms / 60_000:.1f}min"


def _derive_health_flags(stats) -> list[str]:
    """
    Derive actionable health warnings from raw table stats.
    Returns a list of string flag names — empty list means healthy.
    """
    from datetime import datetime, timezone, timedelta

    flags = []

    # Never analyzed
    if stats.last_analyze is None:
        flags.append("never_analyzed")
    else:
        # Check if analyze is stale (> 7 days)
        try:
            last = datetime.fromisoformat(stats.last_analyze)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - last
            if age > timedelta(days=7):
                flags.append("statistics_stale")
        except (ValueError, TypeError):
            pass

    # Never vacuumed (Postgres only — SQLite will always hit this)
    if stats.last_vacuum is None and stats.dead_row_count is not None:
        flags.append("never_vacuumed")

    # Needs vacuum — dead rows > 20% of live rows
    if (
        stats.dead_row_count is not None
        and stats.live_row_count is not None
        and stats.live_row_count > 0
    ):
        dead_ratio = stats.dead_row_count / stats.live_row_count
        if dead_ratio > 0.20:
            flags.append("needs_vacuum")

    # Low cache hit ratio
    if (
        stats.cache_hit_ratio is not None
        and stats.cache_hit_ratio < 0.95
    ):
        flags.append("low_cache_hit_ratio")

    return flags
