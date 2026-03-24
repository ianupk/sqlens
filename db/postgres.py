import time
from db.base import (
    DBDriver,
    QueryResult,
    TableInfo,
    SchemaInfo,
    TableStats,
    SlowQuery,
    ColumnInfo,
    IndexInfo,
    ForeignKeyInfo,
)


class PostgresDriver(DBDriver):
    """
    PostgreSQL driver using psycopg3 (import name: psycopg).

    Requires a Postgres role with at minimum:
        GRANT CONNECT ON DATABASE devdb TO readonly_user;
        GRANT USAGE ON SCHEMA public TO readonly_user;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;

    For get_slow_queries() to work, also requires:
        CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    And in postgresql.conf:
        shared_preload_libraries = 'pg_stat_statements'
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool = None

    def _get_pool(self):
        if self._pool is None:
            from psycopg_pool import ConnectionPool
            self._pool = ConnectionPool(
                conninfo=self.dsn,
                min_size=1,
                max_size=5,
                open=False,
            )
            self._pool.open(wait=True, timeout=5.0)
        return self._pool

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        normalized = sql.strip().rstrip(";")
        if "limit" not in normalized.lower():
            normalized = f"{normalized} LIMIT {limit}"

        pool = self._get_pool()
        start = time.perf_counter()

        try:
            with pool.connection() as conn:
                # row_factory returns dicts instead of tuples
                from psycopg.rows import dict_row
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(normalized)
                    rows = cur.fetchall()
                    columns = [desc.name for desc in cur.description] \
                              if cur.description else []
        except Exception as e:
            raise RuntimeError(f"Query failed: {e}") from e

        elapsed_ms = (time.perf_counter() - start) * 1000

        return QueryResult(
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_ms=round(elapsed_ms, 3),
        )

    # ------------------------------------------------------------------
    # explain
    # ------------------------------------------------------------------

    def explain(self, sql: str, analyze: bool = False) -> dict:
        """
        Returns the Postgres EXPLAIN plan as a parsed Python dict.
        Postgres FORMAT JSON returns a single-element list:
            [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 123.4, ...}}]
        """
        keyword = "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS)" \
                  if analyze else "EXPLAIN (FORMAT JSON)"
        explain_sql = f"{keyword} {sql}"

        pool = self._get_pool()
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(explain_sql)
                    # Postgres returns the JSON as the first column
                    # of the first row — psycopg3 parses it automatically
                    result = cur.fetchone()
                    plan = result[0] if result else {}
        except Exception as e:
            raise RuntimeError(f"EXPLAIN failed: {e}") from e

        return {
            "dialect": "postgres",
            "plan": plan,
        }

    # ------------------------------------------------------------------
    # list_tables
    # ------------------------------------------------------------------

    def list_tables(self, schema: str = "public") -> list[TableInfo]:
        """
        Queries pg_catalog for user tables in the given schema.
        Row estimates come from pg_class.reltuples — updated by ANALYZE,
        so may be approximate for recently modified tables.
        """
        sql = """
            SELECT
                schemaname         AS schema,
                tablename          AS name,
                pg_class.reltuples AS row_estimate,
                pg_total_relation_size(
                    quote_ident(schemaname) || '.' || quote_ident(tablename)
                )                  AS size_bytes
            FROM pg_tables
            JOIN pg_class
              ON pg_class.relname = pg_tables.tablename
            JOIN pg_namespace
              ON pg_namespace.oid = pg_class.relnamespace
             AND pg_namespace.nspname = pg_tables.schemaname
            WHERE schemaname = %s
            ORDER BY tablename
        """
        pool = self._get_pool()
        try:
            with pool.connection() as conn:
                from psycopg.rows import dict_row
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql, (schema,))
                    rows = cur.fetchall()
        except Exception as e:
            raise RuntimeError(f"list_tables failed: {e}") from e

        return [
            TableInfo(
                schema=row["schema"],
                name=row["name"],
                row_estimate=int(row["row_estimate"] or 0),
                size_bytes=int(row["size_bytes"] or 0),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # get_schema
    # ------------------------------------------------------------------

    def get_schema(self, table: str) -> SchemaInfo:
        pool = self._get_pool()
        try:
            with pool.connection() as conn:
                from psycopg.rows import dict_row
                with conn.cursor(row_factory=dict_row) as cur:
                    # Verify table exists
                    cur.execute("""
                        SELECT 1 FROM pg_tables
                        WHERE schemaname = 'public' AND tablename = %s
                    """, (table,))
                    if not cur.fetchone():
                        raise ValueError(f"Table '{table}' does not exist.")

                    columns = self._get_columns(cur, table)
                    indexes = self._get_indexes(cur, table)
                    foreign_keys = self._get_foreign_keys(cur, table)
                    row_count = self._get_live_row_count(cur, table)

        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(f"get_schema failed: {e}") from e

        return SchemaInfo(
            table=table,
            columns=columns,
            indexes=indexes,
            foreign_keys=foreign_keys,
            row_count=row_count,
        )

    def _get_columns(self, cur, table: str) -> list[ColumnInfo]:
        cur.execute("""
            SELECT
                a.attname                                    AS name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                NOT a.attnotnull                             AS nullable,
                pg_get_expr(d.adbin, d.adrelid)              AS default,
                EXISTS (
                    SELECT 1 FROM pg_constraint c
                    WHERE c.conrelid = a.attrelid
                      AND c.contype = 'p'
                      AND a.attnum = ANY(c.conkey)
                ) AS primary_key
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d
              ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attrelid = %s::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """, (table,))

        return [
            ColumnInfo(
                name=row["name"],
                data_type=row["data_type"],
                nullable=row["nullable"],
                default=row["default"],
                primary_key=row["primary_key"],
            )
            for row in cur.fetchall()
        ]

    def _get_indexes(self, cur, table: str) -> list[IndexInfo]:
        cur.execute("""
            SELECT
                i.relname                           AS name,
                ix.indisunique                      AS unique,
                am.amname                           AS index_type,
                array_agg(
                    a.attname ORDER BY k.ordinality
                )                                   AS columns
            FROM pg_index ix
            JOIN pg_class i  ON i.oid  = ix.indexrelid
            JOIN pg_class t  ON t.oid  = ix.indrelid
            JOIN pg_am am    ON am.oid = i.relam
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality)
              ON true
            JOIN pg_attribute a
              ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE t.relname = %s
              AND t.relnamespace = 'public'::regnamespace
            GROUP BY i.relname, ix.indisunique, am.amname
            ORDER BY i.relname
        """, (table,))

        return [
            IndexInfo(
                name=row["name"],
                columns=row["columns"],
                unique=row["unique"],
                index_type=row["index_type"],
            )
            for row in cur.fetchall()
        ]

    def _get_foreign_keys(self, cur, table: str) -> list[ForeignKeyInfo]:
        cur.execute("""
            SELECT
                kcu.column_name         AS column,
                ccu.table_name          AS references_table,
                ccu.column_name         AS references_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON kcu.constraint_name = tc.constraint_name
             AND kcu.table_schema    = tc.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name  = tc.constraint_name
             AND ccu.table_schema     = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name      = %s
              AND tc.table_schema    = 'public'
        """, (table,))

        return [
            ForeignKeyInfo(
                column=row["column"],
                references_table=row["references_table"],
                references_column=row["references_column"],
            )
            for row in cur.fetchall()
        ]

    def _get_live_row_count(self, cur, table: str) -> int | None:
        """
        Gets live row count from pg_stat_user_tables.
        Returns None if stats haven't been collected yet
        (table was never ANALYZEd).
        """
        cur.execute("""
            SELECT n_live_tup
            FROM pg_stat_user_tables
            WHERE relname = %s
        """, (table,))
        row = cur.fetchone()
        return int(row["n_live_tup"]) if row else None

    # ------------------------------------------------------------------
    # get_table_stats
    # ------------------------------------------------------------------

    def get_table_stats(self, table: str) -> TableStats:
        sql = """
            SELECT
                n_live_tup                              AS live_row_count,
                n_dead_tup                              AS dead_row_count,
                last_vacuum::text                       AS last_vacuum,
                last_analyze::text                      AS last_analyze,
                CASE
                    WHEN heap_blks_hit + heap_blks_read = 0 THEN NULL
                    ELSE ROUND(
                        heap_blks_hit::numeric /
                        (heap_blks_hit + heap_blks_read), 4
                    )
                END                                     AS cache_hit_ratio
            FROM pg_stat_user_tables
            WHERE relname = %s
        """
        pool = self._get_pool()
        try:
            with pool.connection() as conn:
                from psycopg.rows import dict_row
                with conn.cursor(row_factory=dict_row) as cur:
                    # Verify table exists first
                    cur.execute(
                        "SELECT 1 FROM pg_tables WHERE tablename = %s",
                        (table,)
                    )
                    if not cur.fetchone():
                        raise ValueError(f"Table '{table}' does not exist.")

                    cur.execute(sql, (table,))
                    row = cur.fetchone()
        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(f"get_table_stats failed: {e}") from e

        if not row:
            # Table exists but has never been ANALYZEd
            return TableStats(
                table=table,
                live_row_count=None,
                dead_row_count=None,
                last_vacuum=None,
                last_analyze=None,
                cache_hit_ratio=None,
                bloat_estimate_bytes=None,
            )

        return TableStats(
            table=table,
            live_row_count=int(row["live_row_count"] or 0),
            dead_row_count=int(row["dead_row_count"] or 0),
            last_vacuum=row["last_vacuum"],
            last_analyze=row["last_analyze"],
            cache_hit_ratio=float(row["cache_hit_ratio"])
                            if row["cache_hit_ratio"] else None,
            bloat_estimate_bytes=None,  # requires pgstattuple extension
        )

    # ------------------------------------------------------------------
    # get_slow_queries
    # ------------------------------------------------------------------

    def get_slow_queries(self, min_ms: int = 100) -> list[SlowQuery]:
        """
        Reads from pg_stat_statements.
        Returns [] gracefully if the extension is not installed
        rather than raising — Claude will report it's unavailable.
        """
        sql = """
            SELECT
                query,
                mean_exec_time          AS mean_ms,
                calls                   AS total_calls,
                total_exec_time         AS total_ms
            FROM pg_stat_statements
            WHERE mean_exec_time >= %s
            ORDER BY mean_exec_time DESC
            LIMIT 25
        """
        pool = self._get_pool()
        try:
            with pool.connection() as conn:
                from psycopg.rows import dict_row
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql, (min_ms,))
                    rows = cur.fetchall()
        except Exception:
            # pg_stat_statements not installed — return empty gracefully
            return []

        return [
            SlowQuery(
                query=row["query"],
                mean_execution_ms=round(float(row["mean_ms"]), 2),
                total_calls=int(row["total_calls"]),
                total_execution_ms=round(float(row["total_ms"]), 2),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # close
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None