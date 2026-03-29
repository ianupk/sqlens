import sqlite3
import time
import re
from pathlib import Path

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

# Guard against SQL injection in table/index names
_SAFE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

def _validate_name(name: str, kind: str = "table") -> str:
    if not _SAFE_NAME.match(name):
        raise ValueError(f"Invalid {kind} name: '{name}'")
    return name

class SQLiteDriver(DBDriver):
    """
    SQLite driver using stdlib sqlite3.

    Key SQLite limitations vs Postgres that affect this driver:
    - No schemas — list_tables() ignores the schema parameter
    - No connection pool — SQLite is file-based, one connection is fine
    - No pg_stat_statements — get_slow_queries() always returns []
    - No vacuum stats — get_table_stats() returns None for most fields
    - EXPLAIN output is flat rows, not a JSON tree like Postgres
    - No native bool type — booleans stored as 0/1 integers

    These are not bugs — they are documented limitations. The tool
    docstrings tell Claude which DB it is talking to so it interprets
    EXPLAIN output correctly.
    """

    def __init__(self, path: str = ":memory:"):
        """
        path: file path to the SQLite DB, or ":memory:" for in-memory.
        Connection is created once and reused — SQLite is not thread-safe
        with a single connection, but that is fine for our single-client
        stdio MCP server.
        """
        self.path = path
        self._conn = self._connect()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            check_same_thread=False,   # safe for our single-threaded use
        )
        # Return rows as sqlite3.Row objects so we can access by column name
        conn.row_factory = sqlite3.Row
        # Enable foreign key enforcement (off by default in SQLite)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Convert a sqlite3.Row to a plain dict."""
        return dict(zip(row.keys(), row))

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------

    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        """
        Execute a SELECT query and return results capped at limit rows.
        The safety layer already validated this is a SELECT, but we
        enforce limit here as a second line of defence.
        """
        # Inject LIMIT if not already present — simple but effective
        # for the kinds of queries this tool receives
        normalized = sql.strip().rstrip(";")
        if "limit" not in normalized.lower():
            normalized = f"{normalized} LIMIT {limit}"

        start = time.perf_counter()
        try:
            cursor = self._conn.execute(normalized)
            raw_rows = cursor.fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"Query failed: {e}") from e

        elapsed_ms = (time.perf_counter() - start) * 1000

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = [self._row_to_dict(r) for r in raw_rows]

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
        SQLite's EXPLAIN QUERY PLAN returns flat rows describing how
        SQLite will execute the query — not a JSON tree like Postgres.

        analyze parameter is ignored for SQLite — EXPLAIN QUERY PLAN
        always estimates without executing.

        Returns a dict with:
        - "dialect": "sqlite"  — so LLM knows how to interpret this
        - "plan": list of row dicts from EXPLAIN QUERY PLAN
        """
        try:
            cursor = self._conn.execute(f"EXPLAIN QUERY PLAN {sql}")
            raw_rows = cursor.fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"EXPLAIN failed: {e}") from e

        plan_rows = [self._row_to_dict(r) for r in raw_rows]

        return {
            "dialect": "sqlite",
            "plan": plan_rows,
        }

    # ------------------------------------------------------------------
    # list_tables
    # ------------------------------------------------------------------

    def list_tables(self, schema: str = "public") -> list[TableInfo]:
        """
        SQLite has no schemas — the schema parameter is ignored.
        Reads from sqlite_master to find all user tables.
        Row estimates come from sqlite_stat1 if ANALYZE has been run,
        otherwise defaults to 0.
        """
        try:
            # Get all user tables (exclude sqlite internal tables)
            cursor = self._conn.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row["name"] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            raise RuntimeError(f"list_tables failed: {e}") from e

        result = []
        for table_name in tables:
            # Get row estimate from sqlite_stat1 if available
            row_estimate = self._get_row_estimate(table_name)
            result.append(TableInfo(
                schema="main",       # SQLite's default schema name
                name=table_name,
                row_estimate=row_estimate,
                size_bytes=None,     # SQLite doesn't expose per-table size easily
            ))

        return result

    def _get_row_estimate(self, table: str) -> int:
        """
        Try to get row estimate from sqlite_stat1 (populated by ANALYZE).
        Falls back to COUNT(*) if sqlite_stat1 is unavailable.
        Falls back to 0 if COUNT fails.
        """
        try:
            cursor = self._conn.execute(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ? LIMIT 1",
                (table,)
            )
            row = cursor.fetchone()
            if row:
                # stat column is space-separated numbers, first is row count
                return int(row["stat"].split()[0])
        except sqlite3.Error:
            pass

        # Fallback: COUNT(*) — slower but always works
        try:
            _validate_name(table)
            cursor = self._conn.execute(f"SELECT COUNT(*) as n FROM {table}")
            row = cursor.fetchone()
            return row["n"] if row else 0
        except sqlite3.Error:
            return 0

    # ------------------------------------------------------------------
    # get_schema
    # ------------------------------------------------------------------

    def get_schema(self, table: str) -> SchemaInfo:
        """
        Uses PRAGMA table_info, PRAGMA index_list, PRAGMA index_info,
        and PRAGMA foreign_key_list to build full schema info.
        """
        # Verify table exists
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        if not cursor.fetchone():
            raise ValueError(f"Table '{table}' does not exist.")

        columns = self._get_columns(table)
        indexes = self._get_indexes(table)
        foreign_keys = self._get_foreign_keys(table)
        row_count = self._get_row_estimate(table)

        return SchemaInfo(
            table=table,
            columns=columns,
            indexes=indexes,
            foreign_keys=foreign_keys,
            row_count=row_count,
        )

    def _get_columns(self, table: str) -> list[ColumnInfo]:
        _validate_name(table)
        cursor = self._conn.execute(f"PRAGMA table_info({table})")
        rows = cursor.fetchall()
        return [
            ColumnInfo(
                name=row["name"],
                data_type=row["type"] or "TEXT",
                nullable=row["notnull"] == 0,
                default=row["dflt_value"],
                primary_key=row["pk"] > 0,
            )
            for row in rows
        ]

    def _get_indexes(self, table: str) -> list[IndexInfo]:
        _validate_name(table)
        cursor = self._conn.execute(f"PRAGMA index_list({table})")
        index_rows = cursor.fetchall()

        indexes = []
        for idx in index_rows:
            # Get columns in this index
            col_cursor = self._conn.execute(
                f"PRAGMA index_info({_validate_name(idx['name'], 'index')})"
            )
            col_rows = col_cursor.fetchall()
            columns = [c["name"] for c in sorted(col_rows, key=lambda r: r["seqno"])]

            indexes.append(IndexInfo(
                name=idx["name"],
                columns=columns,
                unique=bool(idx["unique"]),
                index_type="btree",   # SQLite only has btree indexes
            ))

        return indexes

    def _get_foreign_keys(self, table: str) -> list[ForeignKeyInfo]:
        _validate_name(table)
        cursor = self._conn.execute(f"PRAGMA foreign_key_list({table})")
        rows = cursor.fetchall()
        return [
            ForeignKeyInfo(
                column=row["from"],
                references_table=row["table"],
                references_column=row["to"],
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # get_table_stats
    # ------------------------------------------------------------------

    def get_table_stats(self, table: str) -> TableStats:
        """
        SQLite exposes very limited stats compared to Postgres.
        Most fields are None — Claude is told this in the tool docstring.
        """
        # Verify table exists
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        if not cursor.fetchone():
            raise ValueError(f"Table '{table}' does not exist.")

        live_count = self._get_row_estimate(table)

        return TableStats(
            table=table,
            live_row_count=live_count,
            dead_row_count=None,        # SQLite has no MVCC dead tuples
            last_vacuum=None,           # SQLite has no explicit vacuum log
            last_analyze=None,          # SQLite has no explicit analyze log
            cache_hit_ratio=None,       # SQLite has no buffer pool stats
            bloat_estimate_bytes=None,  # SQLite has no bloat concept
        )

    # ------------------------------------------------------------------
    # get_slow_queries
    # ------------------------------------------------------------------

    def get_slow_queries(self, min_ms: int = 100) -> list[SlowQuery]:
        """
        SQLite has no built-in slow query log.
        Always returns empty list — this is documented behavior, not an error.
        """
        return []

    # ------------------------------------------------------------------
    # close
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._conn:
            self._conn.close()