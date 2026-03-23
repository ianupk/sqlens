from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Data classes — these are what every tool function receives and returns.
# They are plain Python objects with no DB-specific details in them.
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """Metadata for a single column in a table."""
    name: str
    data_type: str
    nullable: bool
    default: str | None
    primary_key: bool


@dataclass
class IndexInfo:
    """Metadata for a single index on a table."""
    name: str
    columns: list[str]
    unique: bool
    index_type: str          # e.g. "btree", "hash", "gin"


@dataclass
class ForeignKeyInfo:
    """A foreign key relationship from this table to another."""
    column: str
    references_table: str
    references_column: str


@dataclass
class QueryResult:
    """
    The result of a SELECT query.
    rows is a list of dicts — each dict maps column name → value.
    """
    rows: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    execution_ms: float


@dataclass
class TableInfo:
    """Summary info for a single table, used by list_tables()."""
    schema: str
    name: str
    row_estimate: int        # approximate, from DB statistics
    size_bytes: int | None   # None if DB doesn't expose this easily


@dataclass
class SchemaInfo:
    """Full structural info for a single table, used by get_schema()."""
    table: str
    columns: list[ColumnInfo]
    indexes: list[IndexInfo]
    foreign_keys: list[ForeignKeyInfo]
    row_count: int | None    # live count if available, None if too expensive


@dataclass
class TableStats:
    """
    Performance-relevant statistics for a table.
    Used by get_table_stats() to help Claude diagnose slow queries.
    Fields that don't apply to a given DB driver should be set to None.
    """
    table: str
    live_row_count: int | None
    dead_row_count: int | None       # postgres: dead tuples from MVCC
    last_vacuum: str | None          # ISO datetime string or None
    last_analyze: str | None         # ISO datetime string or None
    cache_hit_ratio: float | None    # 0.0 to 1.0, None if unavailable
    bloat_estimate_bytes: int | None


@dataclass
class SlowQuery:
    """A single entry from the slow query log."""
    query: str
    mean_execution_ms: float
    total_calls: int
    total_execution_ms: float


# ---------------------------------------------------------------------------
# The abstract driver — every DB implementation must satisfy this interface.
# ---------------------------------------------------------------------------

class DBDriver(ABC):
    """
    Abstract base class for all database drivers.

    Design rules:
    - All methods are synchronous. We are not using async here because
      FastMCP with stdio does not require it, and async adds complexity
      with no benefit in a single-client local tool.
    - All methods return the dataclasses defined above, never raw DB types.
    - All methods raise plain Python exceptions (not DB-specific ones).
      The caller should never need to import psycopg or sqlite3 to handle errors.
    - The driver is responsible for connection management internally.
      Callers never see connection objects.
    """

    @abstractmethod
    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        """
        Execute a read-only SQL query and return results.

        The SQL passed here has already been sanitized by middleware/safety.py.
        The driver should still enforce limit at the driver level as a
        second line of defence — use LIMIT in the query or slice the results.

        Raises:
            RuntimeError: if the query fails for any DB reason.
        """
        ...

    @abstractmethod
    def explain(self, sql: str, analyze: bool = False) -> dict:
        """
        Run EXPLAIN on the query and return the raw plan as a Python dict.

        The dict structure is DB-specific:
        - Postgres returns a list with one element: {"Plan": {...}}
        - SQLite returns a list of flat dicts (EXPLAIN QUERY PLAN rows)
        - MySQL returns a list of dicts (one per EXPLAIN row)

        Claude reads this raw dict. The tool docstring tells Claude how
        to interpret it for the specific DB type.

        If analyze=True, run EXPLAIN ANALYZE (actually executes the query).
        For SQLite, analyze is ignored — EXPLAIN QUERY PLAN always estimates.

        Raises:
            RuntimeError: if EXPLAIN fails.
        """
        ...

    @abstractmethod
    def list_tables(self, schema: str = "public") -> list[TableInfo]:
        """
        Return summary info for all user tables in the given schema.

        For SQLite, schema is ignored (SQLite has no schemas).
        For MySQL, schema maps to the database name.

        Returns an empty list if the schema exists but has no tables.

        Raises:
            RuntimeError: if the schema does not exist or query fails.
        """
        ...

    @abstractmethod
    def get_schema(self, table: str) -> SchemaInfo:
        """
        Return full structural info for a single table.

        Raises:
            ValueError: if the table does not exist.
            RuntimeError: if the query fails for any other reason.
        """
        ...

    @abstractmethod
    def get_table_stats(self, table: str) -> TableStats:
        """
        Return performance-relevant statistics for a table.

        For drivers where some stats are unavailable (e.g. SQLite has
        no vacuum or cache hit stats), set those fields to None.
        Never raise an error just because a stat is unavailable.

        Raises:
            ValueError: if the table does not exist.
        """
        ...

    @abstractmethod
    def get_slow_queries(self, min_ms: int = 100) -> list[SlowQuery]:
        """
        Return queries slower than min_ms milliseconds on average.

        For Postgres: reads from pg_stat_statements.
        Requires: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
        and pg_stat_statements in shared_preload_libraries in postgresql.conf.

        For SQLite and MySQL: returns an empty list — these drivers
        do not expose a slow query log through a standard interface.
        Do NOT raise an error, just return [].
        """
        ...

    def close(self) -> None:
        """
        Optional cleanup hook. Called when the MCP server shuts down.
        Drivers that manage connection pools should close them here.
        Default implementation does nothing.
        """
        pass