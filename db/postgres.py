import os
import time
from typing import Any

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
    PostgreSQL driver using psycopg3 (the `psycopg` package, not psycopg2).

    Key differences from psycopg2:
    - Import is `import psycopg` not `import psycopg2`
    - Connection strings use the same format
    - psycopg3 has better async support (we don't use it here, but good to know)
    - Row factory lets us get dict rows natively

    Connection pooling:
    - Uses psycopg_pool.ConnectionPool for efficiency
    - Pool is created once at init, reused across all tool calls
    - Pool is closed in the close() method

    Read-only enforcement:
    - Connects as a read-only Postgres role (set up in docker-compose)
    - The safety layer in middleware/safety.py is a second line of defence
    """

    def __init__(self, dsn: str):
        """
        dsn: a libpq connection string, e.g.:
             postgresql://readonly_user:secret@localhost:5432/devdb
             or the equivalent key=value format
        """
        self.dsn = dsn
        self._pool = None   # initialized lazily in _get_pool()

    def _get_pool(self):
        """
        Lazily initialize the connection pool on first use.
        This avoids connecting to the DB at import time, which would
        break tests that don't have a Postgres instance available.
        """
        if self._pool is None:
            # Imported here so that missing psycopg doesn't break
            # imports when using the SQLite driver
            from psycopg_pool import ConnectionPool
            self._pool = ConnectionPool(
                conninfo=self.dsn,
                min_size=1,
                max_size=5,
                # Don't open connections at startup — wait until first use
                open=False,
            )
            self._pool.open(wait=True, timeout=5.0)
        return self._pool

    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        raise NotImplementedError("PostgresDriver.execute() — implement on Day 2")

    def explain(self, sql: str, analyze: bool = False) -> dict:
        raise NotImplementedError("PostgresDriver.explain() — implement on Day 2")

    def list_tables(self, schema: str = "public") -> list[TableInfo]:
        raise NotImplementedError("PostgresDriver.list_tables() — implement on Day 2")

    def get_schema(self, table: str) -> SchemaInfo:
        raise NotImplementedError("PostgresDriver.get_schema() — implement on Day 2")

    def get_table_stats(self, table: str) -> TableStats:
        raise NotImplementedError("PostgresDriver.get_table_stats() — implement on Day 2")

    def get_slow_queries(self, min_ms: int = 100) -> list[SlowQuery]:
        raise NotImplementedError("PostgresDriver.get_slow_queries() — implement on Day 2")

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None