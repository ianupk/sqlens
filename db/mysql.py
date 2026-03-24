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


class MySQLDriver(DBDriver):
    """
    MySQL driver using mysql-connector-python.

    Limitations vs Postgres:
    - No pg_stat_statements equivalent — get_slow_queries returns []
      unless the slow query log is enabled and accessible
    - No MVCC dead tuples — dead_row_count is always None
    - EXPLAIN returns tabular rows, not JSON tree (MySQL 8+ supports
      FORMAT=JSON but we use the simple format for compatibility)
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
    ):
        self.config = dict(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
        )
        self._conn = None

    def _get_conn(self):
        """Lazily connect, reconnect if connection dropped."""
        import mysql.connector
        if self._conn is None or not self._conn.is_connected():
            self._conn = mysql.connector.connect(**self.config)
        return self._conn

    def _rows_to_dicts(self, cursor) -> list[dict]:
        if not cursor.description:
            return []
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def execute(self, sql: str, limit: int = 100) -> QueryResult:
        normalized = sql.strip().rstrip(";")
        if "limit" not in normalized.lower():
            normalized = f"{normalized} LIMIT {limit}"

        conn = self._get_conn()
        cursor = conn.cursor()
        start = time.perf_counter()

        try:
            cursor.execute(normalized)
            raw_rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description] \
                      if cursor.description else []
        except Exception as e:
            raise RuntimeError(f"Query failed: {e}") from e
        finally:
            cursor.close()

        elapsed_ms = (time.perf_counter() - start) * 1000
        rows = [dict(zip(columns, r)) for r in raw_rows]

        return QueryResult(
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_ms=round(elapsed_ms, 3),
        )

    def explain(self, sql: str, analyze: bool = False) -> dict:
        explain_sql = f"EXPLAIN {sql}"
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(explain_sql)
            rows = self._rows_to_dicts(cursor)
        except Exception as e:
            raise RuntimeError(f"EXPLAIN failed: {e}") from e
        finally:
            cursor.close()

        return {
            "dialect": "mysql",
            "plan": rows,
        }

    def list_tables(self, schema: str = "public") -> list[TableInfo]:
        # In MySQL, schema = database name
        db_name = self.config["database"]
        sql = """
            SELECT
                table_name      AS name,
                table_rows      AS row_estimate,
                data_length + index_length AS size_bytes
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type   = 'BASE TABLE'
            ORDER BY table_name
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(sql, (db_name,))
            rows = self._rows_to_dicts(cursor)
        except Exception as e:
            raise RuntimeError(f"list_tables failed: {e}") from e
        finally:
            cursor.close()

        return [
            TableInfo(
                schema=db_name,
                name=row["name"],
                row_estimate=int(row["row_estimate"] or 0),
                size_bytes=int(row["size_bytes"] or 0),
            )
            for row in rows
        ]

    def get_schema(self, table: str) -> SchemaInfo:
        db_name = self.config["database"]
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            # Verify table exists
            cursor.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (db_name, table))
            if not cursor.fetchone():
                raise ValueError(f"Table '{table}' does not exist.")

            # Columns
            cursor.execute("""
                SELECT
                    column_name,
                    column_type,
                    is_nullable,
                    column_default,
                    column_key
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (db_name, table))
            col_rows = self._rows_to_dicts(cursor)

            columns = [
                ColumnInfo(
                    name=r["column_name"],
                    data_type=r["column_type"],
                    nullable=r["is_nullable"] == "YES",
                    default=r["column_default"],
                    primary_key=r["column_key"] == "PRI",
                )
                for r in col_rows
            ]

            # Indexes
            cursor.execute(f"SHOW INDEX FROM `{table}`")
            idx_rows = self._rows_to_dicts(cursor)

            indexes_map: dict[str, IndexInfo] = {}
            for r in idx_rows:
                name = r["Key_name"]
                if name not in indexes_map:
                    indexes_map[name] = IndexInfo(
                        name=name,
                        columns=[],
                        unique=r["Non_unique"] == 0,
                        index_type=r["Index_type"].lower(),
                    )
                indexes_map[name].columns.append(r["Column_name"])

            # Foreign keys
            cursor.execute("""
                SELECT
                    kcu.column_name,
                    kcu.referenced_table_name,
                    kcu.referenced_column_name
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema    = kcu.table_schema
                WHERE kcu.table_schema  = %s
                  AND kcu.table_name    = %s
                  AND tc.constraint_type = 'FOREIGN KEY'
            """, (db_name, table))
            fk_rows = self._rows_to_dicts(cursor)

            foreign_keys = [
                ForeignKeyInfo(
                    column=r["column_name"],
                    references_table=r["referenced_table_name"],
                    references_column=r["referenced_column_name"],
                )
                for r in fk_rows
            ]

        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(f"get_schema failed: {e}") from e
        finally:
            cursor.close()

        return SchemaInfo(
            table=table,
            columns=columns,
            indexes=list(indexes_map.values()),
            foreign_keys=foreign_keys,
            row_count=None,
        )

    def get_table_stats(self, table: str) -> TableStats:
        db_name = self.config["database"]
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT table_rows, data_length + index_length AS size_bytes
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (db_name, table))
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Table '{table}' does not exist.")
        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(f"get_table_stats failed: {e}") from e
        finally:
            cursor.close()

        return TableStats(
            table=table,
            live_row_count=int(row[0] or 0),
            dead_row_count=None,
            last_vacuum=None,
            last_analyze=None,
            cache_hit_ratio=None,
            bloat_estimate_bytes=None,
        )

    def get_slow_queries(self, min_ms: int = 100) -> list[SlowQuery]:
        return []

    def close(self) -> None:
        if self._conn and self._conn.is_connected():
            self._conn.close()
            self._conn = None