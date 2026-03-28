"""
Day 1 smoke tests.
These don't test any real DB logic — just that the module structure
is correct and the abstract interface is importable and callable.
"""
import pytest
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
from db.postgres import PostgresDriver
from db.factory import get_driver


def test_dbdriver_is_abstract():
    """You cannot instantiate DBDriver directly."""
    with pytest.raises(TypeError):
        DBDriver()


def test_postgres_driver_importable():
    """PostgresDriver can be imported and instantiated without a real DB."""
    driver = PostgresDriver(dsn="postgresql://fake:fake@localhost/fake")
    assert driver is not None


def test_postgres_driver_methods_raise_not_implemented():
    """Driver methods raise exceptions when trying to connect with fake DSN."""
    from psycopg_pool import PoolTimeout
    driver = PostgresDriver(dsn="postgresql://fake:fake@localhost/fake")

    # With fake credentials, attempting to use the driver should fail
    # (PoolTimeout when trying to connect)
    with pytest.raises((NotImplementedError, PoolTimeout, RuntimeError)):
        driver.execute("SELECT 1")
    with pytest.raises((NotImplementedError, PoolTimeout, RuntimeError)):
        driver.explain("SELECT 1")
    with pytest.raises((NotImplementedError, PoolTimeout, RuntimeError)):
        driver.list_tables()
    with pytest.raises((NotImplementedError, PoolTimeout, RuntimeError)):
        driver.get_schema("orders")
    with pytest.raises((NotImplementedError, PoolTimeout, RuntimeError)):
        driver.get_table_stats("orders")
    # get_slow_queries() gracefully returns [] if pg_stat_statements is not available
    result = driver.get_slow_queries()
    assert isinstance(result, list)


def test_postgres_close_before_pool_init():
    """close() on a driver that never connected should not raise."""
    driver = PostgresDriver(dsn="postgresql://fake:fake@localhost/fake")
    driver.close()  # should be a no-op


def test_dataclasses_instantiate():
    """All dataclasses can be instantiated — no missing required fields."""
    QueryResult(rows=[], columns=[], row_count=0, execution_ms=0.0)
    TableInfo(schema="public", name="orders", row_estimate=0, size_bytes=None)
    ColumnInfo(name="id", data_type="integer", nullable=False,
               default=None, primary_key=True)
    IndexInfo(name="idx_orders_id", columns=["id"], unique=True, index_type="btree")
    ForeignKeyInfo(column="customer_id", references_table="customers",
                   references_column="id")
    TableStats(table="orders", live_row_count=None, dead_row_count=None,
               last_vacuum=None, last_analyze=None,
               cache_hit_ratio=None, bloat_estimate_bytes=None)
    SlowQuery(query="SELECT 1", mean_execution_ms=0.0,
              total_calls=1, total_execution_ms=0.0)


def test_factory_raises_on_unknown_db_type(monkeypatch):
    """factory raises clearly when DB_TYPE is unrecognised."""
    monkeypatch.setenv("DB_TYPE", "oracle")
    with pytest.raises(EnvironmentError, match="Unknown DB_TYPE"):
        get_driver()


def test_factory_raises_when_postgres_url_missing(monkeypatch):
    """factory raises clearly when DB_TYPE=postgres but no URL set."""
    monkeypatch.setenv("DB_TYPE", "postgres")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(EnvironmentError, match="DATABASE_URL"):
        get_driver()
