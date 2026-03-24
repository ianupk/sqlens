"""
Driver tests — run entirely against the SQLite test DB.
Every test uses the sqlite_db_path fixture from conftest.py.
"""
import pytest
from db.sqlite import SQLiteDriver
from db.base import (
    QueryResult, TableInfo, SchemaInfo,
    TableStats, SlowQuery,
)


@pytest.fixture
def driver(sqlite_db_path):
    """Fresh SQLiteDriver pointing at the test DB."""
    d = SQLiteDriver(path=str(sqlite_db_path))
    yield d
    d.close()


# ------------------------------------------------------------------
# execute
# ------------------------------------------------------------------

def test_execute_basic_select(driver):
    result = driver.execute("SELECT * FROM customers")
    assert isinstance(result, QueryResult)
    assert result.row_count == 100
    assert "id" in result.columns
    assert "email" in result.columns
    assert len(result.rows) == 100
    assert result.execution_ms >= 0


def test_execute_with_where(driver):
    result = driver.execute(
        "SELECT * FROM customers WHERE country = 'IN'"
    )
    assert result.row_count > 0
    assert all(r["country"] == "IN" for r in result.rows)


def test_execute_respects_limit(driver):
    result = driver.execute("SELECT * FROM customers", limit=10)
    assert result.row_count <= 10


def test_execute_join(driver):
    result = driver.execute("""
        SELECT c.name, COUNT(o.id) as order_count
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        GROUP BY c.id, c.name
        LIMIT 5
    """)
    assert result.row_count > 0
    assert "order_count" in result.columns


def test_execute_bad_sql_raises(driver):
    with pytest.raises(RuntimeError):
        driver.execute("SELECT * FROM nonexistent_table_xyz")


def test_execute_rows_are_dicts(driver):
    result = driver.execute("SELECT id, name FROM customers LIMIT 1")
    assert isinstance(result.rows[0], dict)
    assert "id" in result.rows[0]
    assert "name" in result.rows[0]


# ------------------------------------------------------------------
# explain
# ------------------------------------------------------------------

def test_explain_returns_dict(driver):
    result = driver.explain("SELECT * FROM orders WHERE customer_id = 1")
    assert isinstance(result, dict)
    assert result["dialect"] == "sqlite"
    assert "plan" in result
    assert isinstance(result["plan"], list)


def test_explain_plan_has_expected_keys(driver):
    result = driver.explain("SELECT * FROM orders")
    for row in result["plan"]:
        # SQLite EXPLAIN QUERY PLAN rows always have these keys
        assert "id" in row
        assert "detail" in row


def test_explain_analyze_ignored_for_sqlite(driver):
    """analyze=True should not raise for SQLite — it's silently ignored."""
    result = driver.explain(
        "SELECT * FROM customers LIMIT 10",
        analyze=True
    )
    assert result["dialect"] == "sqlite"


# ------------------------------------------------------------------
# list_tables
# ------------------------------------------------------------------

def test_list_tables_returns_all_tables(driver):
    tables = driver.list_tables()
    names = [t.name for t in tables]
    assert "customers" in names
    assert "orders" in names
    assert "products" in names
    assert "order_items" in names


def test_list_tables_returns_table_info_objects(driver):
    tables = driver.list_tables()
    for t in tables:
        assert isinstance(t, TableInfo)
        assert t.schema == "main"
        assert isinstance(t.name, str)
        assert isinstance(t.row_estimate, int)


def test_list_tables_schema_param_ignored(driver):
    """SQLite ignores schema param — should still return tables."""
    tables_default = driver.list_tables()
    tables_custom  = driver.list_tables(schema="anything")
    assert len(tables_default) == len(tables_custom)


# ------------------------------------------------------------------
# get_schema
# ------------------------------------------------------------------

def test_get_schema_customers(driver):
    schema = driver.get_schema("customers")
    assert isinstance(schema, SchemaInfo)
    assert schema.table == "customers"

    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "email" in col_names
    assert "country" in col_names


def test_get_schema_primary_key_detected(driver):
    schema = driver.get_schema("customers")
    pk_cols = [c for c in schema.columns if c.primary_key]
    assert len(pk_cols) == 1
    assert pk_cols[0].name == "id"


def test_get_schema_index_detected(driver):
    """products has idx_products_category — must appear in schema."""
    schema = driver.get_schema("products")
    index_names = [i.name for i in schema.indexes]
    assert "idx_products_category" in index_names


def test_get_schema_foreign_key_detected(driver):
    """orders.customer_id references customers.id."""
    schema = driver.get_schema("orders")
    fk_cols = [fk.column for fk in schema.foreign_keys]
    assert "customer_id" in fk_cols


def test_get_schema_nonexistent_table_raises(driver):
    with pytest.raises(ValueError, match="does not exist"):
        driver.get_schema("totally_fake_table")


def test_get_schema_column_types_present(driver):
    schema = driver.get_schema("products")
    for col in schema.columns:
        assert col.data_type  # should never be empty string


# ------------------------------------------------------------------
# get_table_stats
# ------------------------------------------------------------------

def test_get_table_stats_returns_stats_object(driver):
    stats = driver.get_table_stats("orders")
    assert isinstance(stats, TableStats)
    assert stats.table == "orders"


def test_get_table_stats_live_count_present(driver):
    stats = driver.get_table_stats("orders")
    assert stats.live_row_count is not None
    assert stats.live_row_count > 0


def test_get_table_stats_sqlite_fields_are_none(driver):
    """SQLite-specific — these fields should always be None."""
    stats = driver.get_table_stats("customers")
    assert stats.dead_row_count is None
    assert stats.last_vacuum is None
    assert stats.last_analyze is None
    assert stats.cache_hit_ratio is None


def test_get_table_stats_nonexistent_raises(driver):
    with pytest.raises(ValueError, match="does not exist"):
        driver.get_table_stats("ghost_table")


# ------------------------------------------------------------------
# get_slow_queries
# ------------------------------------------------------------------

def test_get_slow_queries_returns_list(driver):
    """SQLite always returns empty list — never raises."""
    result = driver.get_slow_queries()
    assert isinstance(result, list)
    assert result == []


def test_get_slow_queries_with_min_ms(driver):
    result = driver.get_slow_queries(min_ms=500)
    assert isinstance(result, list)


# ------------------------------------------------------------------
# close
# ------------------------------------------------------------------

def test_close_can_be_called_twice(driver):
    """close() should be idempotent."""
    driver.close()
    driver.close()  # should not raise


def test_driver_usable_after_multiple_queries(driver):
    """Connection should stay alive across multiple calls."""
    for _ in range(5):
        result = driver.execute("SELECT COUNT(*) as n FROM customers")
        assert result.rows[0]["n"] == 100