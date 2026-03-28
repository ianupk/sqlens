"""
Schema tool tests — list_tables, get_schema,
get_table_stats, get_slow_queries.
All tests use the SQLite test DB from conftest.py.
"""
import pytest
from db.sqlite import SQLiteDriver
from tools.schema import make_schema_tools


@pytest.fixture
def driver(sqlite_db_path):
    d = SQLiteDriver(path=str(sqlite_db_path))
    yield d
    d.close()


@pytest.fixture
def schema_tools(driver):
    return make_schema_tools(driver)


@pytest.fixture
def list_tables(schema_tools):
    return schema_tools[0]


@pytest.fixture
def get_schema(schema_tools):
    return schema_tools[1]


@pytest.fixture
def get_table_stats(schema_tools):
    return schema_tools[2]


@pytest.fixture
def get_slow_queries(schema_tools):
    return schema_tools[3]


# ===========================================================================
# list_tables
# ===========================================================================

class TestListTables:

    def test_returns_all_tables(self, list_tables):
        result = list_tables(reason="test")
        names = [t["name"] for t in result["tables"]]
        assert "customers" in names
        assert "orders" in names
        assert "products" in names
        assert "order_items" in names

    def test_table_count_correct(self, list_tables):
        result = list_tables(reason="test")
        assert result["table_count"] == 4
        assert len(result["tables"]) == 4

    def test_returns_required_keys(self, list_tables):
        result = list_tables(reason="test")
        assert "tables" in result
        assert "table_count" in result
        assert "schema" in result

    def test_table_object_has_required_fields(self, list_tables):
        result = list_tables(reason="test")
        for t in result["tables"]:
            assert "name" in t
            assert "schema" in t
            assert "row_estimate" in t
            assert "size_bytes" in t
            assert "size_human" in t

    def test_row_estimate_is_integer(self, list_tables):
        result = list_tables(reason="test")
        for t in result["tables"]:
            assert isinstance(t["row_estimate"], int)

    def test_schema_param_ignored_for_sqlite(self, list_tables):
        result_default = list_tables(reason="test")
        result_custom  = list_tables(schema="ignored", reason="test")
        assert result_default["table_count"] == result_custom["table_count"]

    def test_no_error_key_on_success(self, list_tables):
        result = list_tables(reason="test")
        assert "error" not in result

    def test_size_human_format(self, list_tables):
        result = list_tables(reason="test")
        for t in result["tables"]:
            if t["size_human"] is not None:
                assert any(
                    unit in t["size_human"]
                    for unit in ["B", "KB", "MB", "GB"]
                )

    def test_orders_has_large_row_estimate(self, list_tables):
        """orders has 50k rows — estimate should reflect that."""
        result = list_tables(reason="test")
        orders = next(t for t in result["tables"] if t["name"] == "orders")
        assert orders["row_estimate"] > 1000


# ===========================================================================
# get_schema
# ===========================================================================

class TestGetSchema:

    def test_returns_required_keys(self, get_schema):
        result = get_schema(table="customers", reason="test")
        required = {
            "table", "columns", "indexes",
            "foreign_keys", "row_count",
            "column_count", "pk_columns", "unindexed_columns",
        }
        assert required.issubset(result.keys())

    def test_correct_table_name(self, get_schema):
        result = get_schema(table="orders", reason="test")
        assert result["table"] == "orders"

    def test_column_count_correct(self, get_schema):
        result = get_schema(table="customers", reason="test")
        # customers has: id, name, email, country, created_at
        assert result["column_count"] == 5

    def test_columns_have_required_fields(self, get_schema):
        result = get_schema(table="customers", reason="test")
        for col in result["columns"]:
            assert "name" in col
            assert "data_type" in col
            assert "nullable" in col
            assert "primary_key" in col

    def test_primary_key_detected(self, get_schema):
        result = get_schema(table="customers", reason="test")
        pk = result["pk_columns"]
        assert "id" in pk

    def test_all_column_names_present(self, get_schema):
        result = get_schema(table="customers", reason="test")
        names = [c["name"] for c in result["columns"]]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        assert "country" in names
        assert "created_at" in names

    def test_index_detected(self, get_schema):
        """products has idx_products_category."""
        result = get_schema(table="products", reason="test")
        index_names = [i["name"] for i in result["indexes"]]
        assert "idx_products_category" in index_names

    def test_index_columns_correct(self, get_schema):
        result = get_schema(table="products", reason="test")
        cat_idx = next(
            i for i in result["indexes"]
            if i["name"] == "idx_products_category"
        )
        assert "category" in cat_idx["columns"]

    def test_foreign_key_detected(self, get_schema):
        """orders.customer_id references customers.id."""
        result = get_schema(table="orders", reason="test")
        fk_columns = [fk["column"] for fk in result["foreign_keys"]]
        assert "customer_id" in fk_columns

    def test_foreign_key_references_correct(self, get_schema):
        result = get_schema(table="orders", reason="test")
        fk = next(
            fk for fk in result["foreign_keys"]
            if fk["column"] == "customer_id"
        )
        assert fk["references_table"] == "customers"
        assert fk["references_column"] == "id"

    def test_unindexed_columns_detected(self, get_schema):
        """
        orders.customer_id has no index — should appear
        in unindexed_columns.
        """
        result = get_schema(table="orders", reason="test")
        assert "customer_id" in result["unindexed_columns"]

    def test_indexed_columns_not_in_unindexed(self, get_schema):
        """products.category has an index — must NOT be in unindexed_columns."""
        result = get_schema(table="products", reason="test")
        assert "category" not in result["unindexed_columns"]

    def test_nonexistent_table_returns_error(self, get_schema):
        result = get_schema(table="ghost_table_xyz", reason="test")
        assert "error" in result
        assert result["table"] == "ghost_table_xyz"

    def test_no_error_on_valid_table(self, get_schema):
        result = get_schema(table="orders", reason="test")
        assert "error" not in result

    def test_order_items_has_two_foreign_keys(self, get_schema):
        """order_items references both orders and products."""
        result = get_schema(table="order_items", reason="test")
        fk_columns = [fk["column"] for fk in result["foreign_keys"]]
        assert "order_id" in fk_columns
        assert "product_id" in fk_columns

    def test_column_types_are_strings(self, get_schema):
        result = get_schema(table="products", reason="test")
        for col in result["columns"]:
            assert isinstance(col["data_type"], str)
            assert len(col["data_type"]) > 0


# ===========================================================================
# get_table_stats
# ===========================================================================

class TestGetTableStats:

    def test_returns_required_keys(self, get_table_stats):
        result = get_table_stats(table="orders", reason="test")
        required = {
            "table", "live_row_count", "dead_row_count",
            "last_vacuum", "last_analyze", "cache_hit_ratio",
            "bloat_estimate_bytes", "health_flags",
        }
        assert required.issubset(result.keys())

    def test_table_name_correct(self, get_table_stats):
        result = get_table_stats(table="customers", reason="test")
        assert result["table"] == "customers"

    def test_live_row_count_present(self, get_table_stats):
        result = get_table_stats(table="orders", reason="test")
        assert result["live_row_count"] is not None
        assert result["live_row_count"] > 0

    def test_live_row_count_matches_seed(self, get_table_stats):
        result = get_table_stats(table="orders", reason="test")
        assert result["live_row_count"] == 50000

    def test_sqlite_stats_are_none(self, get_table_stats):
        """SQLite does not expose dead rows, vacuum, or cache stats."""
        result = get_table_stats(table="customers", reason="test")
        assert result["dead_row_count"] is None
        assert result["last_vacuum"] is None
        assert result["last_analyze"] is None
        assert result["cache_hit_ratio"] is None

    def test_health_flags_is_list(self, get_table_stats):
        result = get_table_stats(table="orders", reason="test")
        assert isinstance(result["health_flags"], list)

    def test_sqlite_never_analyzed_flag(self, get_table_stats):
        """
        SQLite returns None for last_analyze so never_analyzed
        flag should be set.
        """
        result = get_table_stats(table="orders", reason="test")
        assert "never_analyzed" in result["health_flags"]

    def test_nonexistent_table_returns_error(self, get_table_stats):
        result = get_table_stats(table="ghost_table_xyz", reason="test")
        assert "error" in result

    def test_no_error_on_valid_table(self, get_table_stats):
        result = get_table_stats(table="customers", reason="test")
        assert "error" not in result

    def test_all_tables_have_stats(self, get_table_stats):
        """Every table in the test DB should return stats without error."""
        for table in ["customers", "products", "orders", "order_items"]:
            result = get_table_stats(table=table, reason="test")
            assert "error" not in result
            assert result["live_row_count"] is not None


# ===========================================================================
# get_slow_queries
# ===========================================================================

class TestGetSlowQueries:

    def test_returns_required_keys(self, get_slow_queries):
        result = get_slow_queries(reason="test")
        required = {
            "queries", "query_count",
            "min_ms", "setup_required", "message",
        }
        assert required.issubset(result.keys())

    def test_returns_empty_list_for_sqlite(self, get_slow_queries):
        """SQLite has no slow query log — returns empty list, not error."""
        result = get_slow_queries(reason="test")
        assert result["queries"] == []
        assert result["query_count"] == 0

    def test_no_error_key_for_sqlite(self, get_slow_queries):
        result = get_slow_queries(reason="test")
        assert "error" not in result

    def test_min_ms_reflected_in_result(self, get_slow_queries):
        result = get_slow_queries(min_ms=500, reason="test")
        assert result["min_ms"] == 500

    def test_message_is_string(self, get_slow_queries):
        result = get_slow_queries(reason="test")
        assert isinstance(result["message"], str)
        assert len(result["message"]) > 0

    def test_setup_required_false_for_sqlite(self, get_slow_queries):
        """
        setup_required should be False for SQLite — it's not that
        the extension is missing, it's that the feature doesn't exist.
        """
        result = get_slow_queries(reason="test")
        assert result["setup_required"] is False

    def test_different_min_ms_values(self, get_slow_queries):
        """Varying min_ms should not raise for any value."""
        for ms in [0, 10, 100, 1000, 9999]:
            result = get_slow_queries(min_ms=ms, reason="test")
            assert "error" not in result


# ===========================================================================
# Helper function tests
# ===========================================================================

class TestHelpers:

    def test_format_bytes_bytes(self):
        from tools.schema import _format_bytes
        assert _format_bytes(512) == "512 B"

    def test_format_bytes_kb(self):
        from tools.schema import _format_bytes
        assert "KB" in _format_bytes(2048)

    def test_format_bytes_mb(self):
        from tools.schema import _format_bytes
        assert "MB" in _format_bytes(5 * 1024 * 1024)

    def test_format_bytes_gb(self):
        from tools.schema import _format_bytes
        assert "GB" in _format_bytes(2 * 1024 ** 3)

    def test_format_bytes_none(self):
        from tools.schema import _format_bytes
        assert _format_bytes(None) is None

    def test_format_ms_milliseconds(self):
        from tools.schema import _format_ms
        assert "ms" in _format_ms(450)

    def test_format_ms_seconds(self):
        from tools.schema import _format_ms
        assert "s" in _format_ms(5000)

    def test_format_ms_minutes(self):
        from tools.schema import _format_ms
        assert "min" in _format_ms(120_000)

    def test_derive_health_flags_never_analyzed(self):
        from tools.schema import _derive_health_flags
        from db.base import TableStats
        stats = TableStats(
            table="t",
            live_row_count=1000,
            dead_row_count=None,
            last_vacuum=None,
            last_analyze=None,
            cache_hit_ratio=None,
            bloat_estimate_bytes=None,
        )
        flags = _derive_health_flags(stats)
        assert "never_analyzed" in flags

    def test_derive_health_flags_needs_vacuum(self):
        from tools.schema import _derive_health_flags
        from db.base import TableStats
        stats = TableStats(
            table="t",
            live_row_count=1000,
            dead_row_count=300,   # 30% dead — exceeds 20% threshold
            last_vacuum=None,
            last_analyze="2024-01-01T00:00:00+00:00",
            cache_hit_ratio=0.99,
            bloat_estimate_bytes=None,
        )
        flags = _derive_health_flags(stats)
        assert "needs_vacuum" in flags

    def test_derive_health_flags_low_cache_hit(self):
        from tools.schema import _derive_health_flags
        from db.base import TableStats
        stats = TableStats(
            table="t",
            live_row_count=1000,
            dead_row_count=0,
            last_vacuum="2024-01-01T00:00:00+00:00",
            last_analyze="2024-01-01T00:00:00+00:00",
            cache_hit_ratio=0.80,   # below 0.95 threshold
            bloat_estimate_bytes=None,
        )
        flags = _derive_health_flags(stats)
        assert "low_cache_hit_ratio" in flags

    def test_derive_health_flags_healthy(self):
        """A recently analyzed table with good stats should have no flags."""
        from tools.schema import _derive_health_flags
        from db.base import TableStats
        from datetime import datetime, timezone
        stats = TableStats(
            table="t",
            live_row_count=1000,
            dead_row_count=10,    # 1% dead — fine
            last_vacuum=datetime.now(timezone.utc).isoformat(),
            last_analyze=datetime.now(timezone.utc).isoformat(),
            cache_hit_ratio=0.99,
            bloat_estimate_bytes=None,
        )
        flags = _derive_health_flags(stats)
        assert "needs_vacuum" not in flags
        assert "low_cache_hit_ratio" not in flags
        assert "never_analyzed" not in flags
        assert "statistics_stale" not in flags
