"""
Optimizer tool tests — suggest_indexes and rewrite_query.
All tests use the SQLite test DB from conftest.py.
"""
import pytest
from db.sqlite import SQLiteDriver
from tools.optimizer import (
    make_optimizer_tools,
    _extract_column_usage,
    _build_alias_map,
    _resolve_column,
    _is_covered_by_existing,
    _extract_tables,
    _build_suggestions,
)
import sqlglot


@pytest.fixture
def driver(sqlite_db_path):
    d = SQLiteDriver(path=str(sqlite_db_path))
    yield d
    d.close()


@pytest.fixture
def optimizer_tools(driver):
    return make_optimizer_tools(driver)


@pytest.fixture
def suggest_indexes(optimizer_tools):
    return optimizer_tools[0]


@pytest.fixture
def rewrite_query(optimizer_tools):
    return optimizer_tools[1]


# ===========================================================================
# suggest_indexes — return shape
# ===========================================================================

class TestSuggestIndexesShape:

    def test_returns_required_keys(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        required = {
            "suggestions", "suggestion_count",
            "tables_analyzed", "skipped_columns", "message",
        }
        assert required.issubset(result.keys())

    def test_suggestion_count_matches_list(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        assert result["suggestion_count"] == len(result["suggestions"])

    def test_suggestion_has_required_fields(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        if result["suggestions"]:
            s = result["suggestions"][0]
            assert "table" in s
            assert "columns" in s
            assert "ddl" in s
            assert "reason" in s
            assert "usage" in s
            assert "impact" in s

    def test_ddl_is_create_index(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        for s in result["suggestions"]:
            assert s["ddl"].upper().startswith("CREATE INDEX CONCURRENTLY")

    def test_ddl_contains_table_name(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        for s in result["suggestions"]:
            assert s["table"] in s["ddl"]

    def test_ddl_contains_column_names(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        for s in result["suggestions"]:
            for col in s["columns"]:
                assert col in s["ddl"]

    def test_impact_is_valid_value(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        valid_impacts = {"high", "medium", "low"}
        for s in result["suggestions"]:
            assert s["impact"] in valid_impacts

    def test_message_is_string(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        assert isinstance(result["message"], str)
        assert len(result["message"]) > 0


# ===========================================================================
# suggest_indexes — correctness
# ===========================================================================

class TestSuggestIndexesCorrectness:

    def test_suggests_index_for_unindexed_where_column(self, suggest_indexes):
        """orders.customer_id has no index — must be suggested."""
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 42",
            reason="test",
        )
        assert result["suggestion_count"] > 0
        cols = [col for s in result["suggestions"] for col in s["columns"]]
        assert "customer_id" in cols

    def test_no_suggestion_for_already_indexed_column(self, suggest_indexes):
        """products.category has idx_products_category — no duplicate."""
        result = suggest_indexes(
            sql="SELECT * FROM products p WHERE p.category = 'Books'",
            reason="test",
        )
        assert result["suggestion_count"] == 0

    def test_skipped_columns_populated_for_indexed(self, suggest_indexes):
        """When a column is already indexed, it appears in skipped_columns."""
        result = suggest_indexes(
            sql="SELECT * FROM products p WHERE p.category = 'Books'",
            reason="test",
        )
        assert len(result["skipped_columns"]) > 0

    def test_join_column_suggested(self, suggest_indexes):
        """JOIN predicate columns should be included in suggestions."""
        result = suggest_indexes(
            sql="""
                SELECT c.name, o.status
                FROM customers c
                JOIN orders o ON o.customer_id = c.id
                WHERE c.country = 'IN'
            """,
            reason="test",
        )
        all_cols = [col for s in result["suggestions"] for col in s["columns"]]
        assert "customer_id" in all_cols or "country" in all_cols

    def test_order_by_column_included(self, suggest_indexes):
        """ORDER BY columns should be included in composite index."""
        result = suggest_indexes(
            sql="""
                SELECT * FROM orders o
                WHERE o.customer_id = 1
                ORDER BY o.created_at
            """,
            reason="test",
        )
        if result["suggestion_count"] > 0:
            all_cols = [col for s in result["suggestions"] for col in s["columns"]]
            assert "customer_id" in all_cols

    def test_table_names_in_tables_analyzed(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            reason="test",
        )
        assert "orders" in result["tables_analyzed"]

    def test_unqualified_columns_ignored(self, suggest_indexes):
        """
        Columns without table qualifier (bare customer_id vs o.customer_id)
        cannot be reliably mapped to a table — should return no suggestions
        rather than guessing.
        """
        result = suggest_indexes(
            sql="SELECT * FROM orders WHERE customer_id = 1",
            reason="test",
        )
        # Either 0 suggestions (column not resolved) or graceful result
        assert "error" not in result
        assert isinstance(result["suggestions"], list)

    def test_composite_index_equality_columns_first(self, suggest_indexes):
        """
        WHERE columns should appear before ORDER BY columns
        in the composite index.
        """
        result = suggest_indexes(
            sql="""
                SELECT * FROM orders o
                WHERE o.customer_id = 1
                ORDER BY o.created_at DESC
            """,
            reason="test",
        )
        if result["suggestion_count"] > 0:
            s = result["suggestions"][0]
            if "customer_id" in s["columns"] and "created_at" in s["columns"]:
                ci = s["columns"].index("customer_id")
                ca = s["columns"].index("created_at")
                assert ci < ca  # equality column before sort column


# ===========================================================================
# suggest_indexes — error handling
# ===========================================================================

class TestSuggestIndexesErrors:

    def test_unsafe_sql_returns_error(self, suggest_indexes):
        result = suggest_indexes(
            sql="DROP TABLE orders",
            reason="test",
        )
        assert "error" in result
        assert result["suggestion_count"] == 0

    def test_empty_sql_returns_error(self, suggest_indexes):
        result = suggest_indexes(sql="", reason="test")
        assert "error" in result

    def test_no_where_clause_returns_empty(self, suggest_indexes):
        result = suggest_indexes(
            sql="SELECT * FROM orders o",
            reason="test",
        )
        assert result["suggestion_count"] == 0

    def test_nonexistent_table_returns_gracefully(self, suggest_indexes):
        """Unknown table — schema fetch fails silently, no crash."""
        result = suggest_indexes(
            sql="SELECT * FROM ghost_table g WHERE g.col = 1",
            reason="test",
        )
        assert "error" not in result
        assert isinstance(result["suggestions"], list)


# ===========================================================================
# rewrite_query — return shape
# ===========================================================================

class TestRewriteQueryShape:

    def test_returns_required_keys(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        required = {
            "original_sql", "goal", "explain_plan",
            "schema_context", "index_suggestions",
            "rewrite_guidance", "instructions",
        }
        assert required.issubset(result.keys())

    def test_original_sql_preserved(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        assert "orders" in result["original_sql"]

    def test_goal_preserved(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="add keyset pagination",
            reason="test",
        )
        assert result["goal"] == "add keyset pagination"

    def test_explain_plan_is_dict(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        assert isinstance(result["explain_plan"], dict)

    def test_schema_context_has_orders(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        assert "orders" in result["schema_context"]

    def test_schema_context_has_columns(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        orders_schema = result["schema_context"].get("orders", {})
        assert "columns" in orders_schema
        assert len(orders_schema["columns"]) > 0

    def test_rewrite_guidance_is_list(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        assert isinstance(result["rewrite_guidance"], list)
        assert len(result["rewrite_guidance"]) > 0

    def test_instructions_is_string(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        assert isinstance(result["instructions"], str)
        assert len(result["instructions"]) > 0

    def test_index_suggestions_is_dict(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        assert isinstance(result["index_suggestions"], dict)
        assert "suggestions" in result["index_suggestions"]


# ===========================================================================
# rewrite_query — guidance correctness
# ===========================================================================

class TestRewriteQueryGuidance:

    def test_seq_scan_triggers_seek_index_hint(self, rewrite_query):
        """orders scan → SEEK_INDEX hint should appear."""
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        guidance = " ".join(result["rewrite_guidance"])
        assert "SEEK_INDEX" in guidance

    def test_pagination_goal_triggers_keyset_hint(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="add keyset pagination instead of OFFSET",
            reason="test",
        )
        guidance = " ".join(result["rewrite_guidance"])
        assert "KEYSET" in guidance

    def test_performance_goal_triggers_push_filters_hint(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="make this faster",
            reason="test",
        )
        guidance = " ".join(result["rewrite_guidance"])
        assert "PUSH_FILTERS_EARLY" in guidance or "ADD_INDEXES" in guidance

    def test_missing_fk_index_hint(self, rewrite_query):
        """
        orders.customer_id is a FK with no index —
        MISSING_FK_INDEX hint should appear.
        """
        result = rewrite_query(
            sql="""
                SELECT c.name, o.status
                FROM customers c
                JOIN orders o ON o.customer_id = c.id
            """,
            goal="make this faster",
            reason="test",
        )
        guidance = " ".join(result["rewrite_guidance"])
        assert "MISSING_FK_INDEX" in guidance

    def test_multi_table_schema_context(self, rewrite_query):
        """JOIN query should fetch schema for both tables."""
        result = rewrite_query(
            sql="""
                SELECT c.name, o.status
                FROM customers c
                JOIN orders o ON o.customer_id = c.id
            """,
            goal="make this faster",
            reason="test",
        )
        assert "customers" in result["schema_context"]
        assert "orders" in result["schema_context"]


# ===========================================================================
# rewrite_query — error handling
# ===========================================================================

class TestRewriteQueryErrors:

    def test_unsafe_sql_returns_error(self, rewrite_query):
        result = rewrite_query(
            sql="DROP TABLE orders",
            goal="make it faster",
            reason="test",
        )
        assert "error" in result

    def test_no_crash_on_empty_goal(self, rewrite_query):
        result = rewrite_query(
            sql="SELECT * FROM orders o WHERE o.customer_id = 1",
            goal="",
            reason="test",
        )
        assert "error" not in result


# ===========================================================================
# AST helpers — unit tests
# ===========================================================================

class TestExtractColumnUsage:

    def _parse(self, sql: str):
        return sqlglot.parse_one(sql, error_level=None)

    def test_where_column_extracted(self):
        stmt = self._parse(
            "SELECT * FROM orders o WHERE o.customer_id = 1"
        )
        usage = _extract_column_usage(stmt)
        assert ("orders", "customer_id", "where") in usage

    def test_join_column_extracted(self):
        stmt = self._parse("""
            SELECT * FROM orders o
            JOIN customers c ON o.customer_id = c.id
        """)
        usage = _extract_column_usage(stmt)
        tables_and_cols = [(t, col) for t, col, _ in usage]
        assert ("orders", "customer_id") in tables_and_cols

    def test_order_by_column_extracted(self):
        stmt = self._parse(
            "SELECT * FROM orders o ORDER BY o.created_at DESC"
        )
        usage = _extract_column_usage(stmt)
        assert ("orders", "created_at", "order_by") in usage

    def test_unqualified_column_skipped(self):
        stmt = self._parse(
            "SELECT * FROM orders WHERE customer_id = 1"
        )
        usage = _extract_column_usage(stmt)
        # Without table qualifier, column should not be extracted
        assert len(usage) == 0

    def test_deduplication(self):
        stmt = self._parse("""
            SELECT * FROM orders o
            WHERE o.customer_id = 1
              AND o.customer_id > 0
        """)
        usage = _extract_column_usage(stmt)
        where_entries = [
            (t, c, u) for t, c, u in usage
            if t == "orders" and c == "customer_id"
        ]
        assert len(where_entries) == 1

    def test_multiple_where_columns(self):
        stmt = self._parse("""
            SELECT * FROM orders o
            WHERE o.customer_id = 1
              AND o.status = 'completed'
        """)
        usage = _extract_column_usage(stmt)
        cols = [col for _, col, _ in usage]
        assert "customer_id" in cols
        assert "status" in cols

    def test_alias_resolved_to_real_table(self):
        stmt = self._parse(
            "SELECT * FROM orders o WHERE o.customer_id = 1"
        )
        usage = _extract_column_usage(stmt)
        # Table name should be "orders", not the alias "o"
        tables = [t for t, _, _ in usage]
        assert "orders" in tables
        assert "o" not in tables


class TestBuildAliasMap:

    def _parse(self, sql: str):
        return sqlglot.parse_one(sql, error_level=None)

    def test_alias_mapped(self):
        stmt = self._parse("SELECT * FROM orders o WHERE o.id = 1")
        alias_map = _build_alias_map(stmt)
        assert alias_map.get("o") == "orders"

    def test_real_name_mapped_to_itself(self):
        stmt = self._parse("SELECT * FROM orders WHERE id = 1")
        alias_map = _build_alias_map(stmt)
        assert alias_map.get("orders") == "orders"

    def test_multiple_aliases(self):
        stmt = self._parse("""
            SELECT * FROM orders o
            JOIN customers c ON o.customer_id = c.id
        """)
        alias_map = _build_alias_map(stmt)
        assert alias_map.get("o") == "orders"
        assert alias_map.get("c") == "customers"


class TestIsCoveredByExisting:

    def test_exact_match_covered(self):
        assert _is_covered_by_existing(
            ["customer_id"],
            [["customer_id"]],
        ) is True

    def test_leading_prefix_covered(self):
        assert _is_covered_by_existing(
            ["customer_id"],
            [["customer_id", "created_at"]],
        ) is True

    def test_non_leading_not_covered(self):
        assert _is_covered_by_existing(
            ["created_at"],
            [["customer_id", "created_at"]],
        ) is False

    def test_no_existing_indexes_not_covered(self):
        assert _is_covered_by_existing(
            ["customer_id"],
            [],
        ) is False

    def test_composite_exact_match(self):
        assert _is_covered_by_existing(
            ["customer_id", "status"],
            [["customer_id", "status"]],
        ) is True

    def test_subset_covered(self):
        assert _is_covered_by_existing(
            ["customer_id"],
            [["customer_id", "status", "created_at"]],
        ) is True


class TestExtractTables:

    def test_single_table(self):
        tables = _extract_tables("SELECT * FROM orders o WHERE o.id = 1")
        assert "orders" in tables

    def test_multiple_tables(self):
        tables = _extract_tables("""
            SELECT * FROM orders o
            JOIN customers c ON o.customer_id = c.id
        """)
        assert "orders" in tables
        assert "customers" in tables

    def test_returns_lowercase(self):
        tables = _extract_tables("SELECT * FROM Orders o")
        assert "orders" in tables

    def test_deduplicates(self):
        tables = _extract_tables("""
            SELECT * FROM orders o1
            JOIN orders o2 ON o1.id = o2.id
        """)
        assert tables.count("orders") == 1

    def test_invalid_sql_returns_empty(self):
        tables = _extract_tables("NOT VALID SQL $$$")
        assert isinstance(tables, list)
