"""
Tool function tests — run_query and explain_query.
All tests use the SQLite test DB from conftest.py.
"""
import pytest
from db.sqlite import SQLiteDriver
from tools.query import make_query_tools


@pytest.fixture
def driver(sqlite_db_path):
    d = SQLiteDriver(path=str(sqlite_db_path))
    yield d
    d.close()


@pytest.fixture
def tools(driver):
    run_query, explain_query = make_query_tools(driver)
    return run_query, explain_query


@pytest.fixture
def run_query(tools):
    return tools[0]


@pytest.fixture
def explain_query(tools):
    return tools[1]


# ===========================================================================
# run_query — happy path
# ===========================================================================

class TestRunQuery:

    def test_basic_select(self, run_query):
        result = run_query(
            sql="SELECT * FROM customers",
            reason="testing basic select",
        )
        assert "error" not in result
        assert result["row_count"] == 100
        assert "id" in result["columns"]
        assert isinstance(result["rows"], list)
        assert isinstance(result["execution_ms"], float)

    def test_returns_dict_rows(self, run_query):
        result = run_query(
            sql="SELECT id, name FROM customers LIMIT 5",
            reason="testing row format",
        )
        for row in result["rows"]:
            assert isinstance(row, dict)
            assert "id" in row
            assert "name" in row

    def test_limit_is_respected(self, run_query):
        result = run_query(
            sql="SELECT * FROM customers",
            limit=10,
            reason="testing limit",
        )
        assert result["row_count"] <= 10

    def test_truncated_flag_set_when_limit_hit(self, run_query):
        result = run_query(
            sql="SELECT * FROM customers",
            limit=10,
            reason="testing truncated flag",
        )
        assert result["truncated"] is True

    def test_truncated_flag_false_when_under_limit(self, run_query):
        result = run_query(
            sql="SELECT * FROM customers WHERE country = 'IN' LIMIT 5",
            limit=100,
            reason="testing truncated flag false",
        )
        assert result["truncated"] is False

    def test_where_clause(self, run_query):
        result = run_query(
            sql="SELECT * FROM customers WHERE country = 'US'",
            reason="filtering by country",
        )
        assert all(r["country"] == "US" for r in result["rows"])

    def test_join_query(self, run_query):
        result = run_query(
            sql="""
                SELECT c.name, COUNT(o.id) AS order_count
                FROM customers c
                JOIN orders o ON o.customer_id = c.id
                GROUP BY c.id, c.name
                ORDER BY order_count DESC
                LIMIT 5
            """,
            reason="testing join",
        )
        assert result["row_count"] > 0
        assert "order_count" in result["columns"]

    def test_aggregate_query(self, run_query):
        result = run_query(
            sql="SELECT COUNT(*) AS total, AVG(total_cents) AS avg_total FROM orders",
            reason="aggregate test",
        )
        assert result["row_count"] == 1
        assert result["rows"][0]["total"] == 50000   # 50k orders in conftest

    def test_execution_ms_is_positive(self, run_query):
        result = run_query(
            sql="SELECT 1 AS n",
            reason="timing test",
        )
        assert result["execution_ms"] >= 0

    def test_columns_list_correct(self, run_query):
        result = run_query(
            sql="SELECT id, name, country FROM customers LIMIT 1",
            reason="columns test",
        )
        assert result["columns"] == ["id", "name", "country"]

    def test_reason_param_not_in_result(self, run_query):
        result = run_query(
            sql="SELECT 1 AS n",
            reason="should not appear in output",
        )
        assert "reason" not in result

    def test_empty_result_set(self, run_query):
        result = run_query(
            sql="SELECT * FROM customers WHERE id = 999999",
            reason="empty result test",
        )
        assert result["row_count"] == 0
        assert result["rows"] == []
        assert "error" not in result


# ===========================================================================
# run_query — error handling
# ===========================================================================

class TestRunQueryErrors:

    def test_unsafe_sql_returns_error_dict(self, run_query):
        result = run_query(
            sql="DROP TABLE customers",
            reason="testing safety",
        )
        assert "error" in result
        assert result["blocked"] is True
        assert result["row_count"] == 0

    def test_insert_is_blocked(self, run_query):
        result = run_query(
            sql="INSERT INTO customers (name) VALUES ('evil')",
            reason="testing safety",
        )
        assert result["blocked"] is True

    def test_nonexistent_table_returns_error(self, run_query):
        result = run_query(
            sql="SELECT * FROM nonexistent_table_xyz",
            reason="testing bad table",
        )
        assert "error" in result
        assert result["blocked"] is False

    def test_invalid_column_returns_error(self, run_query):
        result = run_query(
            sql="SELECT nonexistent_column FROM customers",
            reason="testing bad column",
        )
        assert "error" in result

    def test_error_result_has_empty_rows(self, run_query):
        result = run_query(
            sql="SELECT * FROM ghost_table",
            reason="error structure test",
        )
        assert result["rows"] == []
        assert result["columns"] == []


# ===========================================================================
# explain_query — happy path
# ===========================================================================

class TestExplainQuery:

    def test_returns_dict(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM orders",
            reason="basic explain test",
        )
        assert isinstance(result, dict)

    def test_required_keys_present(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM orders",
            reason="keys test",
        )
        required_keys = {
            "dialect", "summary", "has_seq_scan",
            "has_bad_estimate", "slowest_nodes", "plan_tree",
        }
        assert required_keys.issubset(result.keys())

    def test_dialect_is_sqlite(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM customers",
            reason="dialect test",
        )
        assert result["dialect"] == "sqlite"

    def test_full_scan_detected(self, explain_query):
        """
        orders has no index on customer_id and 50k rows —
        SQLite emits SCAN which the parser flags as slow.
        """
        result = explain_query(
            sql="SELECT * FROM orders WHERE customer_id = 42",
            reason="testing scan detection",
        )
        assert result["has_seq_scan"] is True

    def test_index_scan_detected(self, explain_query):
        """
        products has idx_products_category — filtering on category
        should use the index and NOT trigger has_seq_scan.
        """
        result = explain_query(
            sql="SELECT * FROM products WHERE category = 'Books'",
            reason="testing index detection",
        )
        assert result["has_seq_scan"] is False

    def test_summary_is_nonempty_string(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM orders",
            reason="summary test",
        )
        assert isinstance(result["summary"], str)
        assert len(result["summary"]) > 0

    def test_plan_tree_is_list(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM customers LIMIT 10",
            reason="plan tree test",
        )
        assert isinstance(result["plan_tree"], list)

    def test_slowest_nodes_is_list(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM orders WHERE customer_id = 1",
            reason="slowest nodes test",
        )
        assert isinstance(result["slowest_nodes"], list)

    def test_analyze_true_does_not_raise(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM customers LIMIT 5",
            analyze=True,
            reason="analyze flag test",
        )
        assert "error" not in result

    def test_join_explain(self, explain_query):
        result = explain_query(
            sql="""
                SELECT c.name, o.status
                FROM customers c
                JOIN orders o ON o.customer_id = c.id
                WHERE c.country = 'IN'
                LIMIT 10
            """,
            reason="join explain test",
        )
        assert "error" not in result
        assert isinstance(result["plan_tree"], list)
        assert len(result["plan_tree"]) > 0


# ===========================================================================
# explain_query — error handling
# ===========================================================================

class TestExplainQueryErrors:

    def test_unsafe_sql_returns_error(self, explain_query):
        result = explain_query(
            sql="DROP TABLE orders",
            reason="safety test",
        )
        assert "error" in result
        assert result["blocked"] is True

    def test_invalid_sql_returns_error(self, explain_query):
        result = explain_query(
            sql="SELECT * FROM nonexistent_xyz",
            reason="invalid table test",
        )
        assert "error" in result


# ===========================================================================
# plan_parser — unit tests
# ===========================================================================

class TestPlanParser:

    def test_parse_empty_plan(self):
        from db.plan_parser import parse_explain
        result = parse_explain({"dialect": "postgres", "plan": {}})
        assert result.summary == "No plan data available."

    def test_parse_sqlite_seq_scan(self):
        from db.plan_parser import parse_explain
        raw = {
            "dialect": "sqlite",
            "plan": [
                {"id": 2, "parent": 0, "notused": 0,
                 "detail": "SCAN orders"}
            ],
        }
        result = parse_explain(raw)
        assert result.has_seq_scan is True
        assert result.nodes[0].severity == "slow"

    def test_parse_sqlite_seq_scan_legacy_format(self):
        """Older SQLite versions emit 'SCAN TABLE orders' — also handled."""
        from db.plan_parser import parse_explain
        raw = {
            "dialect": "sqlite",
            "plan": [
                {"id": 2, "parent": 0, "notused": 0,
                 "detail": "SCAN TABLE orders"}
            ],
        }
        result = parse_explain(raw)
        assert result.has_seq_scan is True
        assert result.nodes[0].severity == "slow"

    def test_parse_sqlite_index_scan(self):
        from db.plan_parser import parse_explain
        raw = {
            "dialect": "sqlite",
            "plan": [
                {"id": 2, "parent": 0, "notused": 0,
                 "detail": "SEARCH products USING INDEX idx_products_category"}
            ],
        }
        result = parse_explain(raw)
        assert result.has_seq_scan is False
        assert result.nodes[0].severity == "ok"

    def test_parse_sqlite_sort_node(self):
        from db.plan_parser import parse_explain
        raw = {
            "dialect": "sqlite",
            "plan": [
                {"id": 2, "parent": 0, "notused": 0,
                 "detail": "USE TEMP B-TREE FOR ORDER BY"}
            ],
        }
        result = parse_explain(raw)
        assert result.nodes[0].severity == "warn"

    def test_plan_to_dict_serializable(self):
        import json
        from db.plan_parser import parse_explain, plan_to_dict
        raw = {
            "dialect": "sqlite",
            "plan": [
                {"id": 2, "parent": 0, "notused": 0,
                 "detail": "SCAN orders"}
            ],
        }
        plan = parse_explain(raw)
        d = plan_to_dict(plan)
        serialized = json.dumps(d)
        assert serialized

    def test_postgres_seq_scan_scored_slow(self):
        from db.plan_parser import _annotate_postgres_node, SEVERITY_SLOW
        node = {
            "Node Type": "Seq Scan",
            "Total Cost": 9800.0,
            "Plan Rows": 50000,
            "Relation Name": "orders",
        }
        annotated = _annotate_postgres_node(node)
        assert annotated.severity == SEVERITY_SLOW
        assert "orders" in annotated.reason

    def test_postgres_index_scan_scored_ok(self):
        from db.plan_parser import _annotate_postgres_node, SEVERITY_OK
        node = {
            "Node Type": "Index Scan",
            "Total Cost": 8.5,
            "Plan Rows": 1,
            "Index Name": "idx_customers_email",
            "Relation Name": "customers",
        }
        annotated = _annotate_postgres_node(node)
        assert annotated.severity == SEVERITY_OK

    def test_bad_estimate_flagged(self):
        from db.plan_parser import _annotate_postgres_node, SEVERITY_WARN
        node = {
            "Node Type": "Index Scan",
            "Total Cost": 5.0,
            "Plan Rows": 1,
            "Actual Rows": 50000,
            "Relation Name": "orders",
        }
        annotated = _annotate_postgres_node(node)
        assert annotated.estimate_off is True
        assert annotated.severity == SEVERITY_WARN

    def test_children_parsed_recursively(self):
        from db.plan_parser import _annotate_postgres_node
        node = {
            "Node Type": "Hash Join",
            "Total Cost": 500.0,
            "Plan Rows": 1000,
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Total Cost": 300.0,
                    "Plan Rows": 5000,
                    "Relation Name": "orders",
                },
                {
                    "Node Type": "Hash",
                    "Total Cost": 100.0,
                    "Plan Rows": 100,
                },
            ],
        }
        annotated = _annotate_postgres_node(node)
        assert len(annotated.children) == 2
        assert annotated.children[0].node_type == "Seq Scan"
        assert annotated.children[1].node_type == "Hash"

    def test_sqlite_table_name_extracted_modern(self):
        """Table name extracted from modern 'SCAN orders' format."""
        from db.plan_parser import _extract_sqlite_table
        assert _extract_sqlite_table("SCAN orders") == "orders"

    def test_sqlite_table_name_extracted_legacy(self):
        """Table name extracted from legacy 'SCAN TABLE orders' format."""
        from db.plan_parser import _extract_sqlite_table
        assert _extract_sqlite_table("SCAN TABLE orders") == "orders"

    def test_sqlite_index_name_extracted(self):
        from db.plan_parser import _extract_sqlite_index
        detail = "SEARCH products USING INDEX idx_products_category"
        assert _extract_sqlite_index(detail) == "idx_products_category"
