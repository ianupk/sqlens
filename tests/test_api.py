"""
API route tests using FastAPI's TestClient.

No real server is needed — TestClient makes in-process HTTP calls.
All tests use the SQLite test DB from conftest.py via monkeypatch.
"""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(sqlite_db_path, monkeypatch):
    """
    TestClient with env vars pointing at the SQLite test DB.
    Clears lru_cache between tests so each test gets a fresh driver.
    """
    import os
    monkeypatch.setenv("DB_TYPE", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(sqlite_db_path))

    # Clear cached driver and tools so monkeypatched env takes effect
    from api.dependencies import get_driver, get_tools
    get_driver.cache_clear()
    get_tools.cache_clear()

    from api.main import app
    with TestClient(app) as c:
        yield c

    # Clean up cache after test
    from api.dependencies import get_driver, get_tools
    get_driver.cache_clear()
    get_tools.cache_clear()


# ===========================================================================
# Health and root
# ===========================================================================

class TestSystem:

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_root(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "docs" in r.json()

    def test_docs_available(self, client):
        r = client.get("/docs")
        assert r.status_code == 200


# ===========================================================================
# /query/run
# ===========================================================================

class TestRunQuery:

    def test_basic_select(self, client):
        r = client.post("/query/run", json={
            "sql": "SELECT id, name FROM customers LIMIT 5",
            "reason": "test",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == 5
        assert "id" in body["columns"]
        assert body["blocked"] is False

    def test_limit_respected(self, client):
        r = client.post("/query/run", json={
            "sql": "SELECT * FROM customers",
            "limit": 3,
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["row_count"] <= 3

    def test_truncated_flag(self, client):
        r = client.post("/query/run", json={
            "sql": "SELECT * FROM customers",
            "limit": 10,
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["truncated"] is True

    def test_blocked_write(self, client):
        r = client.post("/query/run", json={
            "sql": "DELETE FROM customers WHERE id = 1",
            "reason": "test",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["blocked"] is True
        assert body["row_count"] == 0

    def test_invalid_table(self, client):
        r = client.post("/query/run", json={
            "sql": "SELECT * FROM ghost_table_xyz",
            "reason": "test",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["error"] is not None

    def test_limit_validation(self, client):
        """limit > 1000 should be rejected by Pydantic validation."""
        r = client.post("/query/run", json={
            "sql": "SELECT 1",
            "limit": 9999,
            "reason": "test",
        })
        assert r.status_code == 422

    def test_missing_sql_field(self, client):
        r = client.post("/query/run", json={"reason": "test"})
        assert r.status_code == 422


# ===========================================================================
# /query/explain
# ===========================================================================

class TestExplainQuery:

    def test_basic_explain(self, client):
        r = client.post("/query/explain", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 42",
            "reason": "test",
        })
        assert r.status_code == 200
        body = r.json()
        assert "dialect" in body
        assert "summary" in body
        assert "has_seq_scan" in body
        assert "plan_tree" in body

    def test_has_seq_scan_detected(self, client):
        """orders.customer_id has no index — must detect seq scan."""
        r = client.post("/query/explain", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 42",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["has_seq_scan"] is True

    def test_index_scan_detected(self, client):
        """products.category has an index — no seq scan expected."""
        r = client.post("/query/explain", json={
            "sql": "SELECT * FROM products p WHERE p.category = 'Books'",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["has_seq_scan"] is False

    def test_blocked_write(self, client):
        r = client.post("/query/explain", json={
            "sql": "DROP TABLE orders",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["blocked"] is True

    def test_analyze_flag_accepted(self, client):
        r = client.post("/query/explain", json={
            "sql": "SELECT * FROM customers LIMIT 5",
            "analyze": True,
            "reason": "test",
        })
        assert r.status_code == 200


# ===========================================================================
# /schema/tables
# ===========================================================================

class TestListTables:

    def test_returns_all_tables(self, client):
        r = client.get("/schema/tables")
        assert r.status_code == 200
        body = r.json()
        names = [t["name"] for t in body["tables"]]
        assert "customers" in names
        assert "orders" in names
        assert "products" in names

    def test_table_count(self, client):
        r = client.get("/schema/tables")
        assert r.status_code == 200
        assert r.json()["table_count"] == 4

    def test_schema_query_param(self, client):
        r = client.get("/schema/tables?schema=public&reason=test")
        assert r.status_code == 200

    def test_table_has_row_estimate(self, client):
        r = client.get("/schema/tables")
        assert r.status_code == 200
        for t in r.json()["tables"]:
            assert isinstance(t["row_estimate"], int)


# ===========================================================================
# /schema/table/{table}
# ===========================================================================

class TestGetSchema:

    def test_returns_schema(self, client):
        r = client.get("/schema/table/customers")
        assert r.status_code == 200
        body = r.json()
        assert body["table"] == "customers"
        assert body["column_count"] == 5

    def test_columns_present(self, client):
        r = client.get("/schema/table/orders")
        assert r.status_code == 200
        names = [c["name"] for c in r.json()["columns"]]
        assert "customer_id" in names
        assert "status" in names

    def test_pk_detected(self, client):
        r = client.get("/schema/table/customers")
        assert r.status_code == 200
        assert "id" in r.json()["pk_columns"]

    def test_index_detected(self, client):
        r = client.get("/schema/table/products")
        assert r.status_code == 200
        index_names = [i["name"] for i in r.json()["indexes"]]
        assert "idx_products_category" in index_names

    def test_unindexed_columns_detected(self, client):
        r = client.get("/schema/table/orders")
        assert r.status_code == 200
        assert "customer_id" in r.json()["unindexed_columns"]

    def test_foreign_key_detected(self, client):
        r = client.get("/schema/table/orders")
        assert r.status_code == 200
        fk_cols = [fk["column"] for fk in r.json()["foreign_keys"]]
        assert "customer_id" in fk_cols

    def test_nonexistent_table(self, client):
        r = client.get("/schema/table/ghost_xyz")
        assert r.status_code == 200
        assert r.json()["error"] is not None

    def test_reason_query_param(self, client):
        r = client.get("/schema/table/orders?reason=test")
        assert r.status_code == 200


# ===========================================================================
# /schema/stats/{table}
# ===========================================================================

class TestTableStats:

    def test_returns_stats(self, client):
        r = client.get("/schema/stats/orders")
        assert r.status_code == 200
        body = r.json()
        assert body["table"] == "orders"
        assert body["live_row_count"] is not None

    def test_health_flags_present(self, client):
        r = client.get("/schema/stats/orders")
        assert r.status_code == 200
        assert isinstance(r.json()["health_flags"], list)

    def test_nonexistent_table(self, client):
        r = client.get("/schema/stats/ghost_xyz")
        assert r.status_code == 200
        assert r.json()["error"] is not None


# ===========================================================================
# /schema/slow-queries
# ===========================================================================

class TestSlowQueries:

    def test_returns_empty_for_sqlite(self, client):
        r = client.get("/schema/slow-queries")
        assert r.status_code == 200
        body = r.json()
        assert body["query_count"] == 0
        assert isinstance(body["message"], str)

    def test_min_ms_param(self, client):
        r = client.get("/schema/slow-queries?min_ms=500")
        assert r.status_code == 200
        assert r.json()["min_ms"] == 500

    def test_negative_min_ms_rejected(self, client):
        r = client.get("/schema/slow-queries?min_ms=-1")
        assert r.status_code == 422


# ===========================================================================
# /optimizer/indexes
# ===========================================================================

class TestSuggestIndexes:

    def test_suggests_index(self, client):
        r = client.post("/optimizer/indexes", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 42",
            "reason": "test",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["suggestion_count"] > 0
        assert body["suggestions"][0]["ddl"].startswith(
            "CREATE INDEX CONCURRENTLY"
        )

    def test_no_suggestion_for_indexed_column(self, client):
        r = client.post("/optimizer/indexes", json={
            "sql": "SELECT * FROM products p WHERE p.category = 'Books'",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["suggestion_count"] == 0

    def test_blocked_write(self, client):
        r = client.post("/optimizer/indexes", json={
            "sql": "DROP TABLE orders",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["error"] is not None

    def test_suggestion_has_required_fields(self, client):
        r = client.post("/optimizer/indexes", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 42",
            "reason": "test",
        })
        assert r.status_code == 200
        s = r.json()["suggestions"][0]
        for field in ["table", "columns", "ddl", "reason", "usage", "impact"]:
            assert field in s


# ===========================================================================
# /optimizer/rewrite
# ===========================================================================

class TestRewriteQuery:

    def test_returns_context(self, client):
        r = client.post("/optimizer/rewrite", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 1",
            "goal": "make this faster",
            "reason": "test",
        })
        assert r.status_code == 200
        body = r.json()
        assert "original_sql" in body
        assert "explain_plan" in body
        assert "schema_context" in body
        assert "rewrite_guidance" in body
        assert "instructions" in body

    def test_goal_preserved(self, client):
        r = client.post("/optimizer/rewrite", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 1",
            "goal": "add keyset pagination",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["goal"] == "add keyset pagination"

    def test_missing_goal_rejected(self, client):
        r = client.post("/optimizer/rewrite", json={
            "sql": "SELECT * FROM orders o WHERE o.customer_id = 1",
            "reason": "test",
        })
        assert r.status_code == 422

    def test_blocked_write(self, client):
        r = client.post("/optimizer/rewrite", json={
            "sql": "DROP TABLE orders",
            "goal": "make it faster",
            "reason": "test",
        })
        assert r.status_code == 200
        assert r.json()["error"] is not None


# ===========================================================================
# /audit/logs
# ===========================================================================

class TestAuditLogs:

    def test_returns_list(self, client):
        # Make a tool call first so there is something in the log
        client.post("/query/run", json={
            "sql": "SELECT 1 AS n",
            "reason": "generating audit entry",
        })
        r = client.get("/audit/logs")
        assert r.status_code == 200
        body = r.json()
        assert "entries" in body
        assert "entry_count" in body
        assert isinstance(body["entries"], list)

    def test_limit_param(self, client):
        r = client.get("/audit/logs?limit=5")
        assert r.status_code == 200
        assert r.json()["limit"] == 5

    def test_limit_too_large_rejected(self, client):
        r = client.get("/audit/logs?limit=9999")
        assert r.status_code == 422


# ===========================================================================
# CORS headers
# ===========================================================================

class TestCORS:

    def test_cors_header_present(self, client):
        r = client.options(
            "/query/run",
            headers={"Origin": "http://localhost:3000"},
        )
        assert "access-control-allow-origin" in r.headers

    def test_allowed_origin(self, client):
        r = client.get(
            "/health",
            headers={"Origin": "http://localhost:3000"},
        )
        assert r.status_code == 200
