"""
Safety layer tests.

Organized into four sections:
  1. Statements that MUST pass (valid SELECTs)
  2. Statements that MUST be blocked (writes, DDL)
  3. Evasion attempts (obfuscated SQL trying to bypass the sanitizer)
  4. Edge cases (empty input, multiple statements, nested queries)

The goal is that if any test in sections 2-4 passes without raising
UnsafeSQLError, you have a real security hole.
"""
import pytest
from middleware.safety import sanitize, UnsafeSQLError, _check_raw_keywords


# ===========================================================================
# Section 1 — Valid SELECT statements that must pass
# ===========================================================================

class TestValidSelects:

    def test_simple_select(self):
        result = sanitize("SELECT id, name FROM customers")
        assert "customers" in result

    def test_select_with_where(self):
        result = sanitize("SELECT * FROM orders WHERE status = 'completed'")
        assert result  # any non-empty string is fine

    def test_select_with_join(self):
        sql = """
            SELECT c.name, COUNT(o.id) as total
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            GROUP BY c.id
        """
        result = sanitize(sql)
        assert result

    def test_select_with_subquery(self):
        sql = """
            SELECT * FROM orders
            WHERE customer_id IN (
                SELECT id FROM customers WHERE country = 'IN'
            )
        """
        result = sanitize(sql)
        assert result

    def test_select_with_cte(self):
        sql = """
            WITH ranked AS (
                SELECT id, name,
                       ROW_NUMBER() OVER (ORDER BY id) AS rn
                FROM customers
            )
            SELECT * FROM ranked WHERE rn <= 10
        """
        result = sanitize(sql)
        assert result

    def test_select_with_window_function(self):
        sql = """
            SELECT
                name,
                SUM(total_cents) OVER (PARTITION BY country) as country_total
            FROM customers
            JOIN orders ON orders.customer_id = customers.id
        """
        result = sanitize(sql)
        assert result

    def test_select_aggregate(self):
        result = sanitize(
            "SELECT COUNT(*), AVG(total_cents) FROM orders"
        )
        assert result

    def test_select_with_limit_and_offset(self):
        result = sanitize(
            "SELECT * FROM products LIMIT 20 OFFSET 40"
        )
        assert result

    def test_select_returns_normalized_sql(self):
        """sanitize() returns clean SQL string, not the original."""
        raw = "   SELECT   id,name   FROM   customers   "
        result = sanitize(raw)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_select_with_sqlite_dialect(self):
        result = sanitize(
            "SELECT * FROM customers LIMIT 10",
            dialect="sqlite"
        )
        assert result

    def test_explain_prefix_is_separate(self):
        """EXPLAIN is added by the driver, not passed through sanitize."""
        result = sanitize("SELECT id FROM orders WHERE id = 1")
        assert result  # plain SELECT passes


# ===========================================================================
# Section 2 — Write and DDL statements that must be blocked
# ===========================================================================

class TestBlockedWriteStatements:

    def test_blocks_insert(self):
        with pytest.raises(UnsafeSQLError, match="INSERT"):
            sanitize("INSERT INTO customers (name) VALUES ('evil')")

    def test_blocks_update(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("UPDATE customers SET name = 'hacked' WHERE id = 1")

    def test_blocks_delete(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("DELETE FROM customers WHERE id = 1")

    def test_blocks_drop_table(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("DROP TABLE customers")

    def test_blocks_drop_database(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("DROP DATABASE devdb")

    def test_blocks_truncate(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("TRUNCATE TABLE customers")

    def test_blocks_create_table(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("CREATE TABLE evil (id INT)")

    def test_blocks_create_index(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("CREATE INDEX idx_evil ON customers(name)")

    def test_blocks_alter_table(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("ALTER TABLE customers ADD COLUMN evil TEXT")

    def test_blocks_merge(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("""
                MERGE INTO customers AS target
                USING (SELECT 1 AS id) AS source ON target.id = source.id
                WHEN MATCHED THEN UPDATE SET name = 'evil'
            """)


class TestBlockedFunctions:

    def test_blocks_pg_read_file(self):
        with pytest.raises(UnsafeSQLError, match="pg_read_file"):
            sanitize("SELECT pg_read_file('/etc/passwd')")

    def test_blocks_pg_ls_dir(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT pg_ls_dir('/var/lib/postgresql')")

    def test_blocks_lo_export(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT lo_export(12345, '/tmp/dump.sql')")

    def test_blocks_pg_read_binary_file(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT pg_read_binary_file('/etc/shadow')")

    def test_blocks_load_file_mysql(self):
        with pytest.raises(UnsafeSQLError, match="load_file"):
            sanitize("SELECT load_file('/etc/passwd')", dialect="mysql")


# ===========================================================================
# Section 3 — Evasion attempts
# These are the tests that matter most. Real attackers use these patterns.
# ===========================================================================

class TestEvasionAttempts:

    def test_semicolon_injection(self):
        """Classic injection: valid SELECT followed by DROP."""
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT * FROM customers; DROP TABLE customers")

    def test_semicolon_injection_insert(self):
        with pytest.raises(UnsafeSQLError):
            sanitize(
                "SELECT * FROM orders; "
                "INSERT INTO customers(name) VALUES('evil')"
            )

    def test_comment_hiding_drop(self):
        """SQL comment used to hide the real statement."""
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT 1; -- nothing to see\nDROP TABLE customers")

    def test_mixed_case_evasion_insert(self):
        """iNsErT should still be caught by raw keyword check."""
        with pytest.raises(UnsafeSQLError):
            sanitize("iNsErT iNtO customers (name) VALUES ('x')")

    def test_mixed_case_evasion_drop(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("DrOp TaBlE customers")

    def test_whitespace_evasion(self):
        """Extra whitespace between keywords."""
        with pytest.raises(UnsafeSQLError):
            sanitize("DROP   TABLE   customers")

    def test_newline_evasion(self):
        """Newlines between keywords."""
        with pytest.raises(UnsafeSQLError):
            sanitize("DROP\nTABLE\ncustomers")

    def test_select_into_blocked(self):
        """SELECT INTO is a write operation in Postgres."""
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT * INTO backup_customers FROM customers")

    def test_copy_command_blocked(self):
        """COPY can write files from Postgres."""
        with pytest.raises(UnsafeSQLError):
            sanitize("COPY customers TO '/tmp/dump.csv'")

    def test_function_in_subquery(self):
        """Dangerous function hidden inside a subquery."""
        with pytest.raises(UnsafeSQLError):
            sanitize("""
                SELECT * FROM customers
                WHERE id IN (
                    SELECT id FROM customers
                    WHERE name = pg_read_file('/etc/passwd')
                )
            """)

    def test_function_in_cte(self):
        """Dangerous function hidden inside a CTE."""
        with pytest.raises(UnsafeSQLError):
            sanitize("""
                WITH evil AS (
                    SELECT pg_ls_dir('/') AS f
                )
                SELECT * FROM evil
            """)

    def test_sleep_dos_attempt(self):
        """pg_sleep can be used for denial of service."""
        with pytest.raises(UnsafeSQLError):
            sanitize("SELECT pg_sleep(30)")

    def test_write_inside_cte(self):
        """
        CTE containing an INSERT — sqlglot catches this as a
        blocked expression type inside the AST walk.
        Note: not all databases support writable CTEs but we block anyway.
        """
        with pytest.raises(UnsafeSQLError):
            sanitize("""
                WITH ins AS (
                    INSERT INTO customers (name) VALUES ('evil')
                    RETURNING id
                )
                SELECT * FROM ins
            """)

    def test_grant_blocked(self):
        """GRANT is a privilege escalation vector."""
        with pytest.raises(UnsafeSQLError):
            sanitize("GRANT ALL ON customers TO evil_user")

    def test_set_role_blocked(self):
        """SET ROLE could escalate privileges."""
        with pytest.raises(UnsafeSQLError):
            sanitize("SET ROLE postgres")


# ===========================================================================
# Section 4 — Edge cases
# ===========================================================================

class TestEdgeCases:

    def test_empty_string_raises(self):
        with pytest.raises(UnsafeSQLError, match="Empty"):
            sanitize("")

    def test_whitespace_only_raises(self):
        with pytest.raises(UnsafeSQLError, match="Empty"):
            sanitize("   \n\t  ")

    def test_unparseable_sql_raises(self):
        with pytest.raises(UnsafeSQLError):
            sanitize("THIS IS NOT SQL AT ALL $$$$")

    def test_multiple_selects_blocked(self):
        """Two SELECTs separated by semicolon — both are reads but still blocked."""
        with pytest.raises(UnsafeSQLError, match="Multiple statements"):
            sanitize(
                "SELECT * FROM customers; SELECT * FROM orders"
            )

    def test_trailing_semicolon_ok(self):
        """A single SELECT with a trailing semicolon is fine."""
        result = sanitize("SELECT * FROM customers;")
        assert result

    def test_deeply_nested_select_ok(self):
        """Deeply nested but purely read — should pass."""
        sql = """
            SELECT *
            FROM (
                SELECT *
                FROM (
                    SELECT id, name
                    FROM customers
                    WHERE country = 'IN'
                ) AS inner1
                WHERE id > 10
            ) AS inner2
            LIMIT 5
        """
        result = sanitize(sql)
        assert result

    def test_unicode_in_string_literal_ok(self):
        """Unicode values in WHERE clauses should not break parsing."""
        result = sanitize(
            "SELECT * FROM customers WHERE name = 'राहुल'"
        )
        assert result

    def test_sql_with_only_comment_raises(self):
        """A statement that is only a comment produces no valid SQL."""
        with pytest.raises(UnsafeSQLError):
            sanitize("-- just a comment")

    def test_numeric_only_select_ok(self):
        """SELECT without FROM — valid SQL."""
        result = sanitize("SELECT 1 + 1 AS result")
        assert result

    def test_error_message_is_descriptive(self):
        """The exception message should tell you what was blocked."""
        with pytest.raises(UnsafeSQLError) as exc_info:
            sanitize("DROP TABLE customers")
        assert len(str(exc_info.value)) > 10  # not an empty message
