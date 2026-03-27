"""
Audit middleware tests.
"""
import json
import pytest
from pathlib import Path
from middleware.audit import audit_tool, read_audit_log, AUDIT_LOG_PATH


@pytest.fixture(autouse=True)
def clean_audit_log(tmp_path, monkeypatch):
    """
    Redirect audit log to a temp file for every test.
    Prevents test runs from polluting your real audit.log.
    """
    test_log = tmp_path / "test_audit.log"
    monkeypatch.setattr(
        "middleware.audit.AUDIT_LOG_PATH",
        test_log
    )
    yield test_log


# ------------------------------------------------------------------
# Basic behavior
# ------------------------------------------------------------------

def test_decorated_function_returns_normally():
    """audit_tool should not change the return value."""
    @audit_tool
    def my_tool(sql: str, reason: str = "") -> dict:
        return {"rows": []}

    result = my_tool(sql="SELECT 1", reason="testing")
    assert result == {"rows": []}


def test_audit_log_entry_written(clean_audit_log):
    @audit_tool
    def my_tool(sql: str, reason: str = "") -> dict:
        return {"rows": []}

    my_tool(sql="SELECT 1", reason="test reason")

    assert clean_audit_log.exists()
    lines = clean_audit_log.read_text().strip().splitlines()
    assert len(lines) == 1


def test_audit_entry_fields(clean_audit_log):
    @audit_tool
    def run_query(sql: str, limit: int = 100, reason: str = "") -> dict:
        return {"row_count": 5}

    run_query(sql="SELECT * FROM orders", limit=10, reason="checking slow query")

    entry = json.loads(clean_audit_log.read_text().strip())

    assert entry["tool"] == "run_query"
    assert entry["status"] == "ok"
    assert entry["reason"] == "checking slow query"
    assert "ts" in entry
    assert "duration_ms" in entry
    assert entry["inputs"]["sql"] == "SELECT * FROM orders"
    assert entry["inputs"]["limit"] == "10"


def test_audit_captures_error(clean_audit_log):
    @audit_tool
    def bad_tool(sql: str, reason: str = "") -> dict:
        raise ValueError("something went wrong")

    with pytest.raises(ValueError):
        bad_tool(sql="SELECT 1", reason="")

    entry = json.loads(clean_audit_log.read_text().strip())

    assert entry["status"] == "error"
    assert "ValueError" in entry["error"]
    assert "something went wrong" in entry["error"]


def test_audit_reraises_exception():
    """The original exception must propagate — not swallowed."""
    @audit_tool
    def failing_tool(sql: str, reason: str = "") -> dict:
        raise RuntimeError("DB connection failed")

    with pytest.raises(RuntimeError, match="DB connection failed"):
        failing_tool(sql="SELECT 1", reason="")


def test_audit_multiple_calls(clean_audit_log):
    @audit_tool
    def my_tool(sql: str, reason: str = "") -> dict:
        return {}

    my_tool(sql="SELECT 1", reason="first")
    my_tool(sql="SELECT 2", reason="second")
    my_tool(sql="SELECT 3", reason="third")

    lines = clean_audit_log.read_text().strip().splitlines()
    assert len(lines) == 3

    entries = [json.loads(line) for line in lines]
    assert entries[0]["inputs"]["sql"] == "SELECT 1"
    assert entries[2]["inputs"]["sql"] == "SELECT 3"


def test_audit_duration_is_positive(clean_audit_log):
    @audit_tool
    def my_tool(reason: str = "") -> dict:
        return {}

    my_tool(reason="timing test")

    entry = json.loads(clean_audit_log.read_text().strip())
    assert entry["duration_ms"] >= 0


def test_audit_long_sql_is_truncated(clean_audit_log):
    @audit_tool
    def my_tool(sql: str, reason: str = "") -> dict:
        return {}

    long_sql = "SELECT " + ", ".join([f"col_{i}" for i in range(500)])
    my_tool(sql=long_sql, reason="")

    entry = json.loads(clean_audit_log.read_text().strip())
    assert len(entry["inputs"]["sql"]) <= 501  # 500 chars + ellipsis


def test_audit_no_reason_defaults_to_empty(clean_audit_log):
    @audit_tool
    def my_tool(sql: str) -> dict:
        return {}

    my_tool(sql="SELECT 1")

    entry = json.loads(clean_audit_log.read_text().strip())
    assert entry["reason"] == ""


# ------------------------------------------------------------------
# read_audit_log
# ------------------------------------------------------------------

def test_read_audit_log_returns_list(clean_audit_log):
    @audit_tool
    def my_tool(reason: str = "") -> dict:
        return {}

    my_tool(reason="r1")
    my_tool(reason="r2")

    entries = read_audit_log()
    assert isinstance(entries, list)
    assert len(entries) == 2


def test_read_audit_log_empty_when_no_file():
    """If audit.log does not exist yet, return empty list."""
    # clean_audit_log fixture redirected the path but the file
    # was never written — so it does not exist
    entries = read_audit_log(limit=10)
    assert entries == []


def test_read_audit_log_respects_limit(clean_audit_log):
    @audit_tool
    def my_tool(reason: str = "") -> dict:
        return {}

    for i in range(20):
        my_tool(reason=f"call {i}")

    entries = read_audit_log(limit=5)
    assert len(entries) == 5
    # should be the LAST 5 entries
    assert entries[-1]["reason"] == "call 19"
