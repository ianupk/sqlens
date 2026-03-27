import json
import time
import traceback
import functools
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Any


# Audit log location — relative to wherever the server is run from.
# In production you would make this configurable via env var.
AUDIT_LOG_PATH = Path("audit.log")


def audit_tool(fn: Callable) -> Callable:
    """
    Decorator for MCP tool functions.

    Wraps any tool function and writes a structured JSONL entry to
    audit.log for every call — whether it succeeds or fails.

    Each log entry contains:
        ts          ISO 8601 timestamp (UTC)
        tool        name of the tool function called
        inputs      dict of all kwargs passed to the tool
                    (values truncated to 500 chars to keep log readable)
        reason      the `reason` kwarg if provided by LLM, else ""
        status      "ok" or "error"
        duration_ms how long the tool took in milliseconds
        result      first 300 chars of the result on success
        error       exception message on failure

    Usage
    -----
    Apply this decorator to any function that will be registered as an
    MCP tool. The function must accept **kwargs so the decorator can
    log all inputs uniformly.

        @audit_tool
        def run_query(sql: str, limit: int = 100, reason: str = "") -> dict:
            ...

    The `reason` parameter is a convention throughout this project —
    the MCP server's system prompt instructs the LLM to always provide
    a one-sentence justification when calling a tool. The audit log
    captures this reason so you can review the LLM's decision-making.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs) -> Any:
        start = time.perf_counter()
        status = "ok"
        result_preview = ""
        error_message = ""

        try:
            result = fn(*args, **kwargs)
            result_preview = _truncate(str(result), 300)
            return result

        except Exception as exc:
            status = "error"
            error_message = f"{type(exc).__name__}: {exc}"
            # Re-raise — the tool caller (FastMCP) handles the error response
            raise

        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 3)
            _write_entry(
                tool=fn.__name__,
                inputs=kwargs,
                reason=kwargs.get("reason", ""),
                status=status,
                duration_ms=duration_ms,
                result_preview=result_preview,
                error_message=error_message,
            )

    return wrapper


def _write_entry(
    tool: str,
    inputs: dict,
    reason: str,
    status: str,
    duration_ms: float,
    result_preview: str,
    error_message: str,
) -> None:
    """
    Write a single JSONL entry to audit.log.
    Creates the file if it does not exist.
    Uses append mode so entries accumulate across server restarts.
    """
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "tool": tool,
        "inputs": {k: _truncate(str(v), 500) for k, v in inputs.items()},
        "reason": reason,
        "status": status,
        "duration_ms": duration_ms,
    }

    if status == "ok":
        entry["result"] = result_preview
    else:
        entry["error"] = error_message

    try:
        with AUDIT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        # Never let audit logging failure crash the tool call.
        # If we can't write to the log, the tool result is more
        # important than the audit entry.
        pass


def _truncate(value: str, max_len: int) -> str:
    """Truncate a string to max_len chars, adding ellipsis if cut."""
    if len(value) <= max_len:
        return value
    return value[:max_len] + "…"


def read_audit_log(limit: int = 100) -> list[dict]:
    """
    Read the most recent `limit` entries from audit.log.
    Returns a list of dicts parsed from JSONL.
    Returns [] if the log file does not exist yet.

    This is used by the get_audit_log tool on Day 7.
    """
    if not AUDIT_LOG_PATH.exists():
        return []

    lines = AUDIT_LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
    recent = lines[-limit:]  # take last N lines

    entries = []
    for line in recent:
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # skip malformed lines

    return entries
