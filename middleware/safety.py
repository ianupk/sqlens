import re
import sqlglot
from sqlglot import exp


class UnsafeSQLError(Exception):
    pass


# ---------------------------------------------------------------------------
# Blocked AST node types (WRITE / DDL ONLY)
# ---------------------------------------------------------------------------

BLOCKED_EXPRESSION_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    exp.Command,
    exp.Transaction,
    exp.Merge,
    exp.Cache,
)

# ---------------------------------------------------------------------------
# Blocked function names
# ---------------------------------------------------------------------------

BLOCKED_FUNCTIONS = frozenset({
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_ls_waldir",
    "pg_ls_logdir",
    "pg_stat_file",
    "lo_export",
    "lo_import",
    "lo_from_bytea",
    "pg_execute_server_program",
    "load_file",
    "into_outfile",
    "exec",
    "execute",
    "system",
    "shell",
    "cmd",
})

# ---------------------------------------------------------------------------
# Raw keyword regex
# ---------------------------------------------------------------------------

BLOCKED_KEYWORDS_RAW = re.compile(
    r"""
    \b(
        insert\s+into    |
        update\s+\w      |
        delete\s+from    |
        drop\s+table     |
        drop\s+database  |
        drop\s+schema    |
        drop\s+index     |
        truncate\s+table |
        create\s+table   |
        create\s+index   |
        create\s+schema  |
        alter\s+table    |
        copy\s+\w        |
        into\s+outfile   |
        load\s+data      |
        grant\s+\w       |
        revoke\s+\w      |
        set\s+role       |
        set\s+session    |
        pg_sleep         |
        sleep\s*\(
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


# ---------------------------------------------------------------------------
# MAIN ENTRY
# ---------------------------------------------------------------------------

def sanitize(sql: str, dialect: str = "postgres") -> str:
    if not sql or not sql.strip():
        raise UnsafeSQLError("Empty SQL string provided.")

    _check_raw_keywords(sql)

    try:
        statements = sqlglot.parse(sql, dialect=dialect, error_level=None)
    except Exception as e:
        raise UnsafeSQLError(f"SQL could not be parsed: {e}") from e

    valid_statements = [s for s in statements if s is not None]

    # ✅ FIX: handle comment-only SQL
    if not valid_statements:
        raise UnsafeSQLError("No valid SQL statement found.")

    if len(valid_statements) > 1:
        raise UnsafeSQLError(
            f"Multiple statements are not allowed. "
            f"Got {len(valid_statements)} statements."
        )

    statement = valid_statements[0]

    # ------------------------------------------------------------------
    # Allow SELECT or WITH (CTE)
    # ------------------------------------------------------------------
    if not isinstance(statement, (exp.Select, exp.With)):
        raise UnsafeSQLError(
            f"Only SELECT statements are allowed. "
            f"Detected: {type(statement).__name__}"
        )

    # ------------------------------------------------------------------
    # Block SELECT INTO (Postgres write)
    # ------------------------------------------------------------------
    if isinstance(statement, exp.Select) and statement.args.get("into"):
        raise UnsafeSQLError("SELECT INTO is not allowed.")

    # ------------------------------------------------------------------
    # AST checks
    # ------------------------------------------------------------------
    _check_ast_for_blocked_nodes(statement)
    _check_ast_for_blocked_functions(statement)

    return statement.sql(dialect=dialect)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _check_raw_keywords(sql: str) -> None:
    match = BLOCKED_KEYWORDS_RAW.search(sql)
    if match:
        raise UnsafeSQLError(
            f"Blocked keyword detected: '{match.group().strip()}'."
        )


def _check_ast_for_blocked_nodes(statement: exp.Expression) -> None:
    for node in statement.walk():
        if isinstance(node, BLOCKED_EXPRESSION_TYPES):
            raise UnsafeSQLError(
                f"Blocked expression '{type(node).__name__}' detected."
            )


def _check_ast_for_blocked_functions(statement: exp.Expression) -> None:
    for node in statement.walk():
        name = None

        if isinstance(node, exp.Anonymous):
            name = node.name

        elif isinstance(node, exp.Func):
            name = node.sql_name() if hasattr(node, "sql_name") else None

        if name and name.lower() in BLOCKED_FUNCTIONS:
            raise UnsafeSQLError(
                f"Blocked function '{name}' is not permitted."
            )
