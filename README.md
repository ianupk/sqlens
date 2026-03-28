# SQL Optimizer MCP Server

An MCP (Model Context Protocol) server that gives any MCP-compatible
AI assistant live, read-only access to your database for query
explanation, performance analysis, and index optimization.

Works with any LLM that supports MCP — VS Code with GitHub Copilot,
Cursor, Claude Desktop, or any other MCP-compatible client.

---

## What it does

Ask your AI assistant questions like:

- *"Why is this query slow?"*
  → The server runs EXPLAIN, identifies sequential scans, and returns
  an annotated plan with severity scores. The assistant explains the
  bottleneck in plain English.

- *"What indexes should I add?"*
  → The server walks the query's AST using sqlglot, extracts columns
  used in WHERE, JOIN, and ORDER BY, cross-references existing indexes,
  and returns copy-paste ready `CREATE INDEX CONCURRENTLY` statements.

- *"Rewrite this for keyset pagination"*
  → The server fetches the EXPLAIN plan and schema context, then
  provides the assistant with structured guidance to rewrite the
  query and show the plan improvement.

- *"Is this table healthy?"*
  → The server reads live stats (vacuum age, dead tuples, cache hit
  ratio) and returns actionable health flags.

---

## Architecture

```
MCP client (VS Code / Cursor / Claude Desktop / any MCP host)
      ↓  stdio
FastMCP server
      ↓
┌──────────────────────────────────────────────┐
│  tools/            Business logic            │
│    query.py        run_query, explain_query  │
│    schema.py       list_tables, get_schema   │
│                    get_table_stats,          │
│                    get_slow_queries          │
│    optimizer.py    suggest_indexes,          │
│                    rewrite_query             │
├──────────────────────────────────────────────┤
│  middleware/       Cross-cutting concerns    │
│    safety.py       sqlglot AST sanitizer     │
│    audit.py        JSONL audit log           │
├──────────────────────────────────────────────┤
│  db/               Database drivers          │
│    postgres.py     psycopg3 + pg_catalog     │
│    sqlite.py       stdlib sqlite3            │
│    mysql.py        mysql-connector           │
└──────────────────────────────────────────────┘
```

---

## Tools

| Tool | What it does |
|---|---|
| `list_tables` | All tables with row estimates and sizes |
| `get_schema` | Columns, indexes, foreign keys for a table |
| `run_query` | Execute a read-only SELECT query |
| `explain_query` | Annotated EXPLAIN plan with severity scores |
| `suggest_indexes` | AST-based index suggestions with DDL |
| `get_table_stats` | Vacuum age, dead tuples, cache hit ratio |
| `get_slow_queries` | Slowest queries from pg_stat_statements |
| `rewrite_query` | Structured rewrite with plan comparison |

---

## Quickstart

**Prerequisites:** Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/ianupk/sql-optimizer-mcp
cd sql-optimizer-mcp
uv sync --dev

# Seed a demo SQLite database and verify everything works
uv run python scripts/seed_demo_db.py
uv run python scripts/run_demo.py
```

---

## Connect your MCP client

The server uses **stdio** — your client launches it as a subprocess. No ports, no network config.

**VS Code (Copilot):** Already configured — open the project and run Command Palette → `MCP: List Servers`.

**Cursor / Claude Desktop / others:** Add the following server config (update paths):

```json
{
  "mcpServers": {
    "sql-optimizer": {
      "command": "uv",
      "args": ["run", "python", "-m", "mcp_server.server"],
      "cwd": "/path/to/sql-optimizer-mcp",
      "env": {
        "DB_TYPE": "sqlite",
        "SQLITE_PATH": "/path/to/sql-optimizer-mcp/demo.db"
      }
    }
  }
}
```

> **Where to put this:**
>
> - **Cursor:** Settings → MCP → Add new server
> - **Claude Desktop (macOS):** `~/Library/Application Support/Claude/claude_desktop_config.json`
> - **Claude Desktop (Windows):** `%APPDATA%\Claude\claude_desktop_config.json`

---

## Database configuration

Set via `env` in your client config or in a `.env` file.

| Database | Variables |
|---|---|
| SQLite | `DB_TYPE=sqlite` `SQLITE_PATH=./demo.db` |
| PostgreSQL | `DB_TYPE=postgres` `DATABASE_URL=postgresql://user:pass@host:5432/db` |
| MySQL | `DB_TYPE=mysql` `MYSQL_HOST=…` `MYSQL_USER=…` `MYSQL_PASSWORD=…` `MYSQL_DB=…` |

---

## PostgreSQL read-only setup

Always connect as a read-only role. This is the most important
security step — it enforces read-only access at the database level,
independent of any application-level controls.

```sql
CREATE USER readonly_user WITH PASSWORD 'yourpassword';

GRANT CONNECT ON DATABASE yourdb TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO readonly_user;

-- For get_slow_queries() support:
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

---

## Demo walkthrough

**1. Find why a query is slow**

```
Why is this query slow?
SELECT * FROM orders o WHERE o.customer_id = 42
```

The server detects a full table scan on 100k rows and suggests:

```sql
CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders (customer_id);
```

**2. Optimize a JOIN**

```
Optimize this query:
SELECT c.first_name, COUNT(o.id)
FROM customers c
JOIN orders o ON o.customer_id = c.id
WHERE c.country = 'IN'
GROUP BY c.id
```

The server checks schemas for both tables, identifies the unindexed
foreign key, and suggests indexes for both the join column and the
filter column.

**3. Rewrite for pagination**

```
Rewrite this to use keyset pagination:
SELECT * FROM orders ORDER BY id LIMIT 20 OFFSET 10000
```

The server provides context and guidance. The assistant rewrites to
`WHERE id > :last_seen_id ORDER BY id LIMIT 20` and explains why
OFFSET degrades at scale.

**4. Check table health**

```
Is the orders table in good health?
```

The server returns live statistics and flags issues like stale
statistics, excessive dead tuples, or low cache hit ratio with
specific maintenance commands to run.

---

## Security model

### Write protection

**Layer 1 — SQL sanitizer (built in, always active):**
`middleware/safety.py` parses every SQL string with sqlglot and
rejects anything that is not a pure SELECT. INSERT, UPDATE, DELETE,
DROP, TRUNCATE, COPY, and dangerous functions like `pg_read_file`
are blocked before the database is ever touched.

**Layer 2 — Database permissions (your responsibility):**
The sanitizer protects against writes that come through this server.
It does not protect against anything that connects to your database
through a different path — another client, a shell tool, or an agent
that decides to go around MCP entirely.

The only reliable protection against that is enforcing read-only
access at the database level, independent of this server:

- **PostgreSQL / MySQL:** connect as a role that has only SELECT
  privilege. Even if someone connects directly with `psql` or a
  GUI client, the database rejects any write at the engine level.
  See the PostgreSQL read-only setup section above.

- **SQLite:** set the file to read-only after seeding:
  `chmod 444 yourdb.db`. The OS blocks writes before SQLite
  processes them, regardless of how the connection was made.

This is not optional if you are pointing this server at a database
you care about. Application-level controls are a useful first
filter — they give you clear error messages and audit log entries.
But they are not a security boundary. Database-level permissions
are the actual hard stop.

### Audit log

Every tool call is recorded to `audit.log` in JSONL format:

```json
{
  "ts": "2024-11-15T10:23:41.123Z",
  "tool": "explain_query",
  "inputs": {"sql": "SELECT * FROM orders WHERE customer_id = 42"},
  "reason": "User asked why this query is slow",
  "status": "ok",
  "duration_ms": 4.2,
  "result": "{\"has_seq_scan\": true, \"summary\": \"Full table scan...\"}"
}
```

---

## Running tests

```bash
uv run pytest -v                       # full suite
uv run pytest tests/test_safety.py    # safety layer
uv run pytest tests/test_optimizer.py # index suggestions
uv run pytest tests/test_drivers.py   # database drivers
```

---

## Project structure

```
sql-optimizer-mcp/
├── db/                   Database drivers (Postgres, SQLite, MySQL)
│   ├── base.py           DBDriver abstract interface + dataclasses
│   ├── plan_parser.py    EXPLAIN JSON parser with severity scoring
│   ├── postgres.py       psycopg3 driver
│   ├── sqlite.py         sqlite3 driver
│   └── mysql.py          mysql-connector driver
├── middleware/
│   ├── safety.py         sqlglot AST sanitizer (6 defence layers)
│   └── audit.py          @audit_tool decorator, JSONL logging
├── tools/
│   ├── query.py          run_query, explain_query
│   ├── schema.py         list_tables, get_schema, stats tools
│   └── optimizer.py      suggest_indexes, rewrite_query
├── mcp_server/
│   └── server.py         FastMCP entry point, system prompt
├── scripts/
│   ├── seed_demo_db.py   Creates demo.db with realistic data
│   └── run_demo.py       Exercises every tool end-to-end
└── tests/                pytest suite (~100 tests)
```

---

## Extending

**Add a new tool:**

1. Write the function in `tools/` and decorate with `@audit_tool`
2. Register it in `mcp_server/server.py`
3. Add tests in `tests/`

**Add a new database driver:**

1. Implement the `DBDriver` ABC in `db/`
2. Add a case to `db/factory.py`
3. Handle dialect differences in `db/plan_parser.py`

**Add the HTTP layer (FastAPI):**
The tool functions in `tools/` are plain Python functions — any
FastAPI route can call them directly with the same driver instance.
No refactoring required.

---

## Tech stack

| Component | Technology |
|---|---|
| MCP framework | FastMCP |
| Postgres driver | psycopg3 |
| SQL parser / sanitizer | sqlglot |
| Package manager | uv |
| Testing | pytest |
