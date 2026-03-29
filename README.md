<p align="center">
  <img src="assets/banner.svg" alt="SQLens" width="800" />
</p>

<p align="center">
  <strong>An MCP server that gives AI assistants direct, safe access to your database for query analysis, optimization, and index recommendations.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-≥3.11-blue?logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/MCP-1.0-green?logo=data:image/svg+xml;base64,PHN2ZyBmaWxsPSJ3aGl0ZSIgZmlsbC1ydWxlPSJldmVub2RkIiBoZWlnaHQ9IjFlbSIgdmlld0JveD0iMCAwIDI0IDI0IiB3aWR0aD0iMWVtIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPjxwYXRoIGQ9Ik0xNS42ODggMi4zNDNhMi41ODggMi41ODggMCAwMC0zLjYxIDBsLTkuNjI2IDkuNDRhLjg2My44NjMgMCAwMS0xLjIwMyAwIC44MjMuODIzIDAgMDEwLTEuMThsOS42MjYtOS40NGE0LjMxMyA0LjMxMyAwIDAxNi4wMTYgMCA0LjExNiA0LjExNiAwIDAxMS4yMDQgMy41NCA0LjMgNC4zIDAgMDEzLjYwOSAxLjE4bC4wNS4wNWE0LjExNSA0LjExNSAwIDAxMCA1LjlsLTguNzA2IDguNTM3YS4yNzQuMjc0IDAgMDAwIC4zOTNsMS43ODggMS43NTRhLjgyMy44MjMgMCAwMTAgMS4xOC44NjMuODYzIDAgMDEtMS4yMDMgMGwtMS43ODgtMS43NTNhMS45MiAxLjkyIDAgMDEwLTIuNzU0bDguNzA2LTguNTM4YTIuNDcgMi40NyAwIDAwMC0zLjU0bC0uMDUtLjA0OWEyLjU4OCAyLjU4OCAwIDAwLTMuNjA3LS4wMDNsLTcuMTcyIDcuMDM0LS4wMDIuMDAyLS4wOTguMDk3YS44NjMuODYzIDAgMDEtMS4yMDQgMCAuODIzLjgyMyAwIDAxMC0xLjE4bDcuMjczLTcuMTMzYTIuNDcgMi40NyAwIDAwLS4wMDMtMy41Mzd6Ij48L3BhdGg+PHBhdGggZD0iTTE0LjQ4NSA0LjcwM2EuODIzLjgyMyAwIDAwMC0xLjE4Ljg2My44NjMgMCAwMC0xLjIwNCAwbC03LjExOSA2Ljk4MmE0LjExNSA0LjExNSAwIDAwMCA1LjkgNC4zMTQgNC4zMTQgMCAwMDYuMDE2IDBsNy4xMi02Ljk4MmEuODIzLjgyMyAwIDAwMC0xLjE4Ljg2My44NjMgMCAwMC0xLjIwNCAwbC03LjExOSA2Ljk4MmEyLjU4OCAyLjU4OCAwIDAxLTMuNjEgMCAyLjQ3IDIuNDcgMCAwMTAtMy41NGw3LjEyLTYuOTgyeiI+PC9wYXRoPjwvc3ZnPgo=" alt="MCP" />
  <img src="https://img.shields.io/badge/databases-postgres%20%7C%20sqlite%20%7C%20mysql-orange" alt="Databases" />
</p>

---

## What is this?

SQLens is a [Model Context Protocol](https://modelcontextprotocol.io/) server that connects AI coding assistants (Cursor, VS Code Copilot, Claude Desktop) to a live database. The AI can then:

- **Run read-only queries** and inspect results
- **Analyze execution plans** with severity-based diagnostics (slow / warn / ok)
- **Suggest indexes** with copy-paste-ready `CREATE INDEX` statements
- **Rewrite queries** for better performance
- **Inspect schemas, stats, and slow query logs**

It also ships with a **web UI** — a dark-themed SQL editor with an interactive execution plan tree visualizer.

## Features

### 🔧 8 MCP Tools

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables with row estimates and sizes |
| `get_schema` | Full column, index, and foreign key details for any table |
| `run_query` | Execute read-only SELECT queries (writes are blocked) |
| `explain_query` | Annotated EXPLAIN plan with severity scores and plain-English summaries |
| `suggest_indexes` | AST-based index recommendations with `CREATE INDEX CONCURRENTLY` DDL |
| `rewrite_query` | Structured query rewrite workflow with context gathering |
| `get_table_stats` | Table health: row counts, vacuum status, cache hit ratio |
| `get_slow_queries` | Slowest queries from `pg_stat_statements` (Postgres) |

### 🛡️ Safety Layer

Every SQL query passes through a multi-layer safety system before execution:

- **AST parsing** via `sqlglot` — blocks INSERT, UPDATE, DELETE, DROP, ALTER, and 20+ dangerous functions
- **Regex pre-filter** — catches write keywords even if the parser is bypassed
- **Single-statement enforcement** — no multi-statement injection
- **SELECT INTO blocking** — prevents silent data exfiltration
- **Table name validation** — regex guard against SQL injection in schema endpoints
- **Audit logging** — every tool call logged with inputs, outputs, and timing

### 🖥️ Web UI

A Next.js frontend with:

- **Monaco SQL editor** with syntax highlighting and line numbers
- **Interactive plan tree** — D3-powered node graph with severity-based color coding (red = slow, yellow = warn, green = ok)
- **Resizable split layout** — drag the handle to balance editor and results
- **Results table, Explain Plan, and Index Suggestions** — all in tabbed panels

### 🗄️ Multi-Database Support

| Database | Query | Explain | Schema | Stats | Slow Queries |
|----------|-------|---------|--------|-------|--------------|
| **PostgreSQL** | ✅ | ✅ JSON tree | ✅ | ✅ Full | ✅ pg_stat_statements |
| **SQLite** | ✅ | ✅ Flat plan | ✅ | ⚠️ Limited | ❌ Not available |
| **MySQL** | ✅ | ✅ Tabular | ✅ | ⚠️ Limited | ❌ Not available |

---

## Quick Start

### Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** — fast Python package manager
- **Node.js 18+** — for the web UI (optional)

### 1. Clone and install

```bash
git clone https://github.com/ianupk/sqlens.git
cd sqlens
uv sync
```

### 2. Configure your database

Copy the example env and edit it:

```bash
cp .env.example .env
```

**SQLite** (quickest — comes with a demo database):

```env
DB_TYPE=sqlite
SQLITE_PATH=./demo.db
```

**PostgreSQL**:

```env
DB_TYPE=postgres
DATABASE_URL=postgresql://readonly_user:password@localhost:5432/your_db
```

**MySQL**:

```env
DB_TYPE=mysql
MYSQL_HOST=localhost
MYSQL_USER=readonly_user
MYSQL_PASSWORD=password
MYSQL_DB=your_db
```

> **Tip:** Always use a read-only database user. The safety layer blocks writes, but defense-in-depth never hurts.

### 3. Seed the demo database (optional)

If you want to try it out with sample data (5 tables, ~400k rows):

```bash
uv run python scripts/seed_demo_db.py
```

This creates `demo.db` with `customers`, `orders`, `order_items`, `products`, and `product_reviews`.

---

## Usage

### As an MCP Server (AI Assistant)

#### Cursor / VS Code

Add to your `.cursor/mcp.json` or VS Code MCP settings:

```json
{
  "mcpServers": {
    "sqlens": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "python", "-m", "mcp_server.server"],
      "cwd": "/path/to/sqlens",
      "env": {
        "DB_TYPE": "sqlite",
        "SQLITE_PATH": "${workspaceFolder}/demo.db"
      }
    }
  }
}
```

#### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sqlens": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/sqlens", "python", "-m", "mcp_server.server"],
      "env": {
        "DB_TYPE": "sqlite",
        "SQLITE_PATH": "/path/to/sqlens/demo.db"
      }
    }
  }
}
```

#### Run standalone (for testing)

```bash
uv run python -m mcp_server.server
```

The server communicates over **stdio** using the MCP protocol.

---

### Web UI

Start the backend API and frontend:

```bash
# Terminal 1 — API server (FastAPI)
uv run uvicorn api.main:app --reload --port 8000

# Terminal 2 — Frontend (Next.js)
cd frontend
npm install
npm run dev
```

Open **<http://localhost:3000>** — you'll see the SQL editor, table sidebar, and plan visualizer.

---

## Architecture

```
sqlens/
├── mcp_server/          # MCP server entry point (stdio transport)
│   └── server.py        # Tool registration + system prompt
├── api/                 # REST API (FastAPI) for the web UI
│   ├── main.py          # App setup, CORS, routing
│   ├── dependencies.py  # Lazy driver + tool initialization
│   └── routes/          # HTTP endpoints → tool functions
├── tools/               # Core business logic
│   ├── query.py         # run_query, explain_query
│   ├── schema.py        # list_tables, get_schema, get_table_stats, get_slow_queries
│   └── optimizer.py     # suggest_indexes, rewrite_query (AST analysis)
├── db/                  # Database drivers
│   ├── base.py          # Abstract interface + data classes
│   ├── factory.py       # Driver factory (reads DB_TYPE from env)
│   ├── sqlite.py        # SQLite driver
│   ├── postgres.py      # PostgreSQL driver (psycopg3)
│   ├── mysql.py         # MySQL driver
│   └── plan_parser.py   # EXPLAIN output → annotated plan tree
├── middleware/
│   ├── safety.py        # SQL sanitization (sqlglot AST + regex)
│   └── audit.py         # JSONL audit logging decorator
├── frontend/            # Next.js web UI
│   ├── components/      # SqlEditor, PlanTree, ResizableSplit
│   └── lib/api.ts       # Frontend API client
└── scripts/
    └── seed_demo_db.py  # Generate demo data
```

### Data Flow

```
User (AI or Web UI)
  → SQL query
  → middleware/safety.py (sanitize + block writes)
  → tools/query.py (run_query or explain_query)
  → db/{sqlite,postgres,mysql}.py (execute against DB)
  → db/plan_parser.py (annotate plan with severity)
  → middleware/audit.py (log to audit.log)
  → Response (results / plan tree / suggestions)
```

---

## Example Queries

Once connected, ask your AI assistant:

```
"What tables exist in the database?"
"Show me orders for customer 42"
"Why is this query slow? SELECT * FROM orders WHERE status = 'pending'"
"Suggest indexes for this query"
"Rewrite this query to be faster"
```

Or paste a complex query in the web UI editor and click **Explain Plan** to see the visual tree.

---

## Configuration Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_TYPE` | Yes | `postgres` | Database type: `postgres`, `sqlite`, or `mysql` |
| `DATABASE_URL` | If Postgres | — | PostgreSQL connection string |
| `SQLITE_PATH` | If SQLite | `:memory:` | Path to SQLite database file |
| `MYSQL_HOST` | If MySQL | `localhost` | MySQL server hostname |
| `MYSQL_USER` | If MySQL | — | MySQL username |
| `MYSQL_PASSWORD` | If MySQL | — | MySQL password |
| `MYSQL_DB` | If MySQL | — | MySQL database name |

