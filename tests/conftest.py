import sqlite3
import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def sqlite_db_path(tmp_path_factory) -> Path:
    """
    Creates a temporary SQLite database with realistic sample tables.
    Scoped to the session — created once, reused across all tests.

    Row counts are deliberately large enough to trigger SQLite's
    query planner to emit SCAN on unindexed columns. SQLite treats
    small tables as trivially cheap regardless of indexes, so we
    need enough rows to make the planner's decision meaningful.

    Table sizes:
        customers:    100 rows
        products:      50 rows
        orders:     50000 rows  ← triggers SCAN on customer_id (no index)
        order_items: 100000 rows
    """
    db_path = tmp_path_factory.mktemp("db") / "test.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE customers (
            id         INTEGER PRIMARY KEY,
            name       TEXT    NOT NULL,
            email      TEXT    UNIQUE NOT NULL,
            country    TEXT    NOT NULL,
            created_at TEXT    NOT NULL
        );

        CREATE TABLE products (
            id          INTEGER PRIMARY KEY,
            name        TEXT    NOT NULL,
            category    TEXT    NOT NULL,
            price_cents INTEGER NOT NULL
        );

        CREATE TABLE orders (
            id          INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            status      TEXT    NOT NULL,
            total_cents INTEGER NOT NULL,
            created_at  TEXT    NOT NULL
        );

        CREATE TABLE order_items (
            id               INTEGER PRIMARY KEY,
            order_id         INTEGER NOT NULL REFERENCES orders(id),
            product_id       INTEGER NOT NULL REFERENCES products(id),
            quantity         INTEGER NOT NULL,
            unit_price_cents INTEGER NOT NULL
        );

        -- Intentionally NO index on orders.customer_id.
        -- This is the slow query target for scan detection tests and demo.
        -- products.category has an index to contrast indexed vs unindexed.
        CREATE INDEX idx_products_category ON products(category);
    """)

    # --- customers: 100 rows ---
    customers = [
        (
            i,
            f"Customer {i}",
            f"customer{i}@example.com",
            ["IN", "US", "UK", "DE"][i % 4],
            "2024-01-01",
        )
        for i in range(1, 101)
    ]
    cursor.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?)",
        customers,
    )

    # --- products: 50 rows ---
    products = [
        (i, f"Product {i}", ["Electronics", "Books", "Clothing"][i % 3], i * 999)
        for i in range(1, 51)
    ]
    cursor.executemany(
        "INSERT INTO products VALUES (?,?,?,?)",
        products,
    )

    # --- orders: 50000 rows ---
    # customer_id cycles 1-100 so every customer has ~500 orders.
    # No index on customer_id — queries filtering by it will SCAN.
    orders = [
        (
            i,
            (i % 100) + 1,
            ["pending", "completed", "cancelled"][i % 3],
            i * 1500,
            "2024-06-01",
        )
        for i in range(1, 50001)
    ]
    cursor.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        orders,
    )

    # --- order_items: 100000 rows ---
    items = [
        (i, (i % 50000) + 1, (i % 50) + 1, (i % 5) + 1, 999)
        for i in range(1, 100001)
    ]
    cursor.executemany(
        "INSERT INTO order_items VALUES (?,?,?,?,?)",
        items,
    )

    conn.commit()
    conn.close()

    return db_path
