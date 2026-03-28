"""
Seed script for the demo database.

Creates a SQLite database at the path specified by SQLITE_PATH
(or ./demo.db by default) with realistic e-commerce data designed
to showcase the optimizer's capabilities.

Deliberately bad design decisions included:
- orders.customer_id:      no index (triggers seq scan on large table)
- order_items.order_id:    no index (triggers seq scan on join)
- products.name:           no index (text search will be slow)
- orders.created_at:       no index (date range queries will be slow)
- product_reviews.product_id: no index (review lookups will be slow)

Run with:
    uv run python scripts/seed_demo_db.py
    uv run python scripts/seed_demo_db.py --path ./my_demo.db
    uv run python scripts/seed_demo_db.py --rows 200000
"""

import argparse
import os
import random
import sqlite3
import stat
import time
from datetime import date, timedelta
from pathlib import Path


COUNTRIES  = ["IN", "US", "UK", "DE", "FR", "JP", "AU", "CA", "BR", "SG"]
CATEGORIES = ["Electronics", "Books", "Clothing", "Home", "Sports", "Toys"]
STATUSES   = ["pending", "completed", "cancelled", "refunded", "shipped"]

FIRST_NAMES = [
    "Rahul", "Priya", "Amit", "Sneha", "Vikram", "Anjali", "Arjun",
    "Pooja", "Kiran", "Deepa", "James", "Sarah", "Michael", "Emma",
    "David", "Lisa", "Robert", "Jennifer", "William", "Jessica",
]
LAST_NAMES = [
    "Kumar", "Sharma", "Singh", "Patel", "Gupta", "Mehta", "Shah",
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Wilson", "Anderson", "Taylor", "Thomas",
]


def generate_date(start_year: int = 2022, end_year: int = 2024) -> str:
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def seed(db_path: str, n_orders: int = 100_000) -> None:
    path = Path(db_path)

    # If file exists and is read-only from a previous seed run,
    # we need to make it writable before we can remove it
    if path.exists():
        path.chmod(
            stat.S_IRUSR | stat.S_IWUSR |
            stat.S_IRGRP | stat.S_IWGRP
        )
        print(f"Removing existing DB at {path}")
        path.unlink()

    # remove WAL and SHM sidecar files if they exist
    for suffix in ["-wal", "-shm"]:
        sidecar = Path(db_path + suffix)
        if sidecar.exists():
            sidecar.unlink()

    print(f"Creating demo DB at {path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    cursor.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous  = NORMAL;
        PRAGMA foreign_keys = ON;

        CREATE TABLE customers (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT    NOT NULL,
            last_name  TEXT    NOT NULL,
            email      TEXT    UNIQUE NOT NULL,
            country    TEXT    NOT NULL,
            created_at TEXT    NOT NULL
        );

        CREATE TABLE products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            category    TEXT    NOT NULL,
            price_cents INTEGER NOT NULL,
            stock       INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT    NOT NULL
        );

        CREATE TABLE orders (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            status      TEXT    NOT NULL,
            total_cents INTEGER NOT NULL,
            created_at  TEXT    NOT NULL,
            shipped_at  TEXT
        );

        CREATE TABLE order_items (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id         INTEGER NOT NULL REFERENCES orders(id),
            product_id       INTEGER NOT NULL REFERENCES products(id),
            quantity         INTEGER NOT NULL,
            unit_price_cents INTEGER NOT NULL
        );

        CREATE TABLE product_reviews (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  INTEGER NOT NULL REFERENCES products(id),
            customer_id INTEGER NOT NULL REFERENCES customers(id),
            rating      INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
            review_text TEXT,
            created_at  TEXT    NOT NULL
        );

        -- Intentionally missing indexes (demo targets):
        --   orders.customer_id         → slow customer order lookups
        --   orders.created_at          → slow date range queries
        --   order_items.order_id       → slow order detail lookups
        --   product_reviews.product_id → slow review lookups

        -- Intentionally present indexes (demo contrast):
        CREATE INDEX idx_products_category ON products(category);
        CREATE UNIQUE INDEX idx_customers_email ON customers(email);
    """)

    # ------------------------------------------------------------------
    # Customers — 10,000 rows
    # ------------------------------------------------------------------
    n_customers = 10_000
    print(f"Seeding {n_customers:,} customers...")
    t = time.perf_counter()

    customers = []
    for i in range(1, n_customers + 1):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        customers.append((
            first,
            last,
            f"{first.lower()}.{last.lower()}.{i}@example.com",
            random.choice(COUNTRIES),
            generate_date(2020, 2022),
        ))

    cursor.executemany(
        "INSERT INTO customers (first_name, last_name, email, country, created_at) "
        "VALUES (?,?,?,?,?)",
        customers,
    )
    print(f"  done in {time.perf_counter() - t:.1f}s")

    # ------------------------------------------------------------------
    # Products — 500 rows
    # ------------------------------------------------------------------
    n_products = 500
    print(f"Seeding {n_products:,} products...")
    t = time.perf_counter()

    products = []
    for i in range(1, n_products + 1):
        cat = random.choice(CATEGORIES)
        products.append((
            f"{cat} Product {i}",
            cat,
            random.randint(499, 99999),
            random.randint(0, 500),
            generate_date(2020, 2022),
        ))

    cursor.executemany(
        "INSERT INTO products (name, category, price_cents, stock, created_at) "
        "VALUES (?,?,?,?,?)",
        products,
    )
    print(f"  done in {time.perf_counter() - t:.1f}s")

    # ------------------------------------------------------------------
    # Orders — n_orders rows (default 100k)
    # ------------------------------------------------------------------
    print(f"Seeding {n_orders:,} orders...")
    t = time.perf_counter()

    orders = []
    for i in range(1, n_orders + 1):
        customer_id = random.randint(1, n_customers)
        status      = random.choice(STATUSES)
        created     = generate_date(2022, 2024)
        shipped     = generate_date(2022, 2024) if status == "shipped" else None
        orders.append((
            customer_id,
            status,
            random.randint(999, 299999),
            created,
            shipped,
        ))

    batch = 10_000
    for start in range(0, n_orders, batch):
        cursor.executemany(
            "INSERT INTO orders "
            "(customer_id, status, total_cents, created_at, shipped_at) "
            "VALUES (?,?,?,?,?)",
            orders[start : start + batch],
        )
        print(f"  orders {start:,} – {min(start + batch, n_orders):,}")

    print(f"  done in {time.perf_counter() - t:.1f}s")

    # ------------------------------------------------------------------
    # Order items — 1 to 5 items per order
    # ------------------------------------------------------------------
    print(f"Seeding order items (~3 per order)...")
    t = time.perf_counter()

    items = []
    for order_id in range(1, n_orders + 1):
        n = random.randint(1, 5)
        for _ in range(n):
            product_id = random.randint(1, n_products)
            items.append((
                order_id,
                product_id,
                random.randint(1, 10),
                random.randint(499, 99999),
            ))

    batch = 20_000
    for start in range(0, len(items), batch):
        cursor.executemany(
            "INSERT INTO order_items "
            "(order_id, product_id, quantity, unit_price_cents) "
            "VALUES (?,?,?,?)",
            items[start : start + batch],
        )
        print(f"  items {start:,} – {min(start + batch, len(items)):,}")

    print(f"  done in {time.perf_counter() - t:.1f}s")

    # ------------------------------------------------------------------
    # Product reviews — ~2 reviews per product
    # ------------------------------------------------------------------
    n_reviews = n_products * 2
    print(f"Seeding {n_reviews:,} product reviews...")
    t = time.perf_counter()

    reviews = []
    for i in range(n_reviews):
        reviews.append((
            random.randint(1, n_products),
            random.randint(1, n_customers),
            random.randint(1, 5),
            f"Review text {i}" if random.random() > 0.3 else None,
            generate_date(2022, 2024),
        ))

    cursor.executemany(
        "INSERT INTO product_reviews "
        "(product_id, customer_id, rating, review_text, created_at) "
        "VALUES (?,?,?,?,?)",
        reviews,
    )
    print(f"  done in {time.perf_counter() - t:.1f}s")

    # ------------------------------------------------------------------
    # Commit and close BEFORE setting permissions
    # ------------------------------------------------------------------
    conn.commit()

    print("\nDatabase summary:")
    for table in [
        "customers", "products", "orders",
        "order_items", "product_reviews",
    ]:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"  {table:<25} {row[0]:>10,} rows")

    conn.close()

    # ------------------------------------------------------------------
    # Set read-only AFTER all writes are complete and connection closed
    # ------------------------------------------------------------------
    path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    print(f"\nSet {path} to read-only (chmod 444)")
    print(f"Demo DB ready at: {path.resolve()}")
    print("\nSlow demo queries to try:")
    print("  SELECT * FROM orders o WHERE o.customer_id = 42")
    print("  SELECT * FROM orders o WHERE o.created_at BETWEEN '2023-01-01' AND '2023-12-31'")
    print("  SELECT * FROM order_items oi WHERE oi.order_id = 1")
    print("  SELECT * FROM product_reviews pr WHERE pr.product_id = 10")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the demo database.")
    parser.add_argument(
        "--path",
        default=os.getenv("SQLITE_PATH", "./demo.db"),
        help="Path to the SQLite database file (default: ./demo.db)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Number of orders to generate (default: 100000)",
    )
    args = parser.parse_args()
    seed(db_path=args.path, n_orders=args.rows)
