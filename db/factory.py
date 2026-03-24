import os
from dotenv import load_dotenv
from db.base import DBDriver

load_dotenv()

def get_driver() -> DBDriver:
    db_type = os.getenv("DB_TYPE", "postgres").lower().strip()

    if db_type == "postgres":
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise EnvironmentError(
                "DATABASE_URL is required when DB_TYPE=postgres.\n"
                "Example: postgresql://readonly_user:secret@localhost:5432/devdb"
            )
        from db.postgres import PostgresDriver
        return PostgresDriver(dsn=dsn)

    elif db_type == "sqlite":
        path = os.getenv("SQLITE_PATH", ":memory:")
        from db.sqlite import SQLiteDriver
        return SQLiteDriver(path=path)

    elif db_type == "mysql":
        from db.mysql import MySQLDriver
        return MySQLDriver(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB"),
        )

    raise EnvironmentError(
        f"Unknown DB_TYPE: '{db_type}'. "
        f"Valid options: postgres, sqlite, mysql"
    )