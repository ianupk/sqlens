import os
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


@lru_cache(maxsize=1)
def get_driver():
    from db.factory import get_driver as _get_driver
    return _get_driver()


@lru_cache(maxsize=1)
def get_tools():
    from tools.query import make_query_tools
    from tools.schema import make_schema_tools
    from tools.optimizer import make_optimizer_tools

    driver = get_driver()

    run_query, explain_query = make_query_tools(driver)
    list_tables, get_schema, get_table_stats, get_slow_queries = (
        make_schema_tools(driver)
    )
    suggest_indexes, rewrite_query = make_optimizer_tools(driver)

    return {
        "run_query":       run_query,
        "explain_query":   explain_query,
        "list_tables":     list_tables,
        "get_schema":      get_schema,
        "get_table_stats": get_table_stats,
        "get_slow_queries": get_slow_queries,
        "suggest_indexes": suggest_indexes,
        "rewrite_query":   rewrite_query,
    }
