from fastapi import APIRouter, Query as QueryParam

router = APIRouter(prefix="/schema", tags=["schema"])


@router.get("/tables")
def list_tables(
    schema: str = QueryParam("public", description="Schema to list tables from"),
    reason: str = QueryParam("", description="Why you are listing tables"),
):
    from api.dependencies import get_tools
    tools = get_tools()
    return tools["list_tables"](schema=schema, reason=reason)


@router.get("/table/{table}")
def get_schema(
    table:  str,
    reason: str = QueryParam("", description="Why you need this schema"),
):
    from api.dependencies import get_tools
    tools = get_tools()
    return tools["get_schema"](table=table, reason=reason)


@router.get("/stats/{table}")
def get_table_stats(
    table:  str,
    reason: str = QueryParam(""),
):
    from api.dependencies import get_tools
    tools = get_tools()
    return tools["get_table_stats"](table=table, reason=reason)


@router.get("/slow-queries")
def get_slow_queries(
    min_ms: int = QueryParam(100, ge=0),
    reason: str = QueryParam(""),
):
    from api.dependencies import get_tools
    tools = get_tools()
    return tools["get_slow_queries"](min_ms=min_ms, reason=reason)
