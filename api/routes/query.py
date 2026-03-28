from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/query", tags=["query"])


class RunQueryRequest(BaseModel):
    sql:    str = Field(..., description="SELECT query to execute")
    limit:  int = Field(100, ge=1, le=1000)
    reason: str = Field("")


class ExplainQueryRequest(BaseModel):
    sql:     str  = Field(..., description="SELECT query to explain")
    analyze: bool = Field(False)
    reason:  str  = Field("")


@router.post("/run")
def run_query(body: RunQueryRequest):
    from api.dependencies import get_tools
    result = get_tools()["run_query"](
        sql=body.sql,
        limit=body.limit,
        reason=body.reason,
    )
    # Ensure blocked key is always present — False on success, True on block
    if "blocked" not in result:
        result["blocked"] = False
    return result


@router.post("/explain")
def explain_query(body: ExplainQueryRequest):
    from api.dependencies import get_tools
    return get_tools()["explain_query"](
        sql=body.sql,
        analyze=body.analyze,
        reason=body.reason,
    )
