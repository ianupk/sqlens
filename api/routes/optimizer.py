from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/optimizer", tags=["optimizer"])


class SuggestIndexesRequest(BaseModel):
    sql:    str = Field(..., description="SELECT query to analyze")
    reason: str = Field("")


class RewriteQueryRequest(BaseModel):
    sql:    str = Field(..., description="SELECT query to rewrite")
    goal:   str = Field(..., description="What the rewrite should achieve")
    reason: str = Field("")


@router.post("/indexes")              # ← was /suggest-indexes
def suggest_indexes(body: SuggestIndexesRequest):
    from api.dependencies import get_tools
    return get_tools()["suggest_indexes"](
        sql=body.sql,
        reason=body.reason,
    )


@router.post("/rewrite")
def rewrite_query(body: RewriteQueryRequest):
    from api.dependencies import get_tools
    return get_tools()["rewrite_query"](
        sql=body.sql,
        goal=body.goal,
        reason=body.reason,
    )
