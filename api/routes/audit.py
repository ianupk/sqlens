from fastapi import APIRouter, Query as QueryParam

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/logs")
def get_audit_logs(
    limit: int = QueryParam(50, ge=1, le=500),
):
    from middleware.audit import read_audit_log
    entries = read_audit_log(limit=limit)
    return {
        "entries":     entries,
        "entry_count": len(entries),
        "limit":       limit,
    }
