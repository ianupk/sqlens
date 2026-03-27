from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Any


SEVERITY_SLOW = "slow"
SEVERITY_WARN = "warn"
SEVERITY_OK   = "ok"

SLOW_NODE_TYPES = frozenset({
    "Seq Scan",
    "Bitmap Heap Scan",
})

WARN_NODE_TYPES = frozenset({
    "Hash Join",
    "Merge Join",
    "Nested Loop",
    "Sort",
    "Materialize",
    "Gather",
    "Gather Merge",
})

SLOW_COST_THRESHOLD = 1000.0
WARN_COST_THRESHOLD = 100.0
SLOW_ROW_THRESHOLD  = 10_000
WARN_ROW_THRESHOLD  = 1_000
ESTIMATE_ACCURACY_THRESHOLD = 10.0


@dataclass
class PlanNodeAnnotated:
    node_type: str
    severity: str

    startup_cost: float | None
    total_cost: float | None
    plan_rows: int | None
    actual_rows: int | None
    actual_ms: float | None

    relation_name: str | None
    index_name: str | None
    join_type: str | None
    filter: str | None

    estimate_off: bool
    estimate_ratio: float | None

    is_parallel: bool
    is_scan: bool

    reason: str

    children: list[PlanNodeAnnotated] = field(default_factory=list)
    raw: dict = field(default_factory=dict)


@dataclass
class ParsedPlan:
    dialect: str
    nodes: list[PlanNodeAnnotated]
    total_cost: float | None
    slowest_nodes: list[PlanNodeAnnotated]
    has_seq_scan: bool
    has_bad_estimate: bool
    summary: str


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def parse_explain(raw_result: dict) -> ParsedPlan:
    dialect   = raw_result.get("dialect", "postgres")
    plan_data = raw_result.get("plan", {})

    if dialect == "sqlite":
        return _parse_sqlite_plan(plan_data)
    else:
        return _parse_postgres_plan(plan_data)


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def _parse_postgres_plan(plan_data: Any) -> ParsedPlan:
    if not plan_data:
        return _empty_plan("postgres")

    if isinstance(plan_data, list) and len(plan_data) > 0:
        root_wrapper = plan_data[0]
    elif isinstance(plan_data, dict):
        root_wrapper = plan_data
    else:
        return _empty_plan("postgres")

    root_node_raw = root_wrapper.get("Plan", {})
    if not root_node_raw:
        return _empty_plan("postgres")

    root_annotated = _annotate_postgres_node(root_node_raw)
    all_nodes = _flatten(root_annotated)

    slow_nodes = sorted(
        [n for n in all_nodes if n.severity == SEVERITY_SLOW],
        key=lambda n: n.total_cost or 0,
        reverse=True,
    )[:3]

    warn_nodes = sorted(
        [n for n in all_nodes if n.severity == SEVERITY_WARN],
        key=lambda n: n.total_cost or 0,
        reverse=True,
    )[:3]

    slowest = slow_nodes or warn_nodes

    has_seq_scan     = any(n.node_type == "Seq Scan" for n in all_nodes)
    has_bad_estimate = any(n.estimate_off for n in all_nodes)
    total_cost       = root_annotated.total_cost

    summary = _build_postgres_summary(
        root_annotated, has_seq_scan, has_bad_estimate, slow_nodes
    )

    return ParsedPlan(
        dialect="postgres",
        nodes=[root_annotated],
        total_cost=total_cost,
        slowest_nodes=slowest,
        has_seq_scan=has_seq_scan,
        has_bad_estimate=has_bad_estimate,
        summary=summary,
    )


def _annotate_postgres_node(node: dict) -> PlanNodeAnnotated:
    node_type    = node.get("Node Type", "Unknown")
    total_cost   = node.get("Total Cost")
    startup_cost = node.get("Startup Cost")
    plan_rows    = node.get("Plan Rows")
    actual_rows  = node.get("Actual Rows")
    actual_ms    = node.get("Actual Total Time")

    relation_name = node.get("Relation Name")
    index_name    = node.get("Index Name")
    join_type     = node.get("Join Type")
    filter_expr   = node.get("Filter") or node.get("Index Cond")

    estimate_ratio = None
    estimate_off   = False
    if actual_rows is not None and plan_rows and plan_rows > 0:
        estimate_ratio = actual_rows / plan_rows
        estimate_off = (
            estimate_ratio > ESTIMATE_ACCURACY_THRESHOLD or
            estimate_ratio < (1 / ESTIMATE_ACCURACY_THRESHOLD)
        )

    is_parallel = node_type in ("Gather", "Gather Merge")
    is_scan     = "Scan" in node_type

    severity, reason = _score_postgres_node(
        node_type=node_type,
        total_cost=total_cost,
        plan_rows=plan_rows,
        actual_rows=actual_rows,
        relation_name=relation_name,
        estimate_off=estimate_off,
        estimate_ratio=estimate_ratio,
    )

    children = [
        _annotate_postgres_node(child)
        for child in node.get("Plans", [])
    ]

    return PlanNodeAnnotated(
        node_type=node_type,
        severity=severity,
        startup_cost=startup_cost,
        total_cost=total_cost,
        plan_rows=plan_rows,
        actual_rows=actual_rows,
        actual_ms=actual_ms,
        relation_name=relation_name,
        index_name=index_name,
        join_type=join_type,
        filter=filter_expr,
        estimate_off=estimate_off,
        estimate_ratio=round(estimate_ratio, 2) if estimate_ratio else None,
        is_parallel=is_parallel,
        is_scan=is_scan,
        reason=reason,
        children=children,
        raw=node,
    )


def _score_postgres_node(
    node_type: str,
    total_cost: float | None,
    plan_rows: int | None,
    actual_rows: int | None,
    relation_name: str | None,
    estimate_off: bool,
    estimate_ratio: float | None,
) -> tuple[str, str]:
    rows = actual_rows if actual_rows is not None else plan_rows or 0
    cost = total_cost or 0.0

    if node_type == "Seq Scan":
        if rows >= SLOW_ROW_THRESHOLD:
            table = f" on '{relation_name}'" if relation_name else ""
            return (
                SEVERITY_SLOW,
                f"Sequential scan{table} reading {rows:,} rows. "
                f"An index on the filter columns would likely eliminate this.",
            )
        elif rows >= WARN_ROW_THRESHOLD:
            table = f" on '{relation_name}'" if relation_name else ""
            return (
                SEVERITY_WARN,
                f"Sequential scan{table} reading {rows:,} rows. "
                f"Acceptable for small tables but watch if this table grows.",
            )

    if node_type in SLOW_NODE_TYPES and cost >= SLOW_COST_THRESHOLD:
        return (
            SEVERITY_SLOW,
            f"{node_type} with cost {cost:.1f} — "
            f"high cost operation on {rows:,} rows.",
        )

    if node_type in WARN_NODE_TYPES and cost >= WARN_COST_THRESHOLD:
        return (
            SEVERITY_WARN,
            f"{node_type} with cost {cost:.1f}. "
            f"May become expensive as data grows.",
        )

    if cost >= SLOW_COST_THRESHOLD:
        return (
            SEVERITY_WARN,
            f"High total cost ({cost:.1f}) for {node_type}.",
        )

    if estimate_off and estimate_ratio is not None:
        direction = "over" if estimate_ratio < 1 else "under"
        return (
            SEVERITY_WARN,
            f"Planner {direction}-estimated rows by "
            f"{estimate_ratio:.1f}x (planned {plan_rows:,}, "
            f"actual {actual_rows:,}). "
            f"Run ANALYZE to refresh statistics.",
        )

    return (
        SEVERITY_OK,
        f"{node_type} — cost {cost:.1f}, {rows:,} rows. No issues.",
    )


def _build_postgres_summary(
    root: PlanNodeAnnotated,
    has_seq_scan: bool,
    has_bad_estimate: bool,
    slow_nodes: list[PlanNodeAnnotated],
) -> str:
    parts = []

    if slow_nodes:
        worst = slow_nodes[0]
        parts.append(
            f"Main bottleneck: {worst.node_type}"
            + (f" on '{worst.relation_name}'" if worst.relation_name else "")
            + f" (cost {worst.total_cost:.1f})."
        )

    if has_seq_scan:
        parts.append("Contains sequential scan(s) — likely missing index.")

    if has_bad_estimate:
        parts.append("Planner estimates are inaccurate — run ANALYZE.")

    if not parts:
        total = root.total_cost or 0
        parts.append(f"Query plan looks healthy. Total cost: {total:.1f}.")

    return " ".join(parts)


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

def _parse_sqlite_plan(plan_rows: list[dict]) -> ParsedPlan:
    if not plan_rows:
        return _empty_plan("sqlite")

    nodes            = []
    has_seq_scan     = False
    has_bad_estimate = False

    for row in plan_rows:
        detail    = row.get("detail", "")
        node_type, severity, reason = _score_sqlite_row(detail)

        if severity == SEVERITY_SLOW:
            has_seq_scan = True

        nodes.append(PlanNodeAnnotated(
            node_type=node_type,
            severity=severity,
            startup_cost=None,
            total_cost=None,
            plan_rows=None,
            actual_rows=None,
            actual_ms=None,
            relation_name=_extract_sqlite_table(detail),
            index_name=_extract_sqlite_index(detail),
            join_type=None,
            filter=None,
            estimate_off=False,
            estimate_ratio=None,
            is_parallel=False,
            is_scan="SCAN" in detail.upper(),
            reason=reason,
            children=[],
            raw=row,
        ))

    slow_nodes = [n for n in nodes if n.severity == SEVERITY_SLOW]
    summary    = _build_sqlite_summary(nodes, has_seq_scan)

    return ParsedPlan(
        dialect="sqlite",
        nodes=nodes,
        total_cost=None,
        slowest_nodes=slow_nodes[:3],
        has_seq_scan=has_seq_scan,
        has_bad_estimate=False,
        summary=summary,
    )


def _score_sqlite_row(detail: str) -> tuple[str, str, str]:
    """
    Score a single SQLite EXPLAIN QUERY PLAN row.

    SQLite detail strings vary by version:
      "SCAN orders"                              → slow (no TABLE keyword)
      "SCAN TABLE orders"                        → slow (older SQLite)
      "SEARCH orders USING INDEX ..."            → ok
      "SEARCH TABLE customers USING INDEX ..."   → ok (older SQLite)
      "USE TEMP B-TREE FOR ORDER BY"             → warn
      "SCAN SUBQUERY ..."                        → warn
    """
    upper = detail.upper()

    # Match "SCAN <table>" or "SCAN TABLE <table>" but NOT "SEARCH"
    # Use word boundary so SCAN doesn't match inside other words
    if re.match(r"SCAN\b", upper) and "SEARCH" not in upper and "SUBQUERY" not in upper:
        return (
            "Seq Scan",
            SEVERITY_SLOW,
            f"Full table scan: {detail}. "
            f"Add an index on the columns used in WHERE or JOIN.",
        )

    if "SCAN SUBQUERY" in upper:
        return (
            "Subquery Scan",
            SEVERITY_WARN,
            f"Scanning a subquery result: {detail}. "
            f"Consider rewriting as a JOIN.",
        )

    if "USE TEMP B-TREE" in upper:
        return (
            "Sort",
            SEVERITY_WARN,
            f"Temporary sort operation: {detail}. "
            f"An index on the ORDER BY columns may eliminate this.",
        )

    if "SEARCH" in upper and "USING" in upper:
        return (
            "Index Scan",
            SEVERITY_OK,
            f"Using index: {detail}.",
        )

    return (
        "Other",
        SEVERITY_OK,
        detail,
    )


def _extract_sqlite_table(detail: str) -> str | None:
    """
    Extract table name from SQLite EXPLAIN QUERY PLAN detail string.
    Handles both modern form "SCAN orders" and older "SCAN TABLE orders".
    """
    match = re.search(
        r"(?:SCAN\s+TABLE\s+|SCAN\s+|SEARCH\s+TABLE\s+|SEARCH\s+)(\w+)",
        detail,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def _extract_sqlite_index(detail: str) -> str | None:
    """Extract index name from SQLite EXPLAIN QUERY PLAN detail string."""
    match = re.search(
        r"USING\s+(?:INDEX\s+)?(\w+)",
        detail,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def _build_sqlite_summary(
    nodes: list[PlanNodeAnnotated],
    has_seq_scan: bool,
) -> str:
    if has_seq_scan:
        scan_tables = [
            n.relation_name for n in nodes
            if n.severity == SEVERITY_SLOW and n.relation_name
        ]
        tables_str = ", ".join(f"'{t}'" for t in scan_tables) \
                     if scan_tables else "one or more tables"
        return (
            f"Full table scan on {tables_str}. "
            f"Add indexes on filter and join columns."
        )

    warn_nodes = [n for n in nodes if n.severity == SEVERITY_WARN]
    if warn_nodes:
        return f"Query uses indexes but has {len(warn_nodes)} potential issue(s)."

    return "Query uses indexes efficiently. No obvious issues."


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _flatten(node: PlanNodeAnnotated) -> list[PlanNodeAnnotated]:
    result = [node]
    for child in node.children:
        result.extend(_flatten(child))
    return result


def _empty_plan(dialect: str) -> ParsedPlan:
    return ParsedPlan(
        dialect=dialect,
        nodes=[],
        total_cost=None,
        slowest_nodes=[],
        has_seq_scan=False,
        has_bad_estimate=False,
        summary="No plan data available.",
    )


def plan_to_dict(plan: ParsedPlan) -> dict:
    """Convert a ParsedPlan to a JSON-serializable dict for tool return."""

    def node_to_dict(n: PlanNodeAnnotated) -> dict:
        return {
            "node_type":      n.node_type,
            "severity":       n.severity,
            "reason":         n.reason,
            "total_cost":     n.total_cost,
            "plan_rows":      n.plan_rows,
            "actual_rows":    n.actual_rows,
            "actual_ms":      n.actual_ms,
            "relation_name":  n.relation_name,
            "index_name":     n.index_name,
            "join_type":      n.join_type,
            "filter":         n.filter,
            "estimate_off":   n.estimate_off,
            "estimate_ratio": n.estimate_ratio,
            "is_scan":        n.is_scan,
            "children":       [node_to_dict(c) for c in n.children],
        }

    return {
        "dialect":          plan.dialect,
        "total_cost":       plan.total_cost,
        "summary":          plan.summary,
        "has_seq_scan":     plan.has_seq_scan,
        "has_bad_estimate": plan.has_bad_estimate,
        "slowest_nodes": [
            {
                "node_type":     n.node_type,
                "severity":      n.severity,
                "reason":        n.reason,
                "relation_name": n.relation_name,
                "total_cost":    n.total_cost,
            }
            for n in plan.slowest_nodes
        ],
        "plan_tree": [node_to_dict(n) for n in plan.nodes],
    }
