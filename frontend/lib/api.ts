const BASE = "/api";

export interface RunQueryResponse {
    columns: string[];
    rows: Record<string, unknown>[];
    row_count: number;
    execution_ms: number;
    truncated: boolean;
    blocked?: boolean;
    error?: string;
}

export interface PlanNode {
    node_type: string;
    severity: "slow" | "warn" | "ok";
    reason: string;
    total_cost: number | null;
    plan_rows: number | null;
    actual_rows: number | null;
    relation_name: string | null;
    index_name: string | null;
    is_scan: boolean;
    children: PlanNode[];
}

export interface ExplainResponse {
    dialect: string;
    summary: string;
    total_cost: number | null;
    has_seq_scan: boolean;
    has_bad_estimate: boolean;
    slowest_nodes: PlanNode[];
    plan_tree: PlanNode[];
    error?: string;
}

export interface TableInfo {
    name: string;
    schema: string;
    row_estimate: number;
    size_human: string | null;
}

export interface IndexSuggestion {
    table: string;
    columns: string[];
    ddl: string;
    reason: string;
    impact: "high" | "medium" | "low";
    usage: string;
}

export interface SuggestResponse {
    suggestions: IndexSuggestion[];
    suggestion_count: number;
    message: string;
    error?: string;
}

export async function runQuery(
    sql: string,
    limit = 100,
): Promise<RunQueryResponse> {
    const res = await fetch(`${BASE}/query/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql, limit, reason: "web UI query" }),
    });
    return res.json();
}

export async function explainQuery(
    sql: string,
): Promise<ExplainResponse> {
    const res = await fetch(`${BASE}/query/explain`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql, analyze: false, reason: "web UI explain" }),
    });
    return res.json();
}

export async function listTables(): Promise<TableInfo[]> {
    const res = await fetch(`${BASE}/schema/tables`);
    const data = await res.json();
    return data.tables ?? [];
}

export async function suggestIndexes(
    sql: string,
): Promise<SuggestResponse> {
    const res = await fetch(`${BASE}/optimizer/indexes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql, reason: "web UI suggest indexes" }),
    });
    return res.json();
}
