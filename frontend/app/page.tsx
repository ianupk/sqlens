"use client";

import { useEffect, useState, useCallback } from "react";
import SqlEditor from "@/components/SqlEditor";
import ResultsTable from "@/components/ResultsTable";
import PlanTree from "@/components/PlanTree";
import IndexSuggestions from "@/components/IndexSuggestions";
import TableBrowser from "@/components/TableBrowser";
import {
    runQuery,
    explainQuery,
    listTables,
    suggestIndexes,
    type RunQueryResponse,
    type ExplainResponse,
    type SuggestResponse,
    type TableInfo,
} from "@/lib/api";

type ActiveTab = "results" | "plan" | "indexes";

const DEMO_SQL =
    "SELECT * FROM orders o WHERE o.customer_id = 42";

export default function Home() {
    const [sql, setSql] = useState(DEMO_SQL);
    const [loading, setLoading] = useState(false);
    const [activeTab, setActiveTab] = useState<ActiveTab>("results");
    const [tables, setTables] = useState<TableInfo[]>([]);
    const [queryResult, setQueryResult] = useState<RunQueryResponse | null>(null);
    const [explainResult, setExplainResult] = useState<ExplainResponse | null>(null);
    const [indexResult, setIndexResult] = useState<SuggestResponse | null>(null);
    const [error, setError] = useState<string | null>(null);

    // Load table list on mount
    useEffect(() => {
        listTables().then(setTables).catch(() => { });
    }, []);

    const handleRun = useCallback(async () => {
        if (!sql.trim() || loading) return;
        setLoading(true);
        setError(null);
        setQueryResult(null);
        setExplainResult(null);
        setIndexResult(null);

        try {
            // Run all three in parallel
            const [qr, er, ir] = await Promise.all([
                runQuery(sql),
                explainQuery(sql),
                suggestIndexes(sql),
            ]);

            if (qr.error && qr.blocked) {
                setError(`Blocked: ${qr.error}`);
            } else {
                setQueryResult(qr);
                setExplainResult(er);
                setIndexResult(ir);

                // Auto-switch to plan tab if seq scan detected
                if (er.has_seq_scan) setActiveTab("plan");
                else setActiveTab("results");
            }
        } catch (e) {
            setError("Could not reach the API server. Is it running on port 8000?");
        } finally {
            setLoading(false);
        }
    }, [sql, loading]);

    const handleSelectTable = (name: string) => {
        setSql(`SELECT * FROM ${name} LIMIT 50`);
    };

    return (
        <div style={{
            display: "grid",
            gridTemplateColumns: "220px 1fr",
            gridTemplateRows: "48px 1fr",
            height: "100vh",
            overflow: "hidden",
        }}>

            {/* Top bar */}
            <div style={{
                gridColumn: "1 / -1",
                display: "flex",
                alignItems: "center",
                padding: "0 16px",
                background: "var(--surface)",
                borderBottom: "1px solid var(--border)",
                gap: 12,
            }}>
                <span style={{ fontWeight: 600, fontSize: 15 }}>
                    SQL Optimizer
                </span>
                <span style={{ color: "var(--muted)", fontSize: 12 }}>
                    connected to demo.db
                </span>
            </div>

            {/* Sidebar — table browser */}
            <div style={{
                borderRight: "1px solid var(--border)",
                overflowY: "auto",
                padding: "12px 8px",
            }}>
                <div style={{
                    fontSize: 11,
                    fontWeight: 600,
                    color: "var(--muted)",
                    padding: "0 4px 8px",
                    letterSpacing: "0.06em",
                    textTransform: "uppercase",
                }}>
                    Tables
                </div>
                <TableBrowser
                    tables={tables}
                    onSelectTable={handleSelectTable}
                />
            </div>

            {/* Main content */}
            <div style={{
                display: "flex",
                flexDirection: "column",
                overflow: "hidden",
                padding: 16,
                gap: 12,
            }}>

                {/* Editor */}
                <SqlEditor
                    value={sql}
                    onChange={setSql}
                    onRun={handleRun}
                    loading={loading}
                />

                {/* Error */}
                {error && (
                    <div style={{
                        padding: "10px 14px",
                        background: "#ef444422",
                        border: "1px solid #ef4444",
                        borderRadius: 8,
                        color: "#ef4444",
                        fontSize: 13,
                    }}>
                        {error}
                    </div>
                )}

                {/* Tabs */}
                {(queryResult || explainResult || indexResult) && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 0, flex: 1, overflow: "hidden" }}>

                        {/* Tab bar */}
                        <div style={{
                            display: "flex",
                            gap: 2,
                            borderBottom: "1px solid var(--border)",
                            marginBottom: 12,
                        }}>
                            {(["results", "plan", "indexes"] as ActiveTab[]).map((tab) => {
                                const labels: Record<ActiveTab, string> = {
                                    results: `Results${queryResult ? ` (${queryResult.row_count})` : ""}`,
                                    plan: `Explain Plan${explainResult?.has_seq_scan ? " ⚠" : ""}`,
                                    indexes: `Index Suggestions${indexResult?.suggestion_count ? ` (${indexResult.suggestion_count})` : ""}`,
                                };
                                return (
                                    <button
                                        key={tab}
                                        onClick={() => setActiveTab(tab)}
                                        style={{
                                            padding: "7px 14px",
                                            background: "transparent",
                                            border: "none",
                                            borderBottom: activeTab === tab
                                                ? "2px solid var(--blue)"
                                                : "2px solid transparent",
                                            color: activeTab === tab
                                                ? "var(--text)"
                                                : "var(--muted)",
                                            cursor: "pointer",
                                            fontSize: 13,
                                            fontWeight: activeTab === tab ? 500 : 400,
                                            marginBottom: -1,
                                        }}
                                    >
                                        {labels[tab]}
                                    </button>
                                );
                            })}
                        </div>

                        {/* Tab content */}
                        <div style={{ flex: 1, overflowY: "auto" }}>
                            {activeTab === "results" && queryResult && !queryResult.error && (
                                <ResultsTable
                                    columns={queryResult.columns}
                                    rows={queryResult.rows}
                                    row_count={queryResult.row_count}
                                    execution_ms={queryResult.execution_ms}
                                    truncated={queryResult.truncated}
                                />
                            )}

                            {activeTab === "results" && queryResult?.error && (
                                <div style={{
                                    padding: "10px 14px",
                                    background: "var(--surface)",
                                    border: "1px solid var(--border)",
                                    borderRadius: 8,
                                    color: "var(--muted)",
                                    fontSize: 13,
                                }}>
                                    {queryResult.error}
                                </div>
                            )}

                            {activeTab === "plan" && explainResult && !explainResult.error && (
                                <PlanTree
                                    planTree={explainResult.plan_tree}
                                    summary={explainResult.summary}
                                />
                            )}

                            {activeTab === "indexes" && indexResult && (
                                <IndexSuggestions
                                    suggestions={indexResult.suggestions}
                                    message={indexResult.message}
                                />
                            )}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
