"use client";

import { useEffect, useState, useCallback } from "react";
import SqlEditor from "@/components/SqlEditor";
import ResultsTable from "@/components/ResultsTable";
import PlanTree from "@/components/PlanTree";
import IndexSuggestions from "@/components/IndexSuggestions";
import TableBrowser from "@/components/TableBrowser";
import ResizableSplit from "@/components/ResizableSplit";
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

const DEMO_SQL = "SELECT * FROM orders o WHERE o.customer_id = 42";

export default function Home() {
    const [sql, setSql] = useState(DEMO_SQL);
    const [loading, setLoading] = useState(false);
    const [activeTab, setActiveTab] = useState<ActiveTab>("results");
    const [tables, setTables] = useState<TableInfo[]>([]);
    const [queryResult, setQueryResult] = useState<RunQueryResponse | null>(null);
    const [explainResult, setExplainResult] = useState<ExplainResponse | null>(null);
    const [indexResult, setIndexResult] = useState<SuggestResponse | null>(null);
    const [error, setError] = useState<string | null>(null);

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

    const hasResults = queryResult || explainResult || indexResult;

    const tabStyle = (isActive: boolean): React.CSSProperties => ({
        padding: "7px 16px",
        background: "transparent",
        border: "none",
        borderBottom: isActive ? "2px solid #3b82f6" : "2px solid transparent",
        color: isActive ? "#e2e8f0" : "#64748b",
        cursor: "pointer",
        fontSize: 13,
        fontWeight: isActive ? 500 : 400,
        marginBottom: -1,
        transition: "color 0.12s, border-color 0.12s",
    });

    const editorPanel = (
        <div style={{ padding: "12px 16px 6px", height: "100%", display: "flex", flexDirection: "column" }}>
            <SqlEditor value={sql} onChange={setSql} onRun={handleRun} loading={loading} />
            {error && (
                <div style={{
                    padding: "8px 12px", marginTop: 8, flexShrink: 0,
                    background: "#ef444422", border: "1px solid #ef4444",
                    borderRadius: 6, color: "#ef4444", fontSize: 12,
                }}>
                    {error}
                </div>
            )}
        </div>
    );

    const resultsPanel = (
        <div style={{ padding: "0 16px 16px", height: "100%", display: "flex", flexDirection: "column" }}>
            {hasResults ? (
                <>
                    <div style={{
                        display: "flex", gap: 0,
                        borderBottom: "1px solid #2a2d3a",
                        flexShrink: 0, paddingTop: 2,
                    }}>
                        {(["results", "plan", "indexes"] as ActiveTab[]).map((tab) => {
                            const labels: Record<ActiveTab, string> = {
                                results: `Results${queryResult ? ` (${queryResult.row_count})` : ""}`,
                                plan: `Explain Plan${explainResult?.has_seq_scan ? " ⚠" : ""}`,
                                indexes: `Index Suggestions${indexResult?.suggestion_count ? ` (${indexResult.suggestion_count})` : ""}`,
                            };
                            return (
                                <button key={tab} onClick={() => setActiveTab(tab)} style={tabStyle(activeTab === tab)}>
                                    {labels[tab]}
                                </button>
                            );
                        })}
                    </div>
                    <div style={{ flex: 1, minHeight: 0, paddingTop: 10, overflow: "auto" }}>
                        {activeTab === "results" && queryResult && !queryResult.error && (
                            <ResultsTable
                                columns={queryResult.columns} rows={queryResult.rows}
                                row_count={queryResult.row_count} execution_ms={queryResult.execution_ms}
                                truncated={queryResult.truncated}
                            />
                        )}
                        {activeTab === "results" && queryResult?.error && (
                            <div style={{
                                padding: "10px 14px", background: "#1a1d27",
                                border: "1px solid #2a2d3a", borderRadius: 8,
                                color: "#64748b", fontSize: 13,
                            }}>
                                {queryResult.error}
                            </div>
                        )}
                        {activeTab === "plan" && explainResult && !explainResult.error && (
                            <PlanTree planTree={explainResult.plan_tree} summary={explainResult.summary} />
                        )}
                        {activeTab === "indexes" && indexResult && (
                            <IndexSuggestions suggestions={indexResult.suggestions} message={indexResult.message} />
                        )}
                    </div>
                </>
            ) : (
                <div style={{
                    flex: 1, display: "flex", alignItems: "center", justifyContent: "center",
                    color: "#64748b", fontSize: 13, opacity: 0.5,
                }}>
                    Run a query to see results
                </div>
            )}
        </div>
    );

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
                display: "flex", alignItems: "center",
                padding: "0 16px",
                background: "#1a1d27",
                borderBottom: "1px solid #2a2d3a",
                gap: 12,
            }}>
                <span style={{ fontWeight: 600, fontSize: 15 }}>SQL Optimizer</span>
                <span style={{ color: "#64748b", fontSize: 12 }}>connected to demo.db</span>
            </div>

            {/* Sidebar */}
            <div style={{
                borderRight: "1px solid #2a2d3a",
                overflowY: "auto", padding: "12px 8px",
            }}>
                <div style={{
                    fontSize: 11, fontWeight: 600, color: "#64748b",
                    padding: "0 4px 8px", letterSpacing: "0.06em", textTransform: "uppercase" as const,
                }}>
                    Tables
                </div>
                <TableBrowser tables={tables} onSelectTable={handleSelectTable} />
            </div>

            {/* Main — resizable split */}
            <div style={{ overflow: "hidden" }}>
                <ResizableSplit
                    top={editorPanel}
                    bottom={resultsPanel}
                    initialRatio={0.35}
                    minSize={80}
                />
            </div>
        </div>
    );
}
