"use client";

import type { IndexSuggestion } from "@/lib/api";

interface Props {
    suggestions: IndexSuggestion[];
    message: string;
}

const IMPACT_COLOR: Record<string, string> = {
    high: "#ef4444",
    medium: "#f59e0b",
    low: "#22c55e",
};

export default function IndexSuggestions({ suggestions, message }: Props) {
    if (suggestions.length === 0) {
        return (
            <div style={{
                padding: 12,
                background: "var(--surface)",
                border: "1px solid var(--border)",
                borderRadius: 8,
                color: "var(--muted)",
                fontSize: 13,
            }}>
                {message}
            </div>
        );
    }

    return (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {suggestions.map((s, i) => (
                <div key={i} style={{
                    background: "var(--surface)",
                    border: "1px solid var(--border)",
                    borderRadius: 8,
                    overflow: "hidden",
                }}>
                    {/* Header */}
                    <div style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 10,
                        padding: "10px 14px",
                        borderBottom: "1px solid var(--border)",
                    }}>
                        <span style={{
                            fontSize: 10,
                            fontWeight: 600,
                            padding: "2px 8px",
                            borderRadius: 20,
                            background: IMPACT_COLOR[s.impact] + "22",
                            color: IMPACT_COLOR[s.impact],
                            textTransform: "uppercase",
                            letterSpacing: "0.05em",
                        }}>
                            {s.impact}
                        </span>
                        <span style={{ fontWeight: 500, fontSize: 13 }}>
                            {s.table} ({s.columns.join(", ")})
                        </span>
                        <span style={{ fontSize: 12, color: "var(--muted)", marginLeft: "auto" }}>
                            {s.usage}
                        </span>
                    </div>

                    {/* DDL */}
                    <div style={{
                        padding: "10px 14px",
                        background: "#0a0c12",
                        fontFamily: "monospace",
                        fontSize: 12,
                        color: "#7dd3fc",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        gap: 12,
                    }}>
                        <code>{s.ddl}</code>
                        <button
                            onClick={() => navigator.clipboard.writeText(s.ddl)}
                            style={{
                                background: "var(--surface)",
                                border: "1px solid var(--border)",
                                borderRadius: 6,
                                color: "var(--muted)",
                                padding: "3px 10px",
                                fontSize: 11,
                                cursor: "pointer",
                                whiteSpace: "nowrap",
                                flexShrink: 0,
                            }}
                        >
                            Copy
                        </button>
                    </div>

                    {/* Reason */}
                    <div style={{
                        padding: "8px 14px",
                        fontSize: 12,
                        color: "var(--muted)",
                    }}>
                        {s.reason}
                    </div>
                </div>
            ))}
        </div>
    );
}
