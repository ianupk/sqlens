"use client";

interface Props {
    columns: string[];
    rows: Record<string, unknown>[];
    row_count: number;
    execution_ms: number;
    truncated: boolean;
}

export default function ResultsTable({
    columns,
    rows,
    row_count,
    execution_ms,
    truncated,
}: Props) {
    if (columns.length === 0) return null;

    return (
        <div>
            {/* Meta bar */}
            <div style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: 8,
            }}>
                <span style={{ color: "var(--muted)", fontSize: 12 }}>
                    {row_count.toLocaleString()} row{row_count !== 1 ? "s" : ""}
                    {truncated ? " (truncated)" : ""}
                </span>
                <span style={{ color: "var(--muted)", fontSize: 12 }}>
                    {execution_ms.toFixed(1)} ms
                </span>
            </div>

            {/* Table */}
            <div style={{ overflowX: "auto", borderRadius: 8, border: "1px solid var(--border)" }}>
                <table style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 13,
                }}>
                    <thead>
                        <tr style={{ background: "var(--surface)" }}>
                            {columns.map((col) => (
                                <th key={col} style={{
                                    padding: "8px 12px",
                                    textAlign: "left",
                                    fontWeight: 500,
                                    color: "var(--muted)",
                                    borderBottom: "1px solid var(--border)",
                                    whiteSpace: "nowrap",
                                }}>
                                    {col}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((row, i) => (
                            <tr
                                key={i}
                                style={{
                                    background: i % 2 === 0 ? "transparent" : "var(--surface)",
                                    borderBottom: "1px solid var(--border)",
                                }}
                            >
                                {columns.map((col) => {
                                    const val = row[col];
                                    return (
                                        <td key={col} style={{
                                            padding: "7px 12px",
                                            color: val === null ? "var(--muted)" : "var(--text)",
                                            fontFamily: typeof val === "number" ? "monospace" : "inherit",
                                            whiteSpace: "nowrap",
                                            maxWidth: 240,
                                            overflow: "hidden",
                                            textOverflow: "ellipsis",
                                        }}>
                                            {val === null
                                                ? "NULL"
                                                : typeof val === "object"
                                                    ? JSON.stringify(val)
                                                    : String(val)}
                                        </td>
                                    );
                                })}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
