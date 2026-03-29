"use client";

import type { TableInfo } from "@/lib/api";

interface Props {
    tables: TableInfo[];
    onSelectTable: (name: string) => void;
}

export default function TableBrowser({ tables, onSelectTable }: Props) {
    return (
        <div style={{
            display: "flex",
            flexDirection: "column",
            gap: 2,
        }}>
            {tables.map((t) => (
                <button
                    key={t.name}
                    onClick={() => onSelectTable(t.name)}
                    style={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        padding: "7px 10px",
                        background: "transparent",
                        border: "none",
                        borderRadius: 6,
                        cursor: "pointer",
                        textAlign: "left",
                        width: "100%",
                        transition: "background 0.1s",
                    }}
                    onMouseEnter={(e) =>
                        (e.currentTarget.style.background = "var(--surface)")
                    }
                    onMouseLeave={(e) =>
                        (e.currentTarget.style.background = "transparent")
                    }
                >
                    <span style={{ fontSize: 13, color: "var(--text)" }}>
                        {t.name}
                    </span>
                    <span style={{ fontSize: 11, color: "var(--muted)" }}>
                        {t.row_estimate.toLocaleString()}
                    </span>
                </button>
            ))}
        </div>
    );
}
