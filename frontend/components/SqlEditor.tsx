"use client";

import dynamic from "next/dynamic";
import type { EditorProps } from "@monaco-editor/react";
import { useRef } from "react";

const MonacoEditor = dynamic<EditorProps>(
    () => import("@monaco-editor/react").then((mod) => mod.default),
    { ssr: false },
);

interface Props {
    value: string;
    onChange: (value: string) => void;
    onRun: () => void;
    loading: boolean;
}

export default function SqlEditor({ value, onChange, onRun, loading }: Props) {
    return (
        <div style={{
            border: "1px solid var(--border)",
            borderRadius: "8px",
            overflow: "hidden",
        }}>
            {/* Toolbar */}
            <div style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "8px 12px",
                background: "var(--surface)",
                borderBottom: "1px solid var(--border)",
            }}>
                <span style={{ color: "var(--muted)", fontSize: 12 }}>
                    SQL Editor — {"\u2318"}Enter to run
                </span>
                <button
                    onClick={onRun}
                    disabled={loading}
                    style={{
                        background: loading ? "var(--border)" : "var(--blue)",
                        color: "#fff",
                        border: "none",
                        borderRadius: "6px",
                        padding: "5px 16px",
                        fontSize: 13,
                        cursor: loading ? "not-allowed" : "pointer",
                        fontWeight: 500,
                    }}
                >
                    {loading ? "Running…" : "Run"}
                </button>
            </div>

            {/* Monaco */}
            <MonacoEditor
                height="180px"
                language="sql"
                theme="vs-dark"
                value={value}
                onChange={(v) => onChange(v ?? "")}
                onMount={(editor, monaco) => {
                    // Cmd+Enter / Ctrl+Enter runs the query
                    editor.addCommand(
                        monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                        onRun,
                    );
                }}
                options={{
                    fontSize: 14,
                    minimap: { enabled: false },
                    lineNumbers: "off",
                    scrollBeyondLastLine: false,
                    wordWrap: "on",
                    padding: { top: 12, bottom: 12 },
                    renderLineHighlight: "none",
                    overviewRulerLanes: 0,
                }}
            />
        </div>
    );
}
