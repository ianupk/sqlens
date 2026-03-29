"use client";

import { useEffect, useRef } from "react";
import * as d3 from "d3";
import type { PlanNode } from "@/lib/api";

interface Props {
    planTree: PlanNode[];
    summary: string;
}

const SEVERITY_COLOR: Record<string, string> = {
    slow: "#ef4444",
    warn: "#f59e0b",
    ok: "#22c55e",
};

const NODE_W = 200;
const NODE_H = 64;
const GAP_X = 40;
const GAP_Y = 100;

export default function PlanTree({ planTree, summary }: Props) {
    const svgRef = useRef<SVGSVGElement>(null);

    useEffect(() => {
        if (!svgRef.current || planTree.length === 0) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll("*").remove();

        // Build a synthetic root if SQLite returns multiple flat nodes
        const rootData: PlanNode =
            planTree.length === 1
                ? planTree[0]
                : {
                    node_type: "Query",
                    severity: "ok",
                    reason: "",
                    total_cost: null,
                    plan_rows: null,
                    actual_rows: null,
                    relation_name: null,
                    index_name: null,
                    is_scan: false,
                    children: planTree,
                };

        // D3 hierarchy
        const root = d3.hierarchy<PlanNode>(rootData, (d) => d.children);

        // Tree layout — we'll compute dimensions after layout
        const treeLayout = d3.tree<PlanNode>()
            .nodeSize([NODE_W + GAP_X, NODE_H + GAP_Y]);

        treeLayout(root);

        // Get bounds
        let minX = Infinity, maxX = -Infinity;
        let minY = Infinity, maxY = -Infinity;
        const pointRoot = root as d3.HierarchyPointNode<PlanNode>;
        pointRoot.each((d) => {
            minX = Math.min(minX, d.x);
            maxX = Math.max(maxX, d.x);
            minY = Math.min(minY, d.y);
            maxY = Math.max(maxY, d.y);
        });

        const padding = 40;
        const width = (maxX - minX) + NODE_W + padding * 2;
        const height = (maxY - minY) + NODE_H + padding * 2;
        const offsetX = -minX + NODE_W / 2 + padding;
        const offsetY = -minY + padding;

        svg
            .attr("width", width)
            .attr("height", height)
            .attr("viewBox", `0 0 ${width} ${height}`);

        const g = svg.append("g")
            .attr("transform", `translate(${offsetX},${offsetY})`);

        // Links
        g.selectAll("path.link")
            .data(root.links())
            .join("path")
            .attr("class", "link")
            .attr("fill", "none")
            .attr("stroke", "#2a2d3a")
            .attr("stroke-width", 1.5)
            .attr("d", d3.linkVertical<d3.HierarchyPointLink<PlanNode>, d3.HierarchyPointNode<PlanNode>>()
                    .x((d) => d.x)
                    .y((d) => d.y) as any
            );

        // Node groups
        const node = g.selectAll("g.node")
            .data(root.descendants())
            .join("g")
            .attr("class", "node")
            .attr("transform", (d: any) =>
                `translate(${d.x - NODE_W / 2},${d.y})`
            );

        // Node background rect
        node.append("rect")
            .attr("width", NODE_W)
            .attr("height", NODE_H)
            .attr("rx", 8)
            .attr("fill", "#1a1d27")
            .attr("stroke", (d) => SEVERITY_COLOR[d.data.severity] ?? "#2a2d3a")
            .attr("stroke-width", 1.5);

        // Severity dot
        node.append("circle")
            .attr("cx", 14)
            .attr("cy", NODE_H / 2)
            .attr("r", 5)
            .attr("fill", (d) => SEVERITY_COLOR[d.data.severity] ?? "#64748b");

        // Node type label
        node.append("text")
            .attr("x", 26)
            .attr("y", NODE_H / 2 - 8)
            .attr("dominant-baseline", "central")
            .attr("fill", "#e2e8f0")
            .attr("font-size", 12)
            .attr("font-weight", 500)
            .text((d) => d.data.node_type);

        // Relation name or index name (subtitle)
        node.append("text")
            .attr("x", 26)
            .attr("y", NODE_H / 2 + 10)
            .attr("dominant-baseline", "central")
            .attr("fill", "#64748b")
            .attr("font-size", 11)
            .text((d) => {
                if (d.data.relation_name) return `table: ${d.data.relation_name}`;
                if (d.data.index_name) return `idx: ${d.data.index_name}`;
                if (d.data.total_cost != null) return `cost: ${d.data.total_cost.toFixed(1)}`;
                return "";
            });

        // Tooltip on hover — show reason
        node.append("title")
            .text((d) => d.data.reason || d.data.node_type);

    }, [planTree]);

    if (planTree.length === 0) return null;

    return (
        <div>
            {/* Summary bar */}
            <div style={{
                background: "var(--surface)",
                border: "1px solid var(--border)",
                borderRadius: "8px 8px 0 0",
                padding: "10px 14px",
                fontSize: 13,
                color: "var(--text)",
                borderBottom: "none",
            }}>
                <strong style={{ marginRight: 8 }}>Plan summary:</strong>
                {summary}
            </div>

            {/* Legend */}
            <div style={{
                display: "flex",
                gap: 16,
                padding: "8px 14px",
                background: "var(--surface)",
                borderLeft: "1px solid var(--border)",
                borderRight: "1px solid var(--border)",
                borderBottom: "1px solid var(--border)",
            }}>
                {[
                    { label: "Slow", color: "#ef4444" },
                    { label: "Warning", color: "#f59e0b" },
                    { label: "OK", color: "#22c55e" },
                ].map(({ label, color }) => (
                    <div key={label} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <div style={{
                            width: 10, height: 10,
                            borderRadius: "50%",
                            background: color,
                        }} />
                        <span style={{ fontSize: 11, color: "var(--muted)" }}>{label}</span>
                    </div>
                ))}
                <span style={{ fontSize: 11, color: "var(--muted)", marginLeft: "auto" }}>
                    Hover a node for details
                </span>
            </div>

            {/* SVG tree */}
            <div style={{
                overflowX: "auto",
                background: "#0f1117",
                border: "1px solid var(--border)",
                borderTop: "none",
                borderRadius: "0 0 8px 8px",
                minHeight: 200,
            }}>
                <svg ref={svgRef} style={{ display: "block" }} />
            </div>
        </div>
    );
}
