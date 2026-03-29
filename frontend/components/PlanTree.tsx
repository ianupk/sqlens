"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import * as d3 from "d3";
import type { PlanNode } from "@/lib/api";

interface Props {
    planTree: PlanNode[];
    summary: string;
}

/* ── Severity-based dark node colors ── */
const SEV_COLORS: Record<string, { bg: string; border: string }> = {
    slow:  { bg: "#6b2020", border: "#8b3030" },   // dark red
    warn:  { bg: "#7a6210", border: "#9a7d18" },   // dark yellow (matches #eab308 dot)
    ok:    { bg: "#1a4a2e", border: "#25603c" },   // dark green
};

/* Special color for root Query node */
const QUERY_COLOR = { bg: "#1e3a5f", border: "#2a5080" }; // dark sky blue

const SEV_LABEL: Record<string, string> = { slow: "SLOW", warn: "WARN", ok: "OK" };

/* ── Card geometry ── */
const W = 220;
const TITLE_H = 36;
const ROW_H = 28;
const FIELD_H = 30;
const PAD = 12;
const DOT_R = 7;
const GAP_X = 80;
const GAP_Y = 20;
const ANIM = 400;

/** Compute dynamic height based on how many rows a node has */
function nodeHeight(d: PlanNode): number {
    let rows = 0;
    if (d.relation_name) rows++;
    if (d.index_name) rows++;
    if (d.total_cost != null) rows++;
    if (d.plan_rows != null) rows++;
    return TITLE_H + Math.max(rows, 1) * ROW_H + PAD;
}

/* Use a fixed H for tree layout spacing (max possible) */
const H_LAYOUT = TITLE_H + 4 * ROW_H + PAD;

function categorize(nodeType: string): string {
    const t = nodeType.toUpperCase();
    if (t.includes("SEARCH") || t.includes("INDEX SCAN") || t.includes("USING INDEX")) return "search";
    if (t.includes("SCAN") || t.includes("SEQ SCAN") || t.includes("TABLE") || t.includes("FULL")) return "scan";
    if (t.includes("SORT") || t.includes("ORDER")) return "sort";
    if (t.includes("HASH")) return "hash";
    if (t.includes("JOIN") || t.includes("NESTED") || t.includes("MERGE") || t.includes("LOOP")) return "join";
    if (t.includes("AGGREGATE") || t.includes("GROUP")) return "aggregate";
    if (t.includes("FILTER") || t.includes("WHERE")) return "filter";
    if (t.includes("MATERIALIZE") || t.includes("TEMP") || t.includes("B-TREE")) return "materialize";
    if (t.includes("LIMIT") || t.includes("FIRST")) return "limit";
    if (t.includes("COMPOUND") || t.includes("UNION")) return "compound";
    return "default";
}

interface TreeNode extends d3.HierarchyPointNode<PlanNode> {
    _children?: TreeNode[];
    x0?: number;
    y0?: number;
}

export default function PlanTree({ planTree, summary }: Props) {
    const containerRef = useRef<HTMLDivElement>(null);
    const svgRef = useRef<SVGSVGElement>(null);
    const gRef = useRef<SVGGElement | null>(null);
    const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
    const [selected, setSelected] = useState<PlanNode | null>(null);
    const [zoomLevel, setZoomLevel] = useState(100);

    const resetZoom = useCallback(() => {
        if (!svgRef.current || !zoomRef.current || !gRef.current) return;
        const svg = d3.select(svgRef.current);
        const container = containerRef.current;
        if (!container) return;
        const bounds = gRef.current.getBBox();
        const cw = container.clientWidth;
        const ch = container.clientHeight;
        const pad = 80;
        const scale = Math.min(cw / (bounds.width + pad * 2), ch / (bounds.height + pad * 2), 1.2);
        const tx = pad - bounds.x * scale + (cw - (bounds.width * scale)) / 2 - pad;
        const ty = ch / 2 - (bounds.y + bounds.height / 2) * scale;
        svg.transition().duration(500).call(
            zoomRef.current.transform as any,
            d3.zoomIdentity.translate(tx, ty).scale(scale),
        );
    }, []);

    useEffect(() => {
        if (!svgRef.current || !containerRef.current || planTree.length === 0) return;
        const svg = d3.select(svgRef.current);
        svg.selectAll("*").remove();

        const rootData: PlanNode =
            planTree.length === 1
                ? planTree[0]
                : { node_type: "Query", severity: "ok", reason: "", total_cost: null, plan_rows: null, actual_rows: null, relation_name: null, index_name: null, is_scan: false, children: planTree };

        const root = d3.hierarchy<PlanNode>(rootData, (d) => d.children) as TreeNode;
        const treeLayout = d3.tree<PlanNode>()
            .nodeSize([H_LAYOUT + GAP_Y, W + GAP_X])
            .separation((a, b) => (a.parent === b.parent ? 1 : 1.4));
        treeLayout(root as any);
        root.each((d: any) => { d.x0 = d.x; d.y0 = d.y; });

        const cw = containerRef.current.clientWidth;
        const ch = containerRef.current.clientHeight;
        svg.attr("width", cw).attr("height", ch);

        const zoom = d3.zoom<SVGSVGElement, unknown>()
            .scaleExtent([0.1, 3])
            .on("zoom", (event) => {
                g.attr("transform", event.transform);
                setZoomLevel(Math.round(event.transform.k * 100));
            });
        svg.call(zoom);
        zoomRef.current = zoom;

        const g = svg.append("g");
        gRef.current = g.node();

        const defs = svg.append("defs");

        // Drop shadow
        const sf = defs.append("filter").attr("id", "sh")
            .attr("x", "-8%").attr("y", "-8%").attr("width", "116%").attr("height", "124%");
        sf.append("feDropShadow").attr("dx", 0).attr("dy", 3).attr("stdDeviation", 6)
            .attr("flood-color", "#000").attr("flood-opacity", 0.25);

        /* ── Smooth bezier link ── */
        function linkPath(d: any) {
            const sh = nodeHeight(d.source.data);
            const sx = d.source.y + W;       // right edge of source
            const sy = d.source.x;            // center of source
            const tx = d.target.y;            // left edge of target
            const ty = d.target.x;            // center of target
            const mx = (sx + tx) / 2;
            return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
        }

        /* ── Build property rows for a node ── */
        function getRows(d: PlanNode): { label: string; value: string }[] {
            const r: { label: string; value: string }[] = [];
            if (d.relation_name) r.push({ label: "Table", value: d.relation_name });
            if (d.index_name) r.push({ label: "Index", value: d.index_name });
            if (d.total_cost != null) r.push({ label: "Cost", value: d.total_cost.toFixed(1) });
            if (d.plan_rows != null) r.push({ label: "Rows", value: d.plan_rows.toLocaleString() });
            if (r.length === 0) r.push({ label: "Severity", value: SEV_LABEL[d.severity] ?? "OK" });
            return r;
        }

        function update(source: TreeNode) {
            treeLayout(root as any);
            const nodes = root.descendants() as TreeNode[];
            const links = root.links();

            // ── Links ──
            const link = g.selectAll<SVGPathElement, d3.HierarchyPointLink<PlanNode>>("path.lnk")
                .data(links, (d: any) => `${d.target.data.node_type}-${d.target.depth}-${d.target.data.relation_name ?? ""}`);

            const linkE = link.enter().append("path")
                .attr("class", "lnk")
                .attr("fill", "none")
                .attr("stroke", "#4a5878")
                .attr("stroke-width", 3)
                .attr("stroke-linecap", "round")
                .attr("d", () => {
                    const o = { x: source.x0 ?? source.x, y: source.y0 ?? source.y, data: source.data };
                    return linkPath({ source: o, target: o });
                });
            linkE.merge(link as any)
                .transition().duration(ANIM).ease(d3.easeCubicInOut)
                .attr("d", (d: any) => linkPath(d));
            link.exit().transition().duration(ANIM).attr("opacity", 0).remove();

            // ── Nodes ──
            const node = g.selectAll<SVGGElement, TreeNode>("g.nd")
                .data(nodes, (d: any) => `${d.data.node_type}-${d.depth}-${d.data.relation_name ?? ""}`);

            const nodeE = node.enter().append("g")
                .attr("class", "nd")
                .attr("transform", `translate(${source.y0 ?? source.y},${(source.x0 ?? source.x) - H_LAYOUT / 2})`)
                .attr("opacity", 0)
                .style("cursor", "pointer")
                .on("click", (_: any, d: TreeNode) => {
                    if (d.children) { (d as any)._children = d.children; (d as any).children = null; }
                    else if ((d as any)._children) { d.children = (d as any)._children; (d as any)._children = null; }
                    setSelected(d.data);
                    update(d);
                });

            // Draw each node card
            nodeE.each(function (d) {
                const el = d3.select(this);
                const isRoot = d.depth === 0 && d.data.node_type === "Query";
                const colors = isRoot ? QUERY_COLOR : (SEV_COLORS[d.data.severity] ?? SEV_COLORS.ok);
                const rows = getRows(d.data);
                const h = nodeHeight(d.data);
                const yOff = (H_LAYOUT - h) / 2; // vertical centering

                // Main card background (rounded rect, colored fill)
                el.append("rect")
                    .attr("class", "nd-bg")
                    .attr("x", 0).attr("y", yOff)
                    .attr("width", W).attr("height", h)
                    .attr("rx", 12)
                    .attr("fill", colors.bg)
                    .attr("stroke", colors.border)
                    .attr("stroke-width", 2)
                    .attr("filter", "url(#sh)");

                // Title text (bold, white, top-left)
                el.append("text")
                    .attr("x", PAD + 2).attr("y", yOff + TITLE_H / 2 + 2)
                    .attr("dominant-baseline", "central")
                    .attr("fill", "#fff")
                    .attr("font-size", "15px")
                    .attr("font-weight", "700")
                    .attr("font-family", "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif")
                    .text(() => {
                        const t = d.data.node_type;
                        return t.length > 18 ? t.slice(0, 17) + "…" : t;
                    });

                // Severity indicator (small pill in top right)
                const sevLabel = SEV_LABEL[d.data.severity] ?? "OK";
                const sevW = sevLabel.length * 7 + 12;
                el.append("rect")
                    .attr("x", W - sevW - PAD).attr("y", yOff + (TITLE_H - 20) / 2)
                    .attr("width", sevW).attr("height", 20)
                    .attr("rx", 10)
                    .attr("fill", "rgba(255,255,255,0.2)");
                el.append("text")
                    .attr("x", W - sevW / 2 - PAD).attr("y", yOff + TITLE_H / 2 + 1)
                    .attr("text-anchor", "middle")
                    .attr("dominant-baseline", "central")
                    .attr("fill", "#fff")
                    .attr("font-size", "9px")
                    .attr("font-weight", "700")
                    .attr("letter-spacing", "0.06em")
                    .attr("font-family", "-apple-system, sans-serif")
                    .text(sevLabel);

                // Property rows
                rows.forEach((row, i) => {
                    const ry = yOff + TITLE_H + i * ROW_H + 2;

                    // Label text
                    el.append("text")
                        .attr("x", PAD + 4).attr("y", ry + ROW_H / 2)
                        .attr("dominant-baseline", "central")
                        .attr("fill", "rgba(255,255,255,0.7)")
                        .attr("font-size", "11px")
                        .attr("font-family", "-apple-system, sans-serif")
                        .text(row.label);

                    // White rounded value field
                    const fieldX = PAD + 4;
                    const fieldW = W - PAD * 2 - 8;
                    // Only show field if there's a value
                    if (row.value) {
                        el.append("rect")
                            .attr("x", 60).attr("y", ry + 3)
                            .attr("width", W - 60 - PAD - 2).attr("height", ROW_H - 6)
                            .attr("rx", 6)
                            .attr("fill", "rgba(255,255,255,0.12)")
                            .attr("stroke", "rgba(255,255,255,0.1)")
                            .attr("stroke-width", 0.5);
                        el.append("text")
                            .attr("x", 66).attr("y", ry + ROW_H / 2)
                            .attr("dominant-baseline", "central")
                            .attr("fill", "#e2e8f0")
                            .attr("font-size", "11px")
                            .attr("font-weight", "500")
                            .attr("font-family", "'SF Mono', 'Fira Code', 'Menlo', monospace")
                            .text(() => {
                                const v = row.value;
                                return v.length > 16 ? v.slice(0, 15) + "…" : v;
                            });
                    }
                });

                // Output connector dot (right edge, centered)
                el.append("circle")
                    .attr("cx", W).attr("cy", H_LAYOUT / 2)
                    .attr("r", DOT_R)
                    .attr("fill", "#8faa6e")
                    .attr("stroke", "#6d8a50")
                    .attr("stroke-width", 2);

                // Input connector dot (left edge, centered) — only if not root
                if (d.depth > 0) {
                    el.append("circle")
                        .attr("cx", 0).attr("cy", H_LAYOUT / 2)
                        .attr("r", DOT_R)
                        .attr("fill", "#8faa6e")
                        .attr("stroke", "#6d8a50")
                        .attr("stroke-width", 2);
                }
            });

            // Merge + update positions
            const nodeU = nodeE.merge(node as any);
            nodeU.transition().duration(ANIM).ease(d3.easeCubicInOut)
                .attr("transform", (d: any) => `translate(${d.y},${d.x - H_LAYOUT / 2})`)
                .attr("opacity", 1);

            node.exit().transition().duration(ANIM)
                .attr("transform", `translate(${source.y},${source.x - H_LAYOUT / 2})`)
                .attr("opacity", 0).remove();

            nodes.forEach((d: any) => { d.x0 = d.x; d.y0 = d.y; });
        }

        update(root);

        // Auto-fit
        requestAnimationFrame(() => {
            if (!gRef.current || !containerRef.current) return;
            const bounds = gRef.current.getBBox();
            const pad = 80;
            const scale = Math.min(cw / (bounds.width + pad * 2), ch / (bounds.height + pad * 2), 1.2);
            const tx = pad - bounds.x * scale + (cw - (bounds.width * scale)) / 2 - pad;
            const ty = ch / 2 - (bounds.y + bounds.height / 2) * scale;
            svg.transition().duration(700).call(
                zoom.transform as any,
                d3.zoomIdentity.translate(tx, ty).scale(scale),
            );
        });

        const resizeObs = new ResizeObserver(() => {
            if (!containerRef.current) return;
            svg.attr("width", containerRef.current.clientWidth)
                .attr("height", containerRef.current.clientHeight);
        });
        resizeObs.observe(containerRef.current);
        return () => resizeObs.disconnect();
    }, [planTree]);

    if (planTree.length === 0) return null;

    return (
        <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
            {/* Summary + controls bar */}
            <div style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "8px 14px", flexWrap: "wrap", gap: "10px",
                background: "#1a1d27", border: "1px solid #2a2d3a",
                borderRadius: "8px 8px 0 0",
            }}>
                <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                    <span style={{ color: "#64748b", fontSize: "11px", fontWeight: 600, textTransform: "uppercase" as const, letterSpacing: "0.04em" }}>
                        Plan:
                    </span>
                    <span style={{ color: "#c8d0e0", fontSize: "12px" }}>{summary}</span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: "16px" }}>
                    {[
                        { l: "Slow", c: "#ef4444" },
                        { l: "Warn", c: "#eab308" },
                        { l: "OK", c: "#22c55e" },
                    ].map(({ l, c }) => (
                        <div key={l} style={{ display: "flex", alignItems: "center", gap: "5px" }}>
                            <div style={{ width: 8, height: 8, borderRadius: "50%", background: c }} />
                            <span style={{ fontSize: "10px", color: "#8892a8" }}>{l}</span>
                        </div>
                    ))}
                    <span style={{ fontSize: "10px", color: "#64748b", fontFamily: "monospace" }}>{zoomLevel}%</span>
                    <button onClick={resetZoom} style={{
                        background: "#2a2d3a", border: "1px solid #3a3e4a", borderRadius: 5,
                        color: "#e2e8f0", padding: "3px 10px", fontSize: "10px", cursor: "pointer",
                        display: "flex", alignItems: "center", gap: "4px",
                    }}>
                        <svg width="10" height="10" viewBox="0 0 16 16" fill="currentColor">
                            <path d="M2 2v4h1.5V3.5H6V2H2zm8 0v1.5h2.5V6H14V2h-4zM3.5 10.5V12.5H6V14H2v-4h1.5zm9 0V14h-4v-1.5h2.5v-2H14z" />
                        </svg>
                        Fit
                    </button>
                    <span style={{ fontSize: "9px", color: "#64748b", opacity: 0.6 }}>Scroll zoom · Drag pan · Click for details</span>
                </div>
            </div>

            {/* Canvas */}
            <div
                ref={containerRef}
                style={{
                    flex: 1, minHeight: 200, overflow: "hidden", cursor: "grab",
                    background: "#131720",
                    border: "1px solid #2a2d3a", borderTop: "none",
                    borderRadius: "0 0 8px 8px", position: "relative",
                }}
                onMouseDown={() => {
                    const el = containerRef.current;
                    if (el) el.style.cursor = "grabbing";
                }}
                onMouseUp={() => {
                    const el = containerRef.current;
                    if (el) el.style.cursor = "grab";
                }}
            >
                <svg ref={svgRef} style={{ display: "block", width: "100%", height: "100%", position: "relative", zIndex: 1 }} />
            </div>

            {/* Detail modal */}
            {selected && (
                <div
                    onClick={() => setSelected(null)}
                    style={{
                        position: "fixed", inset: 0, zIndex: 1000,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        background: "rgba(0,0,0,0.45)", backdropFilter: "blur(4px)",
                    }}
                >
                    <div
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            width: 380, maxWidth: "90vw", borderRadius: 14, overflow: "hidden",
                            boxShadow: "0 20px 60px rgba(0,0,0,0.4)",
                            background: (selected.node_type === "Query" ? QUERY_COLOR : (SEV_COLORS[selected.severity] ?? SEV_COLORS.ok)).bg,
                        }}
                    >
                        {/* Modal header */}
                        <div style={{
                            padding: "14px 18px",
                            display: "flex", alignItems: "center", justifyContent: "space-between",
                        }}>
                            <span style={{ fontWeight: 700, fontSize: 18, color: "#fff" }}>{selected.node_type}</span>
                            <button onClick={() => setSelected(null)} style={{
                                background: "rgba(255,255,255,0.2)", border: "none", color: "#fff",
                                width: 26, height: 26, borderRadius: 6, fontSize: 16, cursor: "pointer",
                                display: "flex", alignItems: "center", justifyContent: "center",
                            }}>×</button>
                        </div>
                        {/* Modal body */}
                        <div style={{ background: "rgba(255,255,255,0.08)", padding: "12px 18px 18px" }}>
                            {selected.reason && (
                                <div style={{
                                    background: "rgba(0,0,0,0.2)", borderRadius: 8,
                                    padding: "10px 12px", fontSize: 12, color: "#ffe4e4",
                                    marginBottom: 14, lineHeight: 1.5,
                                }}>
                                    {selected.reason}
                                </div>
                            )}
                            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                                {selected.relation_name && <MRow label="Table" value={selected.relation_name} />}
                                {selected.index_name && <MRow label="Index" value={selected.index_name} />}
                                {selected.total_cost != null && <MRow label="Total Cost" value={selected.total_cost.toFixed(2)} />}
                                {selected.plan_rows != null && <MRow label="Est. Rows" value={selected.plan_rows.toLocaleString()} />}
                                {selected.actual_rows != null && <MRow label="Actual Rows" value={selected.actual_rows.toLocaleString()} />}
                                <MRow label="Scan" value={selected.is_scan ? "Yes" : "No"} />
                                <MRow label="Severity" value={SEV_LABEL[selected.severity] ?? "OK"} />
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

function MRow({ label, value }: { label: string; value: string }) {
    return (
        <div style={{
            display: "flex", alignItems: "center", justifyContent: "space-between",
            gap: 12,
        }}>
            <span style={{ color: "rgba(255,255,255,0.7)", fontSize: 12 }}>{label}</span>
            <div style={{
                background: "rgba(255,255,255,0.12)", borderRadius: 6,
                padding: "5px 12px", fontSize: 12, fontWeight: 500,
                color: "#e2e8f0", fontFamily: "'SF Mono', 'Fira Code', monospace",
                minWidth: 80, textAlign: "right" as const,
            }}>
                {value}
            </div>
        </div>
    );
}
