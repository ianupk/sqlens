"use client";

import { useRef, useState, useCallback, useEffect } from "react";

interface Props {
    top: React.ReactNode;
    bottom: React.ReactNode;
    initialRatio?: number;
    minSize?: number;
}

/**
 * Resizable vertical split layout.
 * A visible 8px handle with grip dots between the two panels.
 * - Drag to resize
 * - Double-click to reset to default
 * - Visual cursor feedback on hover + drag
 * - Min constraints to prevent collapse
 */
export default function ResizableSplit({
    top,
    bottom,
    initialRatio = 0.35,
    minSize = 80,
}: Props) {
    const wrapRef = useRef<HTMLDivElement>(null);
    const [ratio, setRatio] = useState(initialRatio);
    const [dragging, setDragging] = useState(false);
    const [hovering, setHovering] = useState(false);

    const onPointerDown = useCallback(
        (e: React.PointerEvent) => {
            e.preventDefault();
            (e.target as HTMLElement).setPointerCapture(e.pointerId);
            setDragging(true);
        },
        [],
    );

    const onPointerMove = useCallback(
        (e: React.PointerEvent) => {
            if (!dragging || !wrapRef.current) return;
            const rect = wrapRef.current.getBoundingClientRect();
            const y = e.clientY - rect.top;
            const clamped = Math.max(minSize, Math.min(y, rect.height - minSize));
            setRatio(clamped / rect.height);
        },
        [dragging, minSize],
    );

    const onPointerUp = useCallback(() => {
        setDragging(false);
    }, []);

    // Handle height
    const HANDLE_H = 8;
    const topH = `calc(${ratio * 100}% - ${HANDLE_H / 2}px)`;
    const bottomH = `calc(${(1 - ratio) * 100}% - ${HANDLE_H / 2}px)`;

    const isActive = dragging || hovering;

    return (
        <div
            ref={wrapRef}
            onPointerMove={onPointerMove}
            onPointerUp={onPointerUp}
            style={{
                display: "flex",
                flexDirection: "column",
                height: "100%",
                overflow: "hidden",
                userSelect: dragging ? "none" : undefined,
                cursor: dragging ? "row-resize" : undefined,
            }}
        >
            {/* Top panel */}
            <div style={{
                height: topH,
                overflow: "auto",
                minHeight: minSize,
            }}>
                {top}
            </div>

            {/* Draggable handle */}
            <div
                onPointerDown={onPointerDown}
                onMouseEnter={() => setHovering(true)}
                onMouseLeave={() => setHovering(false)}
                onDoubleClick={() => setRatio(initialRatio)}
                style={{
                    flexShrink: 0,
                    height: HANDLE_H,
                    cursor: "row-resize",
                    position: "relative",
                    zIndex: 50,
                    background: isActive ? "#3b82f6" : "#1e2230",
                    borderTop: `1px solid ${isActive ? "#3b82f6" : "#2a2d3a"}`,
                    borderBottom: `1px solid ${isActive ? "#3b82f6" : "#2a2d3a"}`,
                    transition: "background 0.15s, border-color 0.15s",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                }}
            >
                {/* Grip dots — 3 dots in a row */}
                <div style={{
                    display: "flex",
                    gap: 4,
                    opacity: isActive ? 1 : 0.35,
                    transition: "opacity 0.15s",
                }}>
                    {[0, 1, 2, 3, 4].map((i) => (
                        <div
                            key={i}
                            style={{
                                width: 3,
                                height: 3,
                                borderRadius: "50%",
                                background: isActive ? "#fff" : "#6b7280",
                            }}
                        />
                    ))}
                </div>

                {/* Wider invisible hit target */}
                <div style={{
                    position: "absolute",
                    left: 0,
                    right: 0,
                    top: -6,
                    bottom: -6,
                    cursor: "row-resize",
                }} />
            </div>

            {/* Bottom panel */}
            <div style={{
                height: bottomH,
                overflow: "auto",
                minHeight: minSize,
            }}>
                {bottom}
            </div>
        </div>
    );
}
