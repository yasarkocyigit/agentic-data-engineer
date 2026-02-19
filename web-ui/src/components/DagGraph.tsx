
import React, { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { getStateColor } from '@/lib/airflow/types';
import type { PipelineGraph, GraphNode, GraphEdge } from '@/lib/airflow/types';
import {
    Loader2, RefreshCw, ZoomIn, ZoomOut, Maximize2,
    GitBranch
} from 'lucide-react';
import clsx from 'clsx';

// ─── Layout Constants ───
const NODE_W = 200;
const NODE_H = 64;
const LAYER_GAP_X = 80;
const NODE_GAP_Y = 28;
const PAD = 40;

// ─── Types ───
interface LayoutNode extends GraphNode {
    x: number;
    y: number;
    layer: number;
}

interface DagGraphProps {
    dagId: string;
    runId?: string | null;
    className?: string;
    onNodeClick?: (nodeId: string) => void;
}

// ─── Topological layering ───
function computeLayers(nodes: GraphNode[], edges: GraphEdge[]): Map<string, number> {
    const adj = new Map<string, string[]>();
    const inDeg = new Map<string, number>();

    for (const n of nodes) {
        adj.set(n.id, []);
        inDeg.set(n.id, 0);
    }
    for (const e of edges) {
        adj.get(e.source)?.push(e.target);
        inDeg.set(e.target, (inDeg.get(e.target) || 0) + 1);
    }

    const layers = new Map<string, number>();
    let queue = nodes.filter(n => (inDeg.get(n.id) || 0) === 0).map(n => n.id);
    let layer = 0;

    while (queue.length > 0) {
        const next: string[] = [];
        for (const id of queue) {
            layers.set(id, layer);
            for (const child of (adj.get(id) || [])) {
                const deg = (inDeg.get(child) || 1) - 1;
                inDeg.set(child, deg);
                if (deg === 0) next.push(child);
            }
        }
        queue = next;
        layer++;
    }

    // Fallback: assign unlayered nodes to last layer
    for (const n of nodes) {
        if (!layers.has(n.id)) layers.set(n.id, layer);
    }

    return layers;
}

// ─── Assign x/y positions ───
function layoutGraph(nodes: GraphNode[], edges: GraphEdge[]): LayoutNode[] {
    const layers = computeLayers(nodes, edges);
    const maxLayer = Math.max(...layers.values(), 0);

    // Group nodes by layer
    const buckets: GraphNode[][] = Array.from({ length: maxLayer + 1 }, () => []);
    for (const n of nodes) {
        buckets[layers.get(n.id) || 0].push(n);
    }

    const result: LayoutNode[] = [];

    for (let l = 0; l <= maxLayer; l++) {
        const group = buckets[l];
        const totalH = group.length * NODE_H + (group.length - 1) * NODE_GAP_Y;
        const startY = -totalH / 2;

        group.forEach((n, i) => {
            result.push({
                ...n,
                x: PAD + l * (NODE_W + LAYER_GAP_X),
                y: startY + i * (NODE_H + NODE_GAP_Y),
                layer: l,
            });
        });
    }

    return result;
}

// ─── Edge path with smooth bezier ───
function edgePath(
    src: LayoutNode,
    tgt: LayoutNode
): string {
    const x1 = src.x + NODE_W;
    const y1 = src.y + NODE_H / 2;
    const x2 = tgt.x;
    const y2 = tgt.y + NODE_H / 2;
    const cpx = (x1 + x2) / 2;

    return `M ${x1} ${y1} C ${cpx} ${y1}, ${cpx} ${y2}, ${x2} ${y2}`;
}

// ─── Operator label ───
function shortOperator(op: string): string {
    // SparkSubmitOperator → Spark Submit
    // PythonOperator → Python
    return op
        .replace(/Operator$/, '')
        .replace(/([A-Z])/g, ' $1')
        .trim();
}

// ─── Component ───
export default function DagGraph({ dagId, runId, className, onNodeClick }: DagGraphProps) {
    const [graph, setGraph] = useState<PipelineGraph | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [hoveredNode, setHoveredNode] = useState<string | null>(null);
    const [zoom, setZoom] = useState(1);
    const [pan, setPan] = useState({ x: 0, y: 0 });
    const [isPanning, setIsPanning] = useState(false);
    const panStart = useRef({ x: 0, y: 0, panX: 0, panY: 0 });
    const containerRef = useRef<HTMLDivElement>(null);

    // ─── Fetch graph structure ───
    const fetchGraph = useCallback(async () => {
        try {
            setLoading(true);
            const params = new URLSearchParams();
            if (runId) params.set('run_id', runId);
            const res = await fetch(`/api/orchestrator/dags/${dagId}/structure?${params}`);
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            setGraph(data);
            setError(null);
        } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : 'Failed to load graph';
            setError(msg);
        } finally {
            setLoading(false);
        }
    }, [dagId, runId]);

    useEffect(() => {
        fetchGraph();
        const interval = setInterval(fetchGraph, 15000); // Poll every 15s
        return () => clearInterval(interval);
    }, [fetchGraph]);

    // ─── Layout ───
    const layoutNodes = useMemo(() => {
        if (!graph) return [];
        return layoutGraph(graph.nodes, graph.edges);
    }, [graph]);

    const nodeMap = useMemo(() => {
        const m = new Map<string, LayoutNode>();
        for (const n of layoutNodes) m.set(n.id, n);
        return m;
    }, [layoutNodes]);

    // Viewbox
    const viewBox = useMemo(() => {
        if (layoutNodes.length === 0) return { minX: 0, minY: 0, w: 800, h: 400 };
        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        for (const n of layoutNodes) {
            if (n.x < minX) minX = n.x;
            if (n.y < minY) minY = n.y;
            if (n.x + NODE_W > maxX) maxX = n.x + NODE_W;
            if (n.y + NODE_H > maxY) maxY = n.y + NODE_H;
        }
        return {
            minX: minX - PAD,
            minY: minY - PAD,
            w: maxX - minX + PAD * 2,
            h: maxY - minY + PAD * 2,
        };
    }, [layoutNodes]);

    // ─── Mouse controls ───
    const handleWheel = useCallback((e: React.WheelEvent) => {
        e.preventDefault();
        const delta = e.deltaY > 0 ? -0.1 : 0.1;
        setZoom(z => Math.max(0.3, Math.min(3, z + delta)));
    }, []);

    const handleMouseDown = useCallback((e: React.MouseEvent) => {
        if (e.button !== 0) return;
        setIsPanning(true);
        panStart.current = { x: e.clientX, y: e.clientY, panX: pan.x, panY: pan.y };
    }, [pan]);

    const handleMouseMove = useCallback((e: React.MouseEvent) => {
        if (!isPanning) return;
        const dx = e.clientX - panStart.current.x;
        const dy = e.clientY - panStart.current.y;
        setPan({ x: panStart.current.panX + dx, y: panStart.current.panY + dy });
    }, [isPanning]);

    const handleMouseUp = useCallback(() => {
        setIsPanning(false);
    }, []);

    const resetView = useCallback(() => {
        setZoom(1);
        setPan({ x: 0, y: 0 });
    }, []);

    // ─── Render ───
    if (loading && !graph) {
        return (
            <div className={clsx("flex items-center justify-center h-full", className)}>
                <div className="flex flex-col items-center gap-2">
                    <Loader2 className="w-6 h-6 text-obsidian-info animate-spin" />
                    <span className="text-[11px] text-obsidian-muted">Loading graph...</span>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className={clsx("flex items-center justify-center h-full", className)}>
                <div className="flex flex-col items-center gap-2 text-center">
                    <span className="text-[12px] text-obsidian-danger">{error}</span>
                    <button
                        onClick={fetchGraph}
                        className="px-3 py-1 bg-obsidian-info/20 text-obsidian-info rounded text-[11px] hover:bg-obsidian-info/30"
                    >
                        Retry
                    </button>
                </div>
            </div>
        );
    }

    if (!graph || layoutNodes.length === 0) {
        return (
            <div className={clsx("flex items-center justify-center h-full text-obsidian-muted text-[11px]", className)}>
                No tasks found
            </div>
        );
    }

    const healthySummary = layoutNodes.filter(n => n.state === 'success').length;
    const failedSummary = layoutNodes.filter(n => n.state === 'failed').length;
    const runningSummary = layoutNodes.filter(n => n.state === 'running').length;

    return (
        <div className={clsx("flex flex-col h-full bg-obsidian-bg relative", className)}>
            {/* Toolbar */}
            <div className="flex items-center justify-between px-3 py-1.5 bg-obsidian-panel border-b border-obsidian-border shrink-0">
                <div className="flex items-center gap-2">
                    <GitBranch className="w-3.5 h-3.5 text-obsidian-info" />
                    <span className="text-[10px] text-obsidian-muted uppercase font-semibold tracking-wider">
                        Lineage Graph
                    </span>
                    <span className="text-[10px] text-obsidian-muted/80 ml-1">
                        {layoutNodes.length} tasks · {graph.edges.length} edges
                    </span>
                </div>
                <div className="flex items-center gap-1">
                    {runId && (
                        <div className="flex items-center gap-1.5 mr-2 text-[10px]">
                            {healthySummary > 0 && <span className="text-obsidian-success">✓ {healthySummary}</span>}
                            {failedSummary > 0 && <span className="text-obsidian-danger">✗ {failedSummary}</span>}
                            {runningSummary > 0 && <span className="text-obsidian-info">● {runningSummary}</span>}
                        </div>
                    )}
                    <button
                        onClick={() => setZoom(z => Math.min(3, z + 0.2))}
                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground"
                        title="Zoom in"
                    >
                        <ZoomIn className="w-3.5 h-3.5" />
                    </button>
                    <button
                        onClick={() => setZoom(z => Math.max(0.3, z - 0.2))}
                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground"
                        title="Zoom out"
                    >
                        <ZoomOut className="w-3.5 h-3.5" />
                    </button>
                    <button
                        onClick={resetView}
                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground"
                        title="Fit to screen"
                    >
                        <Maximize2 className="w-3.5 h-3.5" />
                    </button>
                    <button
                        onClick={fetchGraph}
                        className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground ml-1"
                        title="Refresh"
                    >
                        <RefreshCw className={clsx("w-3.5 h-3.5", loading && "animate-spin")} />
                    </button>
                </div>
            </div>

            {/* SVG Canvas */}
            <div
                ref={containerRef}
                className="flex-1 overflow-hidden cursor-grab active:cursor-grabbing"
                onWheel={handleWheel}
                onMouseDown={handleMouseDown}
                onMouseMove={handleMouseMove}
                onMouseUp={handleMouseUp}
                onMouseLeave={handleMouseUp}
            >
                <svg
                    width="100%"
                    height="100%"
                    viewBox={`${viewBox.minX} ${viewBox.minY} ${viewBox.w} ${viewBox.h}`}
                    preserveAspectRatio="xMidYMid meet"
                    style={{
                        transform: `scale(${zoom}) translate(${pan.x / zoom}px, ${pan.y / zoom}px)`,
                        transformOrigin: 'center',
                        transition: isPanning ? 'none' : 'transform 0.15s ease-out',
                    }}
                >
                    <defs>
                        {/* Arrow marker */}
                        <marker
                            id="arrow"
                            viewBox="0 0 10 10"
                            refX="10"
                            refY="5"
                            markerWidth="8"
                            markerHeight="8"
                            orient="auto-start-reverse"
                        >
                            <path d="M 0 0 L 10 5 L 0 10 Z" fill="var(--color-obsidian-muted)" />
                        </marker>
                        <marker
                            id="arrow-active"
                            viewBox="0 0 10 10"
                            refX="10"
                            refY="5"
                            markerWidth="8"
                            markerHeight="8"
                            orient="auto-start-reverse"
                        >
                            <path d="M 0 0 L 10 5 L 0 10 Z" fill="var(--color-obsidian-info)" />
                        </marker>

                        {/* Glow filter for running nodes */}
                        <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
                            <feGaussianBlur stdDeviation="4" result="blur" />
                            <feMerge>
                                <feMergeNode in="blur" />
                                <feMergeNode in="SourceGraphic" />
                            </feMerge>
                        </filter>

                    </defs>

                    {/* Edges */}
                    {graph.edges.map((edge, i) => {
                        const src = nodeMap.get(edge.source);
                        const tgt = nodeMap.get(edge.target);
                        if (!src || !tgt) return null;

                        const isHovered = hoveredNode === edge.source || hoveredNode === edge.target;
                        const srcState = src.state;
                        // Use variable if possible, or mapping function for SVG
                        // But for now let's rely on mapping helper or fallback
                        // Ideally getStateColor should return CSS vars? 
                        // It currently returns hex. Let's stick to hex from helper for states for now 
                        // as getting vars into JS logic is tricky without a mapper.
                        // But we can replace defaults.
                        const stateColor = srcState ? getStateColor(srcState).text : 'var(--color-obsidian-muted)';

                        return (
                            <g key={`edge-${i}`}>
                                {/* Shadow edge */}
                                <path
                                    d={edgePath(src, tgt)}
                                    fill="none"
                                    stroke={isHovered ? stateColor : 'var(--color-obsidian-border)'}
                                    strokeWidth={isHovered ? 2.5 : 1.5}
                                    strokeOpacity={isHovered ? 0.4 : 0.2}
                                    strokeDasharray={isHovered ? '' : ''}
                                />
                                {/* Main edge */}
                                <path
                                    d={edgePath(src, tgt)}
                                    fill="none"
                                    stroke={isHovered ? stateColor : 'var(--color-obsidian-muted)'}
                                    strokeWidth={isHovered ? 2 : 1.2}
                                    markerEnd={isHovered ? 'url(#arrow-active)' : 'url(#arrow)'}
                                    style={{
                                        transition: 'stroke 0.2s, stroke-width 0.2s',
                                    }}
                                />
                                {/* Animated pulse for running */}
                                {srcState === 'running' && (
                                    <circle r="3" fill="var(--color-obsidian-info)">
                                        <animateMotion
                                            dur="2s"
                                            repeatCount="indefinite"
                                            path={edgePath(src, tgt)}
                                        />
                                    </circle>
                                )}
                            </g>
                        );
                    })}

                    {/* Nodes */}
                    {layoutNodes.map((node) => {
                        const isHovered = hoveredNode === node.id;
                        const stateInfo = getStateColor(node.state);
                        const hasState = !!node.state;

                        const nodeStroke = hasState ? stateInfo.text : 'var(--color-obsidian-border)';
                        const nodeFill = hasState
                            ? stateInfo.bg
                            : isHovered ? 'var(--color-obsidian-panel)' : 'var(--color-obsidian-panel)'; // Use panel for both, maybe slightly different helper for hover?

                        // Override for default idle
                        const finalFill = !hasState && !isHovered ? '#16171a' : nodeFill; // Hard to use var in complex JS logic, sticking to hex for specific fallback or just varStr

                        const glowFilter = node.state === 'running' ? 'url(#glow)' : undefined;

                        return (
                            <g
                                key={node.id}
                                onMouseEnter={() => setHoveredNode(node.id)}
                                onMouseLeave={() => setHoveredNode(null)}
                                onClick={(e) => {
                                    e.stopPropagation();
                                    onNodeClick?.(node.id);
                                }}
                                style={{ cursor: 'pointer' }}
                            >
                                {/* Node background */}
                                <rect
                                    x={node.x}
                                    y={node.y}
                                    width={NODE_W}
                                    height={NODE_H}
                                    rx={8}
                                    ry={8}
                                    fill={!hasState && !isHovered ? 'var(--color-obsidian-panel)' : nodeFill}
                                    stroke={nodeStroke}
                                    strokeWidth={isHovered ? 1.5 : 1}
                                    filter={glowFilter}
                                    style={{
                                        transition: 'fill 0.2s, stroke 0.2s, stroke-width 0.2s',
                                    }}
                                />

                                {/* State indicator dot */}
                                {hasState && (
                                    <circle
                                        cx={node.x + 14}
                                        cy={node.y + NODE_H / 2}
                                        r={4}
                                        fill={stateInfo.text}
                                    >
                                        {node.state === 'running' && (
                                            <animate
                                                attributeName="opacity"
                                                values="1;0.3;1"
                                                dur="1.5s"
                                                repeatCount="indefinite"
                                            />
                                        )}
                                    </circle>
                                )}

                                {/* Task label */}
                                <text
                                    x={node.x + (hasState ? 26 : 14)}
                                    y={node.y + 24}
                                    fill={isHovered ? 'var(--color-foreground)' : 'var(--color-obsidian-muted)'}
                                    fontSize="12"
                                    fontWeight="600"
                                    fontFamily="ui-monospace, monospace"
                                    style={{ transition: 'fill 0.2s' }}
                                >
                                    {node.label}
                                </text>

                                {/* Operator type */}
                                <text
                                    x={node.x + (hasState ? 26 : 14)}
                                    y={node.y + 42}
                                    fill="var(--color-obsidian-muted)"
                                    fillOpacity="0.7"
                                    fontSize="10"
                                    fontFamily="system-ui, sans-serif"
                                >
                                    {shortOperator(node.operator)}
                                </text>

                                {/* Duration (if available) */}
                                {node.duration != null && node.duration > 0 && (
                                    <text
                                        x={node.x + NODE_W - 10}
                                        y={node.y + 42}
                                        fill="var(--color-obsidian-muted)"
                                        fontSize="9"
                                        fontFamily="ui-monospace, monospace"
                                        textAnchor="end"
                                    >
                                        {node.duration < 60
                                            ? `${node.duration.toFixed(1)}s`
                                            : `${(node.duration / 60).toFixed(1)}m`}
                                    </text>
                                )}

                                {/* Trigger rule badge for non-standard rules */}
                                {node.triggerRule !== 'all_success' && (
                                    <g>
                                        <rect
                                            x={node.x + NODE_W - 64}
                                            y={node.y + 6}
                                            width={54}
                                            height={16}
                                            rx={4}
                                            fill="rgba(229, 192, 123, 0.1)"
                                            stroke="rgba(229, 192, 123, 0.2)"
                                            strokeWidth={0.5}
                                        />
                                        <text
                                            x={node.x + NODE_W - 37}
                                            y={node.y + 17}
                                            fill="var(--color-obsidian-warning)"
                                            fontSize="8"
                                            fontFamily="system-ui, sans-serif"
                                            textAnchor="middle"
                                        >
                                            {node.triggerRule.replace(/_/g, ' ')}
                                        </text>
                                    </g>
                                )}
                            </g>
                        );
                    })}

                    {/* Tooltip */}
                    {hoveredNode && (() => {
                        const node = nodeMap.get(hoveredNode);
                        if (!node) return null;
                        const stateInfo = getStateColor(node.state);

                        return (
                            <g>
                                <rect
                                    x={node.x + NODE_W + 8}
                                    y={node.y - 8}
                                    width={170}
                                    height={node.state ? 70 : 52}
                                    rx={6}
                                    fill="var(--color-obsidian-panel)"
                                    stroke="var(--color-obsidian-border)"
                                    strokeWidth={1}
                                    filter="drop-shadow(0 4px 6px rgba(0,0,0,0.3))"
                                />
                                <text x={node.x + NODE_W + 18} y={node.y + 8} fill="var(--color-foreground)" fontSize="11" fontWeight="600">
                                    {node.label}
                                </text>
                                <text x={node.x + NODE_W + 18} y={node.y + 24} fill="var(--color-obsidian-muted)" fontSize="9">
                                    {node.operator}
                                </text>
                                <text x={node.x + NODE_W + 18} y={node.y + 38} fill="var(--color-obsidian-muted)" fontSize="9">
                                    Rule: {node.triggerRule}
                                </text>
                                {node.state && (
                                    <text x={node.x + NODE_W + 18} y={node.y + 52} fill={stateInfo.text} fontSize="9" fontWeight="600">
                                        {stateInfo.label}{node.duration != null ? ` · ${node.duration.toFixed(1)}s` : ''}
                                    </text>
                                )}
                            </g>
                        );
                    })()}
                </svg>
            </div>

            {/* Zoom indicator */}
            <div className="absolute bottom-2 right-2 text-[9px] text-obsidian-muted bg-obsidian-panel/90 px-2 py-0.5 rounded border border-obsidian-border">
                {Math.round(zoom * 100)}%
            </div>
        </div>
    );
}
