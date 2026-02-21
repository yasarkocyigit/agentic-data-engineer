
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
    Database, GitBranch, Box, ChevronDown, RefreshCw, Loader2, Info,
    ZoomIn, ZoomOut, Maximize2, ArrowRight, Circle, Layers, Clock, Tag, LayoutPanelLeft
} from 'lucide-react';
import clsx from 'clsx';
import { Sidebar } from '@/components/Sidebar';

// ─── Types ───
interface MarquezNode {
    id: string;
    type: 'DATASET' | 'JOB';
    data: {
        name: string;
        namespace: string;
        type?: string;
        description?: string;
        updatedAt?: string;
        createdAt?: string;
        // Dataset fields
        sourceName?: string;
        fields?: Array<{ name: string; type: string; description?: string }>;
        // Job fields
        latestRun?: {
            state: string;
            startedAt?: string;
            endedAt?: string;
            durationMs?: number;
        };
    };
    inEdges: Array<{ origin: string }>;
    outEdges: Array<{ destination: string }>;
}

interface MarquezEdge {
    origin: string;
    destination: string;
}

interface LayoutNode extends MarquezNode {
    x: number;
    y: number;
    layer: number;
}

interface Namespace {
    name: string;
    createdAt?: string;
    updatedAt?: string;
}

// ─── Constants ───
const NODE_W = 220;
const NODE_H = 50;
const LAYER_GAP = 200;
const NODE_GAP = 80;

// ─── Layout Algorithm ───
function computeLayout(nodes: MarquezNode[], edges: MarquezEdge[]): LayoutNode[] {
    if (nodes.length === 0) return [];

    // Build adjacency
    const outgoing = new Map<string, string[]>();
    const incoming = new Map<string, string[]>();
    nodes.forEach(n => {
        outgoing.set(n.id, []);
        incoming.set(n.id, []);
    });
    edges.forEach(e => {
        outgoing.get(e.origin)?.push(e.destination);
        incoming.get(e.destination)?.push(e.origin);
    });

    // Topological layering (BFS from sources)
    const layers = new Map<string, number>();
    const queue: string[] = [];

    nodes.forEach(n => {
        if ((incoming.get(n.id) || []).length === 0) {
            layers.set(n.id, 0);
            queue.push(n.id);
        }
    });

    // If no sources found (cyclic), assign all to layer 0
    if (queue.length === 0) {
        nodes.forEach((n, i) => layers.set(n.id, i % 3));
    }

    while (queue.length > 0) {
        const current = queue.shift()!;
        const currentLayer = layers.get(current)!;
        (outgoing.get(current) || []).forEach(target => {
            const existing = layers.get(target);
            if (existing === undefined || existing < currentLayer + 1) {
                layers.set(target, currentLayer + 1);
                queue.push(target);
            }
        });
    }

    // Assign x/y
    const layerCounts = new Map<number, number>();
    const result: LayoutNode[] = nodes.map(n => {
        const layer = layers.get(n.id) || 0;
        const idx = layerCounts.get(layer) || 0;
        layerCounts.set(layer, idx + 1);
        return {
            ...n,
            x: layer * (NODE_W + LAYER_GAP) + 60,
            y: idx * (NODE_H + NODE_GAP) + 60,
            layer,
        };
    });

    return result;
}

function edgePath(src: LayoutNode, tgt: LayoutNode): string {
    const x1 = src.x + NODE_W;
    const y1 = src.y + NODE_H / 2;
    const x2 = tgt.x;
    const y2 = tgt.y + NODE_H / 2;
    const cpx = (x1 + x2) / 2;
    return `M ${x1} ${y1} C ${cpx} ${y1}, ${cpx} ${y2}, ${x2} ${y2}`;
}

// ─── Component ───
export default function LineagePage() {
    const [namespaces, setNamespaces] = useState<Namespace[]>([]);
    const [selectedNs, setSelectedNs] = useState<string>('');
    const [jobs, setJobs] = useState<any[]>([]);
    const [datasets, setDatasets] = useState<any[]>([]);
    const [lineageNodes, setLineageNodes] = useState<LayoutNode[]>([]);
    const [lineageEdges, setLineageEdges] = useState<MarquezEdge[]>([]);
    const [selectedNode, setSelectedNode] = useState<LayoutNode | null>(null);
    const [hoveredNode, setHoveredNode] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [graphLoading, setGraphLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [nsDropdownOpen, setNsDropdownOpen] = useState(false);

    // Zoom/Pan
    const [zoom, setZoom] = useState(1);
    const [pan, setPan] = useState({ x: 0, y: 0 });
    const [isPanning, setIsPanning] = useState(false);
    const panStart = useRef({ x: 0, y: 0 });
    const svgRef = useRef<SVGSVGElement>(null);

    // ─── Fetch Namespaces ───
    const fetchNamespaces = useCallback(async () => {
        try {
            setLoading(true);
            const res = await fetch('/api/lineage?action=namespaces');
            const data = await res.json();
            if (data.error) throw new Error(data.error);
            const nsList = data.namespaces || [];
            setNamespaces(nsList);
            if (nsList.length > 0 && !selectedNs) {
                setSelectedNs(nsList[0].name);
            }
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Failed to fetch namespaces');
        } finally {
            setLoading(false);
        }
    }, [selectedNs]);

    // ─── Fetch Jobs & Datasets for namespace ───
    const fetchNamespaceData = useCallback(async (ns: string) => {
        try {
            const [jobsRes, datasetsRes] = await Promise.all([
                fetch(`/api/lineage?action=jobs&ns=${encodeURIComponent(ns)}`),
                fetch(`/api/lineage?action=datasets&ns=${encodeURIComponent(ns)}`)
            ]);
            const jobsData = await jobsRes.json();
            const datasetsData = await datasetsRes.json();
            setJobs(jobsData.jobs || []);
            setDatasets(datasetsData.datasets || []);
        } catch (e) {
            console.error('Failed to fetch namespace data', e);
        }
    }, []);

    // ─── Fetch Lineage Graph ───
    const fetchLineage = useCallback(async (nodeId: string) => {
        try {
            setGraphLoading(true);
            setError(null);
            const res = await fetch(`/api/lineage?action=lineage&nodeId=${encodeURIComponent(nodeId)}&depth=10`);
            const data = await res.json();
            if (data.error) throw new Error(data.error);

            const graph = data.graph || [];
            const nodes: MarquezNode[] = graph.map((n: any) => ({
                id: n.id,
                type: n.type,
                data: n.data,
                inEdges: n.inEdges || [],
                outEdges: n.outEdges || [],
            }));

            const edges: MarquezEdge[] = [];
            graph.forEach((n: any) => {
                (n.outEdges || []).forEach((e: any) => {
                    edges.push({ origin: n.id, destination: e.destination });
                });
            });

            const layout = computeLayout(nodes, edges);
            setLineageNodes(layout);
            setLineageEdges(edges);

            // Center the graph
            setPan({ x: 40, y: 40 });
            setZoom(0.85);
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : 'Failed to fetch lineage');
        } finally {
            setGraphLoading(false);
        }
    }, []);

    // Auto-fetch namespaces on mount
    useEffect(() => { fetchNamespaces(); }, []);

    // Fetch jobs/datasets when namespace changes
    useEffect(() => {
        if (selectedNs) {
            fetchNamespaceData(selectedNs);
        }
    }, [selectedNs, fetchNamespaceData]);

    // ─── Mouse Handlers (Pan) ───
    const handleMouseDown = (e: React.MouseEvent) => {
        if (e.button === 0) {
            setIsPanning(true);
            panStart.current = { x: e.clientX - pan.x, y: e.clientY - pan.y };
        }
    };

    const handleMouseMove = (e: React.MouseEvent) => {
        if (isPanning) {
            setPan({ x: e.clientX - panStart.current.x, y: e.clientY - panStart.current.y });
        }
    };

    const handleMouseUp = () => setIsPanning(false);

    const handleWheel = (e: React.WheelEvent) => {
        e.preventDefault();
        const delta = e.deltaY > 0 ? -0.05 : 0.05;
        setZoom(z => Math.max(0.2, Math.min(3, z + delta)));
    };

    // ─── Node Colors ───
    const getNodeStyle = (node: LayoutNode) => {
        if (node.type === 'DATASET') {
            return {
                fill: hoveredNode === node.id ? 'var(--color-obsidian-panel-header)' : 'var(--color-obsidian-panel)',
                stroke: selectedNode?.id === node.id ? '#38bdf8' : 'var(--color-obsidian-border)',
                iconColor: '#38bdf8', // sky-400
            };
        }
        return {
            fill: hoveredNode === node.id ? 'var(--color-obsidian-panel-header)' : 'var(--color-obsidian-panel)',
            stroke: selectedNode?.id === node.id ? '#818cf8' : 'var(--color-obsidian-border)',
            iconColor: '#818cf8', // indigo-400
        };
    };

    // ─── Job State Colors ───
    const getRunStateColor = (state?: string) => {
        switch (state?.toUpperCase()) {
            case 'COMPLETED': case 'SUCCESS': return 'var(--color-obsidian-success)';
            case 'FAILED': return 'var(--color-obsidian-danger)';
            case 'RUNNING': return 'var(--color-obsidian-warning)';
            case 'ABORTED': return 'var(--color-obsidian-muted)';
            default: return 'var(--color-obsidian-muted)';
        }
    };

    return (
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            <Sidebar />

            <main className="flex-1 flex flex-col min-w-0 overflow-hidden bg-obsidian-bg">
                {/* ─── Top Bar ─── */}
                <div className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/60 border-b border-obsidian-border/30 z-10 w-full">
                    <div className="flex items-center gap-3 text-[12px]">
                        <button
                            onClick={() => window.dispatchEvent(new CustomEvent('openclaw:toggle-sidebar'))}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-obsidian-border/50"
                            title="Toggle Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]" />
                        </button>
                        <div className="w-[1px] h-4 bg-obsidian-border/50"></div>
                        <GitBranch className="w-4 h-4 text-indigo-400 drop-shadow-[0_0_8px_rgba(129,140,248,0.5)]" />
                        <span className="text-white font-semibold tracking-wide">Data Lineage</span>
                        <span className="text-obsidian-muted/50">·</span>

                        {/* Namespace Selector */}
                        <div className="relative">
                            <button
                                onClick={() => setNsDropdownOpen(!nsDropdownOpen)}
                                className="flex items-center gap-1.5 px-3 py-1.5 bg-[#18181b] border border-obsidian-border/50 rounded-md text-[11px] font-medium hover:border-white/20 hover:bg-[#27272a] transition-all active:scale-95 shadow-sm"
                            >
                                <Layers className="w-3 h-3 text-indigo-400" />
                                <span className={clsx(selectedNs ? "text-white/90" : "text-obsidian-muted")}>
                                    {selectedNs || 'Select Namespace'}
                                </span>
                                <ChevronDown className={clsx("w-3.5 h-3.5 text-obsidian-muted transition-transform duration-200", nsDropdownOpen && "rotate-180")} />
                            </button>
                            {nsDropdownOpen && (
                                <div className="absolute top-9 left-0 mt-1 bg-[#18181b]/95 backdrop-blur-xl border border-obsidian-border/50 rounded-lg shadow-2xl z-50 min-w-[200px] overflow-hidden flex flex-col">
                                    {namespaces.map(ns => (
                                        <button
                                            key={ns.name}
                                            onClick={() => { setSelectedNs(ns.name); setNsDropdownOpen(false); }}
                                            className={clsx(
                                                "w-full text-left px-3 py-2.5 text-[11px] transition-all border-l-2 border-transparent hover:bg-white/5",
                                                selectedNs === ns.name ? "bg-indigo-500/10 border-indigo-500 text-white font-medium" : "text-obsidian-muted hover:text-white"
                                            )}
                                        >
                                            {ns.name}
                                        </button>
                                    ))}
                                    {namespaces.length === 0 && (
                                        <div className="px-3 py-3 text-[10px] text-obsidian-muted italic text-center">No namespaces found</div>
                                    )}
                                </div>
                            )}
                        </div>

                        <span className="text-obsidian-muted/50">·</span>
                        <span className="text-obsidian-muted font-mono bg-white/5 px-2 py-0.5 rounded">{jobs.length} jobs</span>
                        <span className="text-obsidian-muted font-mono bg-white/5 px-2 py-0.5 rounded">{datasets.length} datasets</span>
                    </div>

                    <div className="flex items-center gap-2">
                        <button
                            onClick={fetchNamespaces}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-colors border border-transparent hover:border-white/10"
                            title="Refresh"
                        >
                            <RefreshCw className="w-3.5 h-3.5" />
                        </button>
                    </div>
                </div>

                {/* ─── Main Content ─── */}
                <div className="flex-1 flex overflow-hidden">

                    {/* Left: Job/Dataset List */}
                    <div className="w-[280px] border-r border-obsidian-border/30 flex flex-col bg-[#09090b]/80 backdrop-blur-xl overflow-hidden shrink-0 shadow-[4px_0_24px_rgba(0,0,0,0.4)] z-10 transition-all duration-300">
                        {/* Jobs Section */}
                        <div className="flex-1 overflow-hidden flex flex-col border-b border-obsidian-border/30">
                            <div className="px-4 py-3 text-[10px] font-bold text-obsidian-muted uppercase tracking-widest flex items-center justify-between bg-black/40 shrink-0 shadow-sm border-b border-obsidian-border/20 sticky top-0 z-10 backdrop-blur-md">
                                <div className="flex items-center gap-1.5">
                                    <Box className="w-3.5 h-3.5 text-indigo-400" />
                                    Jobs
                                </div>
                                <span className="bg-white/10 px-1.5 py-0.5 rounded text-white/70">{jobs.length}</span>
                            </div>
                            <div className="flex-1 overflow-y-auto w-full custom-scrollbar">
                                {jobs.map((job: any) => {
                                    const isSelected = selectedNode?.id === `job:${selectedNs}:${job.name}`;
                                    return (
                                        <button
                                            key={job.name}
                                            onClick={() => fetchLineage(`job:${selectedNs}:${job.name}`)}
                                            className={clsx(
                                                "w-full text-left px-4 py-2.5 text-[11.5px] flex items-center gap-2.5 group transition-colors border-l-[3px]",
                                                isSelected
                                                    ? "bg-indigo-500/10 border-indigo-400 text-white font-medium shadow-[inset_0_1px_0_rgba(255,255,255,0.02)]"
                                                    : "bg-transparent border-transparent hover:bg-white/5 text-obsidian-muted hover:text-white/90"
                                            )}
                                        >
                                            <Circle className={clsx("w-2 h-2 flex-shrink-0 transition-colors", isSelected ? "text-indigo-400" : "")} style={{ color: !isSelected ? getRunStateColor(job.latestRun?.state) : undefined }} />
                                            <span className="truncate flex-1">{job.name}</span>
                                        </button>
                                    );
                                })}
                                {jobs.length === 0 && !loading && (
                                    <div className="px-4 py-6 text-[11px] text-obsidian-muted/50 italic text-center">No jobs in namespace</div>
                                )}
                            </div>
                        </div>

                        {/* Datasets Section */}
                        <div className="flex-1 overflow-hidden flex flex-col">
                            <div className="px-4 py-3 text-[10px] font-bold text-obsidian-muted uppercase tracking-widest flex items-center justify-between bg-black/40 shrink-0 shadow-sm border-b border-obsidian-border/20 sticky top-0 z-10 backdrop-blur-md">
                                <div className="flex items-center gap-1.5">
                                    <Database className="w-3.5 h-3.5 text-sky-400" />
                                    Datasets
                                </div>
                                <span className="bg-white/10 px-1.5 py-0.5 rounded text-white/70">{datasets.length}</span>
                            </div>
                            <div className="flex-1 overflow-y-auto w-full pb-6 custom-scrollbar">
                                {datasets.map((ds: any) => {
                                    const isSelected = selectedNode?.id === `dataset:${selectedNs}:${ds.name}`;
                                    return (
                                        <button
                                            key={ds.name}
                                            onClick={() => fetchLineage(`dataset:${selectedNs}:${ds.name}`)}
                                            className={clsx(
                                                "w-full text-left px-4 py-2.5 text-[11.5px] flex items-center gap-2.5 group transition-colors border-l-[3px]",
                                                isSelected
                                                    ? "bg-sky-500/10 border-sky-400 text-white font-medium shadow-[inset_0_1px_0_rgba(255,255,255,0.02)]"
                                                    : "bg-transparent border-transparent hover:bg-white/5 text-obsidian-muted hover:text-white/90"
                                            )}
                                        >
                                            <Database className={clsx("w-3.5 h-3.5 flex-shrink-0 transition-colors", isSelected ? "text-sky-400 drop-shadow-[0_0_5px_rgba(56,189,248,0.4)]" : "text-sky-400/50 group-hover:text-sky-400/80")} />
                                            <span className="truncate flex-1">{ds.name}</span>
                                        </button>
                                    );
                                })}
                                {datasets.length === 0 && !loading && (
                                    <div className="px-4 py-6 text-[11px] text-obsidian-muted/50 italic text-center">No datasets in namespace</div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* Center: Lineage Graph */}
                    <div className="flex-1 relative bg-obsidian-bg overflow-hidden">
                        {/* Zoom Controls */}
                        <div className="absolute top-3 right-3 z-20 flex flex-col gap-1">
                            <button onClick={() => setZoom(z => Math.min(3, z + 0.15))} className="p-1.5 bg-obsidian-panel border border-obsidian-border rounded hover:bg-obsidian-panel-hover transition-colors">
                                <ZoomIn className="w-3.5 h-3.5 text-foreground" />
                            </button>
                            <button onClick={() => setZoom(z => Math.max(0.2, z - 0.15))} className="p-1.5 bg-obsidian-panel border border-obsidian-border rounded hover:bg-obsidian-panel-hover transition-colors">
                                <ZoomOut className="w-3.5 h-3.5 text-foreground" />
                            </button>
                            <button onClick={() => { setZoom(0.85); setPan({ x: 40, y: 40 }); }} className="p-1.5 bg-obsidian-panel border border-obsidian-border rounded hover:bg-obsidian-panel-hover transition-colors">
                                <Maximize2 className="w-3.5 h-3.5 text-foreground" />
                            </button>
                        </div>

                        {loading || graphLoading ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-3">
                                    <Loader2 className="w-6 h-6 text-obsidian-purple animate-spin" />
                                    <span className="text-[12px] text-obsidian-muted">{graphLoading ? 'Loading lineage...' : 'Connecting to Marquez...'}</span>
                                </div>
                            </div>
                        ) : error ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-3 max-w-md text-center">
                                    <Info className="w-8 h-8 text-obsidian-danger" />
                                    <span className="text-[12px] text-obsidian-danger">{error}</span>
                                    <button onClick={fetchNamespaces} className="px-3 py-1 bg-obsidian-panel border border-obsidian-border rounded text-[11px] hover:bg-obsidian-panel-hover transition-all active:scale-95">
                                        Retry
                                    </button>
                                </div>
                            </div>
                        ) : lineageNodes.length === 0 ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-6 text-center max-w-sm p-8 rounded-2xl bg-black/20 backdrop-blur-md border border-white/5 shadow-2xl">
                                    <div className="p-4 rounded-full bg-white/5 drop-shadow-[0_0_15px_rgba(255,255,255,0.1)]">
                                        <GitBranch className="w-12 h-12 text-obsidian-border" />
                                    </div>
                                    <div>
                                        <p className="text-[16px] font-medium text-foreground mb-2 tracking-tight">Select a node to explore lineage</p>
                                        <p className="text-[12px] text-obsidian-muted leading-relaxed">Choose any job or dataset from the explorer panel to visualize its upstream and downstream data flow dependencies.</p>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <svg
                                ref={svgRef}
                                className={clsx("w-full h-full", isPanning ? "cursor-grabbing" : "cursor-grab")}
                                onMouseDown={handleMouseDown}
                                onMouseMove={handleMouseMove}
                                onMouseUp={handleMouseUp}
                                onMouseLeave={handleMouseUp}
                                onWheel={handleWheel}
                            >
                                <defs>
                                    <pattern id="ide-grid" width="30" height="30" patternUnits="userSpaceOnUse">
                                        <circle cx="1" cy="1" r="1" fill="rgba(255,255,255,0.05)" />
                                    </pattern>
                                    <filter id="nodeShadow" x="-20%" y="-20%" width="140%" height="140%">
                                        <feDropShadow dx="0" dy="4" stdDeviation="6" floodColor="#000000" floodOpacity="0.4" />
                                    </filter>
                                    <filter id="nodeShadowHover" x="-20%" y="-20%" width="140%" height="140%">
                                        <feDropShadow dx="0" dy="6" stdDeviation="8" floodColor="#000000" floodOpacity="0.6" />
                                    </filter>
                                    <filter id="glowSelected" x="-30%" y="-30%" width="160%" height="160%">
                                        <feGaussianBlur stdDeviation="3" result="blur" />
                                        <feMerge>
                                            <feMergeNode in="blur" />
                                            <feMergeNode in="SourceGraphic" />
                                        </feMerge>
                                    </filter>
                                    <linearGradient id="edgeGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                                        <stop offset="0%" stopColor="var(--color-obsidian-border)" stopOpacity="0.4" />
                                        <stop offset="50%" stopColor="var(--color-obsidian-info)" stopOpacity="0.9" />
                                        <stop offset="100%" stopColor="var(--color-obsidian-border)" stopOpacity="0.4" />
                                    </linearGradient>
                                    <style>
                                        {`
                                            @keyframes flowAnim {
                                                to { stroke-dashoffset: -40; }
                                            }
                                        `}
                                    </style>
                                </defs>
                                <rect width="100%" height="100%" fill="url(#ide-grid)" pointerEvents="none" />
                                <g transform={`translate(${pan.x}, ${pan.y}) scale(${zoom})`}>
                                    {/* Edges */}
                                    {lineageEdges.map((edge, i) => {
                                        const src = lineageNodes.find(n => n.id === edge.origin);
                                        const tgt = lineageNodes.find(n => n.id === edge.destination);
                                        if (!src || !tgt) return null;
                                        const isConnectedToSelected = selectedNode && (selectedNode.id === src.id || selectedNode.id === tgt.id);
                                        return (
                                            <g key={i}>
                                                {/* Background invisible wider line for hovering/clicking (optional) */}
                                                <path
                                                    d={edgePath(src, tgt)}
                                                    fill="none"
                                                    stroke="transparent"
                                                    strokeWidth={10}
                                                />
                                                {/* Main Edge */}
                                                <path
                                                    d={edgePath(src, tgt)}
                                                    fill="none"
                                                    stroke={isConnectedToSelected ? "url(#edgeGradient)" : "var(--color-obsidian-border)"}
                                                    strokeWidth={isConnectedToSelected ? 2.5 : 1.5}
                                                    opacity={isConnectedToSelected ? 1 : 0.3}
                                                    strokeDasharray={isConnectedToSelected ? "4 8" : "none"}
                                                    style={isConnectedToSelected ? { animation: 'flowAnim 1.5s linear infinite' } : {}}
                                                />
                                                {/* Arrowhead / Data Dot */}
                                                <circle
                                                    cx={tgt.x}
                                                    cy={tgt.y + NODE_H / 2}
                                                    r={isConnectedToSelected ? 4 : 3}
                                                    fill={isConnectedToSelected ? "var(--color-obsidian-info)" : "var(--color-obsidian-muted)"}
                                                    filter={isConnectedToSelected ? "url(#glowSelected)" : undefined}
                                                />
                                            </g>
                                        );
                                    })}

                                    {/* Nodes */}
                                    {lineageNodes.map(node => {
                                        const style = getNodeStyle(node);
                                        const label = node.data.name.length > 28
                                            ? node.data.name.slice(0, 12) + "..." + node.data.name.slice(-12)
                                            : node.data.name;
                                        const isSelected = selectedNode?.id === node.id;
                                        const isHovered = hoveredNode === node.id;
                                        return (
                                            <g
                                                key={node.id}
                                                transform={`translate(${node.x}, ${node.y})`}
                                                onClick={() => setSelectedNode(node)}
                                                onMouseEnter={() => setHoveredNode(node.id)}
                                                onMouseLeave={() => setHoveredNode(null)}
                                                className="cursor-pointer transition-all duration-300"
                                            >
                                                {isSelected && (
                                                    <rect
                                                        x={-4} y={-4}
                                                        width={NODE_W + 8} height={NODE_H + 8}
                                                        rx={10}
                                                        fill="none"
                                                        stroke={style.stroke}
                                                        strokeWidth={2}
                                                        filter="url(#glowSelected)"
                                                        opacity={0.7}
                                                    />
                                                )}
                                                <foreignObject x="0" y="0" width={NODE_W} height={NODE_H}>
                                                    <div className={clsx(
                                                        "w-full h-full rounded-lg border flex flex-col justify-center px-3 relative overflow-hidden transition-all duration-300",
                                                        isSelected ? "bg-black/80 shadow-[0_0_20px_rgba(0,0,0,0.5)] border-transparent" : "bg-obsidian-panel/80 hover:bg-obsidian-panel shadow-lg",
                                                        isHovered && !isSelected ? "border-white/20" : "border-obsidian-border/50"
                                                    )}
                                                        style={{
                                                            backdropFilter: 'blur(8px)',
                                                            borderColor: isSelected ? style.stroke : undefined
                                                        }}
                                                    >
                                                        {/* Top Gradient Highlight */}
                                                        {isSelected && (
                                                            <div className="absolute top-0 left-0 right-0 h-[2px] opacity-80"
                                                                style={{ background: `linear-gradient(90deg, transparent, ${style.stroke}, transparent)` }}
                                                            />
                                                        )}

                                                        <div className="flex items-center gap-2.5">
                                                            <div className={clsx(
                                                                "p-1.5 rounded-md shrink-0 transition-colors",
                                                                isSelected ? "bg-white/10" : "bg-black/20"
                                                            )}>
                                                                {node.type === 'DATASET' ? (
                                                                    <Database className="w-4 h-4" style={{ color: style.iconColor }} />
                                                                ) : (
                                                                    <Box className="w-4 h-4" style={{ color: style.iconColor }} />
                                                                )}
                                                            </div>
                                                            <div className="flex flex-col min-w-0">
                                                                <span className={clsx(
                                                                    "text-[12px] truncate transition-colors",
                                                                    isSelected ? "text-white font-semibold" : "text-foreground font-medium"
                                                                )}>
                                                                    {label}
                                                                </span>
                                                                <span className="text-[9px] text-obsidian-muted uppercase tracking-wider font-mono">
                                                                    {node.type}
                                                                </span>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </foreignObject>
                                            </g>
                                        );
                                    })}
                                </g>
                            </svg>
                        )}

                        {/* Legend */}
                        {lineageNodes.length > 0 && (
                            <div className="absolute bottom-4 left-4 flex gap-5 text-[10px] text-white/60 bg-[#18181b]/90 backdrop-blur-md px-4 py-2 rounded-lg border border-white/10 shadow-lg">
                                <div className="flex items-center gap-2">
                                    <div className="w-3 h-3 rounded-sm bg-sky-500/20 border border-sky-500/50" />
                                    Dataset
                                </div>
                                <div className="flex items-center gap-2">
                                    <div className="w-3 h-3 rounded-sm bg-indigo-500/20 border border-indigo-500/50" />
                                    Job
                                </div>
                                <div className="flex items-center gap-2">
                                    <ArrowRight className="w-3.5 h-3.5 text-white/40" />
                                    Data Flow
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Right: Detail Panel */}
                    {selectedNode && (
                        <div className="w-[320px] border-l border-white/5 flex flex-col bg-[#09090b]/80 backdrop-blur-xl overflow-hidden shrink-0 shadow-[-4px_0_24px_rgba(0,0,0,0.4)] z-10 transition-all duration-300 transform translate-x-0">
                            <div className="px-5 py-4 border-b border-white/5 bg-black/40 shadow-sm shrink-0">
                                <div className="flex items-center gap-2.5 mb-2.5">
                                    {selectedNode.type === 'DATASET' ? (
                                        <Database className="w-4.5 h-4.5 text-sky-400 drop-shadow-[0_0_8px_rgba(56,189,248,0.3)]" />
                                    ) : (
                                        <Box className="w-4.5 h-4.5 text-indigo-400 drop-shadow-[0_0_8px_rgba(129,140,248,0.3)]" />
                                    )}
                                    <span className="text-[14px] font-semibold text-white tracking-wide truncate">{selectedNode.data.name}</span>
                                </div>
                                <span className={clsx(
                                    "text-[9px] px-2 py-0.5 rounded font-mono font-bold tracking-widest uppercase shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]",
                                    selectedNode.type === 'DATASET'
                                        ? "bg-sky-500/10 border border-sky-500/20 text-sky-400"
                                        : "bg-indigo-500/10 border border-indigo-500/20 text-indigo-400"
                                )}>
                                    {selectedNode.type}
                                </span>
                            </div>

                            {/* Metadata */}
                            <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4 text-[11px] custom-scrollbar">
                                <div>
                                    <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1 flex items-center gap-1">
                                        <Layers className="w-3.5 h-3.5" /> Namespace
                                    </div>
                                    <div className="text-foreground">{selectedNode.data.namespace}</div>
                                </div>

                                {selectedNode.data.type && (
                                    <div>
                                        <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1 flex items-center gap-1">
                                            <Tag className="w-3.5 h-3.5" /> Type
                                        </div>
                                        <div className="text-foreground">{selectedNode.data.type}</div>
                                    </div>
                                )}

                                {selectedNode.data.description && (
                                    <div>
                                        <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1">Description</div>
                                        <div className="text-foreground">{selectedNode.data.description}</div>
                                    </div>
                                )}

                                {selectedNode.data.updatedAt && (
                                    <div>
                                        <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1 flex items-center gap-1">
                                            <Clock className="w-3.5 h-3.5" /> Updated
                                        </div>
                                        <div className="text-foreground">
                                            {new Date(selectedNode.data.updatedAt).toLocaleString()}
                                        </div>
                                    </div>
                                )}

                                {/* Dataset fields */}
                                {selectedNode.type === 'DATASET' && selectedNode.data.fields && selectedNode.data.fields.length > 0 && (
                                    <div>
                                        <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1">
                                            Columns ({selectedNode.data.fields.length})
                                        </div>
                                        <div className="space-y-1">
                                            {selectedNode.data.fields.map((f, i) => (
                                                <div key={i} className="flex items-center justify-between py-0.5">
                                                    <span className="text-foreground">{f.name}</span>
                                                    <span className="text-[9px] px-1 py-[1px] rounded bg-obsidian-info/10 text-obsidian-info font-mono transition-all active:scale-95">{f.type}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Job latest run */}
                                {selectedNode.type === 'JOB' && selectedNode.data.latestRun && (
                                    <div>
                                        <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1">Latest Run</div>
                                        <div className="flex items-center gap-2 mb-1">
                                            <Circle className="w-2.5 h-2.5" style={{ color: getRunStateColor(selectedNode.data.latestRun.state) }} />
                                            <span className="text-foreground">{selectedNode.data.latestRun.state}</span>
                                        </div>
                                        {selectedNode.data.latestRun.durationMs && (
                                            <div className="text-obsidian-muted">
                                                Duration: {(selectedNode.data.latestRun.durationMs / 1000).toFixed(1)}s
                                            </div>
                                        )}
                                    </div>
                                )}

                                {/* Connections */}
                                <div>
                                    <div className="text-obsidian-muted text-[10px] uppercase tracking-wider mb-1">Connections</div>
                                    <div className="text-foreground">
                                        {selectedNode.inEdges.length} inputs · {selectedNode.outEdges.length} outputs
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </main>
        </div>
    );
}
