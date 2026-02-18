
import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
    Database, GitBranch, Box, ChevronDown, RefreshCw, Loader2, Info,
    ZoomIn, ZoomOut, Maximize2, ArrowRight, Circle, Layers, Clock, Tag
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
                fill: hoveredNode === node.id ? '#1a3a5c' : '#152d4a',
                stroke: selectedNode?.id === node.id ? '#3574f0' : '#2a5a8f',
                iconColor: '#3574f0',
            };
        }
        return {
            fill: hoveredNode === node.id ? '#2d4a2d' : '#1e3a1e',
            stroke: selectedNode?.id === node.id ? '#6aab73' : '#3d6b3d',
            iconColor: '#6aab73',
        };
    };

    // ─── Job State Colors ───
    const getRunStateColor = (state?: string) => {
        switch (state?.toUpperCase()) {
            case 'COMPLETED': case 'SUCCESS': return '#6aab73';
            case 'FAILED': return '#ff5261';
            case 'RUNNING': return '#e5c07b';
            case 'ABORTED': return '#6c707e';
            default: return '#6c707e';
        }
    };

    return (
        <div className="flex h-screen bg-[#1e1f22] text-[#bcbec4] font-sans overflow-hidden">
            <Sidebar />

            <div className="flex-1 flex flex-col min-w-0">
                {/* ─── Top Bar ─── */}
                <div className="h-9 bg-[#2b2d30] border-b border-[#393b40] flex items-center px-4 justify-between shrink-0">
                    <div className="flex items-center gap-3 text-[12px]">
                        <GitBranch className="w-3.5 h-3.5 text-[#c678dd]" />
                        <span className="text-[#bcbec4] font-medium">Data Lineage</span>
                        <span className="text-[#6c707e]">·</span>

                        {/* Namespace Selector */}
                        <div className="relative">
                            <button
                                onClick={() => setNsDropdownOpen(!nsDropdownOpen)}
                                className="flex items-center gap-1.5 px-2 py-0.5 bg-[#1e1f22] border border-[#393b40] rounded text-[11px] hover:border-[#555] transition-colors"
                            >
                                <Layers className="w-3 h-3 text-[#c678dd]" />
                                {selectedNs || 'Select Namespace'}
                                <ChevronDown className="w-3 h-3 text-[#6c707e]" />
                            </button>
                            {nsDropdownOpen && (
                                <div className="absolute top-7 left-0 bg-[#2b2d30] border border-[#393b40] rounded shadow-xl z-50 min-w-[200px]">
                                    {namespaces.map(ns => (
                                        <button
                                            key={ns.name}
                                            onClick={() => { setSelectedNs(ns.name); setNsDropdownOpen(false); }}
                                            className={clsx(
                                                "w-full text-left px-3 py-1.5 text-[11px] hover:bg-[#393b40] transition-colors",
                                                selectedNs === ns.name && "bg-[#393b40] text-white"
                                            )}
                                        >
                                            {ns.name}
                                        </button>
                                    ))}
                                    {namespaces.length === 0 && (
                                        <div className="px-3 py-2 text-[10px] text-[#6c707e]">No namespaces found</div>
                                    )}
                                </div>
                            )}
                        </div>

                        <span className="text-[#6c707e]">·</span>
                        <span className="text-[#6c707e]">{jobs.length} jobs</span>
                        <span className="text-[#6c707e]">·</span>
                        <span className="text-[#6c707e]">{datasets.length} datasets</span>
                    </div>

                    <div className="flex items-center gap-2">
                        <button
                            onClick={fetchNamespaces}
                            className="p-1 hover:bg-[#393b40] rounded text-[#6c707e] hover:text-[#bcbec4] transition-colors"
                            title="Refresh"
                        >
                            <RefreshCw className="w-3.5 h-3.5" />
                        </button>
                    </div>
                </div>

                {/* ─── Main Content ─── */}
                <div className="flex-1 flex overflow-hidden">

                    {/* Left: Job/Dataset List */}
                    <div className="w-[260px] border-r border-[#393b40] flex flex-col bg-[#2b2d30] overflow-hidden shrink-0">
                        {/* Jobs Section */}
                        <div className="border-b border-[#393b40]">
                            <div className="px-3 py-2 text-[10px] font-bold text-[#6c707e] uppercase tracking-wider flex items-center gap-1.5">
                                <Box className="w-3 h-3 text-[#6aab73]" />
                                Jobs ({jobs.length})
                            </div>
                            <div className="max-h-[35vh] overflow-y-auto">
                                {jobs.map((job: any) => (
                                    <button
                                        key={job.name}
                                        onClick={() => fetchLineage(`job:${selectedNs}:${job.name}`)}
                                        className="w-full text-left px-3 py-1.5 text-[11px] hover:bg-[#393b40] transition-colors flex items-center gap-2 group"
                                    >
                                        <Circle className="w-2.5 h-2.5 flex-shrink-0" style={{ color: getRunStateColor(job.latestRun?.state) }} />
                                        <span className="truncate group-hover:text-white transition-colors">{job.name}</span>
                                    </button>
                                ))}
                                {jobs.length === 0 && !loading && (
                                    <div className="px-3 py-2 text-[10px] text-[#6c707e]">No jobs in namespace</div>
                                )}
                            </div>
                        </div>

                        {/* Datasets Section */}
                        <div className="flex-1 overflow-hidden flex flex-col">
                            <div className="px-3 py-2 text-[10px] font-bold text-[#6c707e] uppercase tracking-wider flex items-center gap-1.5">
                                <Database className="w-3 h-3 text-[#3574f0]" />
                                Datasets ({datasets.length})
                            </div>
                            <div className="flex-1 overflow-y-auto">
                                {datasets.map((ds: any) => (
                                    <button
                                        key={ds.name}
                                        onClick={() => fetchLineage(`dataset:${selectedNs}:${ds.name}`)}
                                        className="w-full text-left px-3 py-1.5 text-[11px] hover:bg-[#393b40] transition-colors flex items-center gap-2 group"
                                    >
                                        <Database className="w-3 h-3 text-[#3574f0] flex-shrink-0" />
                                        <span className="truncate group-hover:text-white transition-colors">{ds.name}</span>
                                    </button>
                                ))}
                                {datasets.length === 0 && !loading && (
                                    <div className="px-3 py-2 text-[10px] text-[#6c707e]">No datasets in namespace</div>
                                )}
                            </div>
                        </div>
                    </div>

                    {/* Center: Lineage Graph */}
                    <div className="flex-1 relative bg-[#1e1f22] overflow-hidden">
                        {/* Zoom Controls */}
                        <div className="absolute top-3 right-3 z-20 flex flex-col gap-1">
                            <button onClick={() => setZoom(z => Math.min(3, z + 0.15))} className="p-1.5 bg-[#2b2d30] border border-[#393b40] rounded hover:bg-[#393b40] transition-colors">
                                <ZoomIn className="w-3.5 h-3.5 text-[#bcbec4]" />
                            </button>
                            <button onClick={() => setZoom(z => Math.max(0.2, z - 0.15))} className="p-1.5 bg-[#2b2d30] border border-[#393b40] rounded hover:bg-[#393b40] transition-colors">
                                <ZoomOut className="w-3.5 h-3.5 text-[#bcbec4]" />
                            </button>
                            <button onClick={() => { setZoom(0.85); setPan({ x: 40, y: 40 }); }} className="p-1.5 bg-[#2b2d30] border border-[#393b40] rounded hover:bg-[#393b40] transition-colors">
                                <Maximize2 className="w-3.5 h-3.5 text-[#bcbec4]" />
                            </button>
                        </div>

                        {loading || graphLoading ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-3">
                                    <Loader2 className="w-6 h-6 text-[#c678dd] animate-spin" />
                                    <span className="text-[12px] text-[#6c707e]">{graphLoading ? 'Loading lineage...' : 'Connecting to Marquez...'}</span>
                                </div>
                            </div>
                        ) : error ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-3 max-w-md text-center">
                                    <Info className="w-8 h-8 text-[#ff5261]" />
                                    <span className="text-[12px] text-[#ff5261]">{error}</span>
                                    <button onClick={fetchNamespaces} className="px-3 py-1 bg-[#2b2d30] border border-[#393b40] rounded text-[11px] hover:bg-[#393b40]">
                                        Retry
                                    </button>
                                </div>
                            </div>
                        ) : lineageNodes.length === 0 ? (
                            <div className="absolute inset-0 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-4 text-center">
                                    <GitBranch className="w-12 h-12 text-[#393b40]" />
                                    <div>
                                        <p className="text-[13px] text-[#bcbec4] mb-1">Select a job or dataset to view lineage</p>
                                        <p className="text-[11px] text-[#6c707e]">Click any item from the left panel to explore its data lineage graph</p>
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
                                <g transform={`translate(${pan.x}, ${pan.y}) scale(${zoom})`}>
                                    {/* Edges */}
                                    {lineageEdges.map((edge, i) => {
                                        const src = lineageNodes.find(n => n.id === edge.origin);
                                        const tgt = lineageNodes.find(n => n.id === edge.destination);
                                        if (!src || !tgt) return null;
                                        return (
                                            <g key={i}>
                                                <path
                                                    d={edgePath(src, tgt)}
                                                    fill="none"
                                                    stroke="#393b40"
                                                    strokeWidth={2}
                                                    opacity={0.8}
                                                />
                                                {/* Arrowhead */}
                                                <circle
                                                    cx={tgt.x}
                                                    cy={tgt.y + NODE_H / 2}
                                                    r={3.5}
                                                    fill="#555"
                                                />
                                            </g>
                                        );
                                    })}

                                    {/* Nodes */}
                                    {lineageNodes.map(node => {
                                        const style = getNodeStyle(node);
                                        const label = node.data.name.length > 28
                                            ? node.data.name.slice(-28)
                                            : node.data.name;
                                        return (
                                            <g
                                                key={node.id}
                                                transform={`translate(${node.x}, ${node.y})`}
                                                onClick={() => setSelectedNode(node)}
                                                onMouseEnter={() => setHoveredNode(node.id)}
                                                onMouseLeave={() => setHoveredNode(null)}
                                                className="cursor-pointer"
                                            >
                                                <rect
                                                    x={0} y={0}
                                                    width={NODE_W} height={NODE_H}
                                                    rx={6}
                                                    fill={style.fill}
                                                    stroke={style.stroke}
                                                    strokeWidth={selectedNode?.id === node.id ? 2 : 1}
                                                />
                                                {/* Type badge */}
                                                <rect
                                                    x={8} y={8}
                                                    width={node.type === 'DATASET' ? 52 : 32}
                                                    height={14}
                                                    rx={3}
                                                    fill={style.iconColor}
                                                    opacity={0.15}
                                                />
                                                <text
                                                    x={node.type === 'DATASET' ? 34 : 24}
                                                    y={18}
                                                    textAnchor="middle"
                                                    fill={style.iconColor}
                                                    fontSize={8}
                                                    fontWeight={600}
                                                    fontFamily="monospace"
                                                >
                                                    {node.type === 'DATASET' ? 'DATASET' : 'JOB'}
                                                </text>
                                                {/* Name */}
                                                <text
                                                    x={10}
                                                    y={38}
                                                    fill="#dfe1e5"
                                                    fontSize={11}
                                                    fontFamily="'Inter', sans-serif"
                                                >
                                                    {label}
                                                </text>
                                            </g>
                                        );
                                    })}
                                </g>
                            </svg>
                        )}

                        {/* Legend */}
                        {lineageNodes.length > 0 && (
                            <div className="absolute bottom-3 left-3 flex gap-4 text-[10px] text-[#6c707e] bg-[#2b2d30]/80 px-3 py-1.5 rounded border border-[#393b40]">
                                <div className="flex items-center gap-1.5">
                                    <div className="w-3 h-3 rounded bg-[#152d4a] border border-[#2a5a8f]" />
                                    Dataset
                                </div>
                                <div className="flex items-center gap-1.5">
                                    <div className="w-3 h-3 rounded bg-[#1e3a1e] border border-[#3d6b3d]" />
                                    Job
                                </div>
                                <div className="flex items-center gap-1.5">
                                    <ArrowRight className="w-3 h-3" />
                                    Data Flow
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Right: Detail Panel */}
                    {selectedNode && (
                        <div className="w-[300px] border-l border-[#393b40] bg-[#2b2d30] overflow-y-auto shrink-0">
                            <div className="px-4 py-3 border-b border-[#393b40]">
                                <div className="flex items-center gap-2 mb-1">
                                    {selectedNode.type === 'DATASET' ? (
                                        <Database className="w-4 h-4 text-[#3574f0]" />
                                    ) : (
                                        <Box className="w-4 h-4 text-[#6aab73]" />
                                    )}
                                    <span className="text-[13px] font-medium text-white truncate">{selectedNode.data.name}</span>
                                </div>
                                <span className={clsx(
                                    "text-[9px] px-1.5 py-[1px] rounded font-mono font-medium",
                                    selectedNode.type === 'DATASET' ? "bg-[#3574f0]/15 text-[#3574f0]" : "bg-[#6aab73]/15 text-[#6aab73]"
                                )}>
                                    {selectedNode.type}
                                </span>
                            </div>

                            {/* Metadata */}
                            <div className="px-4 py-3 space-y-3 text-[11px]">
                                <div>
                                    <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1 flex items-center gap-1">
                                        <Layers className="w-3 h-3" /> Namespace
                                    </div>
                                    <div className="text-[#bcbec4]">{selectedNode.data.namespace}</div>
                                </div>

                                {selectedNode.data.type && (
                                    <div>
                                        <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1 flex items-center gap-1">
                                            <Tag className="w-3 h-3" /> Type
                                        </div>
                                        <div className="text-[#bcbec4]">{selectedNode.data.type}</div>
                                    </div>
                                )}

                                {selectedNode.data.description && (
                                    <div>
                                        <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1">Description</div>
                                        <div className="text-[#bcbec4]">{selectedNode.data.description}</div>
                                    </div>
                                )}

                                {selectedNode.data.updatedAt && (
                                    <div>
                                        <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1 flex items-center gap-1">
                                            <Clock className="w-3 h-3" /> Updated
                                        </div>
                                        <div className="text-[#bcbec4]">
                                            {new Date(selectedNode.data.updatedAt).toLocaleString()}
                                        </div>
                                    </div>
                                )}

                                {/* Dataset fields */}
                                {selectedNode.type === 'DATASET' && selectedNode.data.fields && selectedNode.data.fields.length > 0 && (
                                    <div>
                                        <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1">
                                            Columns ({selectedNode.data.fields.length})
                                        </div>
                                        <div className="space-y-1">
                                            {selectedNode.data.fields.map((f, i) => (
                                                <div key={i} className="flex items-center justify-between py-0.5">
                                                    <span className="text-[#bcbec4]">{f.name}</span>
                                                    <span className="text-[9px] px-1 py-[1px] rounded bg-[#61afef]/10 text-[#61afef] font-mono">{f.type}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Job latest run */}
                                {selectedNode.type === 'JOB' && selectedNode.data.latestRun && (
                                    <div>
                                        <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1">Latest Run</div>
                                        <div className="flex items-center gap-2 mb-1">
                                            <Circle className="w-2.5 h-2.5" style={{ color: getRunStateColor(selectedNode.data.latestRun.state) }} />
                                            <span className="text-[#bcbec4]">{selectedNode.data.latestRun.state}</span>
                                        </div>
                                        {selectedNode.data.latestRun.durationMs && (
                                            <div className="text-[#6c707e]">
                                                Duration: {(selectedNode.data.latestRun.durationMs / 1000).toFixed(1)}s
                                            </div>
                                        )}
                                    </div>
                                )}

                                {/* Connections */}
                                <div>
                                    <div className="text-[#6c707e] text-[10px] uppercase tracking-wider mb-1">Connections</div>
                                    <div className="text-[#bcbec4]">
                                        {selectedNode.inEdges.length} inputs · {selectedNode.outEdges.length} outputs
                                    </div>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
