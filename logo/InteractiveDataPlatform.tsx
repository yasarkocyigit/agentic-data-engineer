'use client';

import React, { useState, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

// Types for our nodes and connections
type NodeType = 'source' | 'bronze' | 'silver' | 'gold' | 'serve' | 'control';

interface NodeData {
    id: string;
    type: NodeType;
    title: string;
    desc: string;
    tech: string;
    x: number;
    y: number;
    width: number;
    height: number;
    color: string;
}

const nodes: NodeData[] = [
    { id: 'source', type: 'source', title: 'Source Systems', desc: 'Internal databases, external APIs, and IoT streams providing raw data ingestion.', tech: 'REST / JDBC', x: 50, y: 280, width: 140, height: 140, color: 'blue' },
    { id: 'bronze', type: 'bronze', title: 'Raw Layer', desc: 'Immutable landing zone. Stores history in original format with append-only strategy.', tech: 'Parquet / JSON', x: 360, y: 280, width: 120, height: 140, color: 'amber' },
    { id: 'silver', type: 'silver', title: 'Curated Layer', desc: 'Cleansed, deduplicated, and validated data. Enforces enterprise schema and data quality.', tech: 'Delta Lake', x: 540, y: 280, width: 120, height: 140, color: 'slate' },
    { id: 'gold', type: 'gold', title: 'Semantic Layer', desc: 'Business-level aggregates and dimensional models optimized for high-performance querying.', tech: 'Star Schema', x: 720, y: 280, width: 120, height: 140, color: 'yellow' },
    { id: 'serve', type: 'serve', title: 'Analytics & AI', desc: 'Dashboards, ML models, and downstream applications consuming trusted data products.', tech: 'BI / API', x: 1000, y: 280, width: 140, height: 140, color: 'zinc' },
    // Widened Control Nodes to fit text
    { id: 'meta', type: 'control', title: 'Metadata Store', desc: 'Centralized repository for pipeline configurations, watermarks, and lineage tracking.', tech: 'Relational DB', x: 400, y: 70, width: 160, height: 60, color: 'neutral' },
    { id: 'adf', type: 'control', title: 'Orchestration Engine', desc: 'Workflow manager that triggers processing jobs based on metadata and events.', tech: 'Pipeline Runner', x: 640, y: 70, width: 160, height: 60, color: 'stone' },
];

// Connection definitions (who lights up when hovering whom)
const connections: Record<string, string[]> = {
    'source': ['link-source-bronze', 'node-bronze'],
    'bronze': ['link-source-bronze', 'link-bronze-silver', 'node-source', 'node-silver', 'link-adf-bronze'],
    'silver': ['link-bronze-silver', 'link-silver-gold', 'node-bronze', 'node-gold', 'link-adf-silver'],
    'gold': ['link-silver-gold', 'link-gold-serve', 'node-silver', 'node-serve', 'link-adf-gold'],
    'serve': ['link-gold-serve', 'node-gold'],
    'meta': ['link-adf-meta', 'node-adf'],
    'adf': ['link-adf-meta', 'link-adf-bronze', 'link-adf-silver', 'link-adf-gold', 'node-meta', 'node-bronze', 'node-silver', 'node-gold']
};

export default function InteractiveDataPlatform({ monochrome = false }: { monochrome?: boolean }) {
    const [hoveredNode, setHoveredNode] = useState<string | null>(null);
    const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
    const containerRef = useRef<HTMLDivElement>(null);

    // Handle mouse move for tooltip positioning
    const handleMouseMove = (e: React.MouseEvent) => {
        if (containerRef.current) {
            const rect = containerRef.current.getBoundingClientRect();
            setMousePos({
                x: e.clientX - rect.left,
                y: e.clientY - rect.top
            });
        }
    };

    // Helper to check if an element should be highlighted
    const isHighlighted = (id: string) => {
        if (!hoveredNode) return false;
        if (hoveredNode === id) return true;
        return connections[hoveredNode]?.includes(id);
    };

    // Helper to check if an element should be dimmed
    const isDimmed = (id: string) => {
        if (!hoveredNode) return false;
        return !isHighlighted(id);
    };

    return (
        <div
            ref={containerRef}
            onMouseMove={handleMouseMove}
            className="relative w-full bg-[#050505] rounded-none border border-white/10 shadow-2xl group/container md:aspect-[16/9]"
        >
            {/* MOBILE VIEW (Vertical System Flow) */}
            <div className="md:hidden p-6 flex flex-col relative z-10">
                {/* Header */}
                <div className="mb-8">
                    <h3 className="text-2xl font-bold text-white tracking-tight font-display">Modern Data <span className="text-zinc-500">Platform</span></h3>
                </div>

                {/* Continuous Data Flow Line */}
                <div className="absolute left-[2.25rem] top-24 bottom-12 w-[2px] bg-gradient-to-b from-blue-500/20 via-blue-500/10 to-transparent z-0 ml-[1px]" />

                {/* Stacked Cards */}
                <div className="flex flex-col gap-8">
                    {nodes.filter(n => n.type !== 'control').map((node, i) => (
                        <div key={node.id} className="relative z-10 pl-12 group">
                            {/* Node Connector Dot */}
                            <div className="absolute left-0 top-6 w-3 h-3 rounded-full bg-black border border-white/20 z-20 shadow-[0_0_10px_rgba(0,0,0,1)]">
                                <div className={`absolute inset-0.5 rounded-full opacity-50`} style={{ backgroundColor: getNodeColor(node.color, monochrome) }} />
                            </div>

                            <div className="bg-white/[0.03] border border-white/10 p-5 rounded-none relative overflow-hidden backdrop-blur-sm">
                                <div className="absolute inset-0 bg-gradient-to-r from-white/[0.02] to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />

                                <div className="flex items-center justify-between mb-2 relative z-10">
                                    <h3 className="text-lg font-bold text-white font-display tracking-tight">{node.title}</h3>
                                    <span className="text-[10px] text-white/30 font-mono uppercase tracking-widest">{node.type}</span>
                                </div>

                                <p className="text-white/60 text-xs mb-3 font-light leading-relaxed relative z-10 line-clamp-3">{node.desc}</p>

                                <div className="flex items-center gap-2 relative z-10">
                                    <div className={`w-1.5 h-1.5 rounded-full`} style={{ backgroundColor: getNodeColor(node.color, monochrome) }} />
                                    <span className="text-xs text-white/80 font-mono tracking-wider">
                                        {node.tech}
                                    </span>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>

                {/* Control Plane Section for Mobile */}
                <div className="mt-8 pt-8 border-t border-white/5 relative">
                    <h4 className="text-xs font-mono text-white/40 uppercase tracking-widest mb-6">Control Plane & Governance</h4>
                    <div className="grid grid-cols-1 gap-4 pl-0">
                        {nodes.filter(n => n.type === 'control').map(node => (
                            <div key={node.id} className="bg-white/[0.01] border border-white/5 p-4 rounded-none flex items-center justify-between">
                                <div>
                                    <h3 className="text-sm font-bold text-white font-display tracking-tight mb-1">{node.title}</h3>
                                </div>
                                <span className="text-[10px] bg-white/5 px-2 py-1 text-white/50">{node.tech}</span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            {/* DESKTOP VIEW (Original SVG + Backgrounds) */}
            <div className="hidden md:block absolute inset-0 overflow-hidden rounded-none">
                {/* Premium Background Effects */}
                <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_0%,rgba(255,255,255,0.03),rgba(255,255,255,0)_70%)] pointer-events-none" />

                {/* Circuit Grid Pattern */}
                <div className="absolute inset-0 opacity-10 pointer-events-none"
                    style={{
                        backgroundImage: `linear-gradient(rgba(255,255,255,0.05) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.05) 1px, transparent 1px)`,
                        backgroundSize: '40px 40px'
                    }}
                />

                {/* Header UI */}
                <div className="absolute top-8 left-8 z-20 pointer-events-none">
                    <h3 className="text-2xl font-bold text-white tracking-tight font-display">Modern Data <span className="text-zinc-500">Platform</span></h3>
                    <div className="flex gap-4 mt-2 text-xs font-medium text-white/40 font-mono uppercase tracking-wider">
                        <div className="flex items-center gap-2"><span className="w-1.5 h-1.5 rounded-none bg-zinc-500"></span> Control Plane</div>
                        <div className="flex items-center gap-2"><span className="w-1.5 h-1.5 rounded-none bg-zinc-400"></span> Data Plane</div>
                    </div>
                </div>

                <svg viewBox="0 0 1200 600" className="w-full h-full relative z-10">
                    <defs>
                        <marker id="arrow" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
                            <polygon points="0 0, 10 3.5, 0 7" fill="#334155" />
                        </marker>
                        <marker id="arrow-active" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
                            <polygon points="0 0, 10 3.5, 0 7" fill="#d4d4d8" />
                        </marker>


                    </defs>

                    {/* ZONES (Glass Panels) - Angular */}
                    <rect x="380" y="40" width="440" height="120" rx="0" fill="rgba(255, 255, 255, 0.01)" stroke="rgba(255, 255, 255, 0.05)" strokeWidth="1" strokeDasharray="4 4" />
                    <text x="600" y="30" fontSize="10" fill="#71717a" textAnchor="middle" letterSpacing="2" fontWeight="600" className="font-mono uppercase opacity-70">Control Plane</text>

                    <rect x="320" y="200" width="560" height="320" rx="0" fill="rgba(255, 255, 255, 0.01)" stroke="rgba(255, 255, 255, 0.05)" strokeWidth="1" strokeDasharray="4 4" />
                    <text x="600" y="540" fontSize="10" fill="#a1a1aa" textAnchor="middle" letterSpacing="2" fontWeight="600" className="font-mono uppercase opacity-70">Unified Data Plane</text>

                    {/* CONNECTIONS */}
                    <g className={`transition-opacity duration-500 ${hoveredNode ? 'opacity-20' : 'opacity-60'}`}>
                        {/* Data Flow */}
                        <path id="link-source-bronze" d="M 190 350 L 360 350" stroke="#334155" strokeWidth="1.5" markerEnd="url(#arrow)" />
                        <path id="link-bronze-silver" d="M 480 350 L 540 350" stroke="#334155" strokeWidth="1.5" markerEnd="url(#arrow)" />
                        <path id="link-silver-gold" d="M 660 350 L 720 350" stroke="#334155" strokeWidth="1.5" markerEnd="url(#arrow)" />
                        <path id="link-gold-serve" d="M 840 350 L 1000 350" stroke="#334155" strokeWidth="1.5" markerEnd="url(#arrow)" />

                        {/* Control Flow */}
                        <path id="link-adf-meta" d="M 640 100 L 560 100" stroke="#52525b" strokeWidth="1.5" strokeDasharray="4,4" markerEnd="url(#arrow)" className="opacity-40" />
                        <path id="link-adf-bronze" d="M 720 130 L 720 160 L 420 160 L 420 280" stroke="#52525b" strokeWidth="1.5" strokeDasharray="4,4" fill="none" className="opacity-40" />
                        <path id="link-adf-silver" d="M 720 130 L 720 160 L 600 160 L 600 280" stroke="#52525b" strokeWidth="1.5" strokeDasharray="4,4" fill="none" className="opacity-40" />
                        <path id="link-adf-gold" d="M 720 130 L 720 160 L 780 160 L 780 280" stroke="#52525b" strokeWidth="1.5" strokeDasharray="4,4" fill="none" className="opacity-40" />
                    </g>

                    {/* Highlighted Connections Overlay (Animated) */}
                    {hoveredNode && (
                        <g className="pointer-events-none">
                            {/* Data Flow Highlights */}
                            <path d="M 190 350 L 360 350" stroke="#d4d4d8" strokeWidth="2" markerEnd="url(#arrow-active)" className={isHighlighted('link-source-bronze') ? 'opacity-100' : 'opacity-0'} />
                            <path d="M 480 350 L 540 350" stroke="#d4d4d8" strokeWidth="2" markerEnd="url(#arrow-active)" className={isHighlighted('link-bronze-silver') ? 'opacity-100' : 'opacity-0'} />
                            <path d="M 660 350 L 720 350" stroke="#d4d4d8" strokeWidth="2" markerEnd="url(#arrow-active)" className={isHighlighted('link-silver-gold') ? 'opacity-100' : 'opacity-0'} />
                            <path d="M 840 350 L 1000 350" stroke="#d4d4d8" strokeWidth="2" markerEnd="url(#arrow-active)" className={isHighlighted('link-gold-serve') ? 'opacity-100' : 'opacity-0'} />

                            {/* Control Flow Highlights */}
                            <path d="M 640 100 L 560 100" stroke="#a1a1aa" strokeWidth="2" strokeDasharray="4,4" markerEnd="url(#arrow-active)" className={isHighlighted('link-adf-meta') ? 'opacity-100' : 'opacity-0'} />
                            <path d="M 720 130 L 720 160 L 420 160 L 420 280" stroke="#a1a1aa" strokeWidth="2" strokeDasharray="4,4" fill="none" className={isHighlighted('link-adf-bronze') ? 'opacity-100' : 'opacity-0'} />
                            <path d="M 720 130 L 720 160 L 600 160 L 600 280" stroke="#a1a1aa" strokeWidth="2" strokeDasharray="4,4" fill="none" className={isHighlighted('link-adf-silver') ? 'opacity-100' : 'opacity-0'} />
                            <path d="M 720 130 L 720 160 L 780 160 L 780 280" stroke="#a1a1aa" strokeWidth="2" strokeDasharray="4,4" fill="none" className={isHighlighted('link-adf-gold') ? 'opacity-100' : 'opacity-0'} />
                        </g>
                    )}

                    {/* ANIMATED PARTICLES - Data Packets */}
                    <g className={hoveredNode ? 'opacity-10' : 'opacity-100'}>
                        {/* Source -> Bronze */}
                        <rect width="6" height="6" fill="#d4d4d8" rx="1">
                            <animateMotion dur="3s" repeatCount="indefinite" path="M 190 350 L 360 350" keyPoints="0;1" keyTimes="0;1" calcMode="linear" />
                        </rect>

                        {/* Bronze -> Silver */}
                        <rect width="6" height="6" fill="#d4d4d8" rx="1">
                            <animateMotion dur="3s" begin="0.8s" repeatCount="indefinite" path="M 480 350 L 540 350" keyPoints="0;1" keyTimes="0;1" calcMode="linear" />
                        </rect>

                        {/* Silver -> Gold */}
                        <rect width="6" height="6" fill="#d4d4d8" rx="1">
                            <animateMotion dur="3s" begin="1.6s" repeatCount="indefinite" path="M 660 350 L 720 350" keyPoints="0;1" keyTimes="0;1" calcMode="linear" />
                        </rect>

                        {/* Gold -> Serve */}
                        <rect width="6" height="6" fill="#d4d4d8" rx="1">
                            <animateMotion dur="3s" begin="2.4s" repeatCount="indefinite" path="M 840 350 L 1000 350" keyPoints="0;1" keyTimes="0;1" calcMode="linear" />
                        </rect>

                        {/* Control Signals */}
                        <circle r="2" fill="#a1a1aa"><animateMotion dur="4s" repeatCount="indefinite" path="M 640 100 L 560 100" /></circle>
                        <circle r="2" fill="#a1a1aa"><animateMotion dur="6s" repeatCount="indefinite" path="M 720 130 L 720 160 L 420 160 L 420 280" /></circle>
                    </g>

                    {/* NODES */}
                    {nodes.map((node) => {
                        const isDimmedState = isDimmed(node.id);
                        const isHoveredState = hoveredNode === node.id;

                        return (
                            <g
                                key={node.id}
                                transform={`translate(${node.x}, ${node.y})`}
                                onMouseEnter={() => setHoveredNode(node.id)}
                                onMouseLeave={() => setHoveredNode(null)}
                                className={`cursor-pointer transition-all duration-500 ${isDimmedState ? 'opacity-20 blur-[1px]' : 'opacity-100'}`}
                            >
                                {/* Node Background - Angular (rx=0) */}
                                <rect
                                    width={node.width}
                                    height={node.height}
                                    rx="0"
                                    fill="#0A0A0A"
                                    stroke={isHoveredState ? getNodeColor(node.color, monochrome) : "rgba(255,255,255,0.1)"}
                                    strokeWidth={isHoveredState ? 2 : 1}
                                    className="transition-all duration-300"
                                    style={{
                                        // filter property removed to improve performance on low-end devices
                                    }}
                                />

                                {/* Title */}
                                <text
                                    x={node.width / 2}
                                    y={30}
                                    textAnchor="middle"
                                    className="text-sm font-bold fill-white pointer-events-none tracking-tight"
                                    style={{ textShadow: '0 2px 10px rgba(0,0,0,0.8)' }}
                                >
                                    {node.title}
                                </text>

                                {/* Tech Badge */}
                                <rect
                                    x={node.width / 2 - 50}
                                    y={node.height - 28}
                                    width="100"
                                    height="18"
                                    rx="0"
                                    fill="rgba(255,255,255,0.03)"
                                    stroke="rgba(255,255,255,0.08)"
                                    strokeWidth="1"
                                />
                                <text
                                    x={node.width / 2}
                                    y={node.height - 16}
                                    textAnchor="middle"
                                    className="text-[9px] font-mono fill-white/50 pointer-events-none uppercase tracking-widest"
                                >
                                    {node.tech}
                                </text>

                                {/* Icons/Graphics - Meaningful & Angular */}
                                {node.type === 'source' && (
                                    <g transform="translate(0, 40)" opacity="0.9">
                                        {/* Database Cylinder (Angular) */}
                                        <path d={`M ${node.width / 2 - 25},20 L ${node.width / 2 - 5},20 L ${node.width / 2 - 5},45 L ${node.width / 2 - 25},45 Z`} fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <line x1={node.width / 2 - 25} y1="28" x2={node.width / 2 - 5} y2="28" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        {/* API/Stream Signal */}
                                        <path d={`M ${node.width / 2 + 5},30 L ${node.width / 2 + 15},20 L ${node.width / 2 + 25},30 L ${node.width / 2 + 15},40 Z`} fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <circle cx={node.width / 2 + 15} cy="30" r="2" fill={getNodeColor(node.color, monochrome)} />
                                    </g>
                                )}
                                {node.type === 'serve' && (
                                    <g transform="translate(0, 40)" opacity="0.9">
                                        {/* Dashboard Layout */}
                                        <rect x={node.width / 2 - 25} y="15" width="20" height="15" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 + 5} y="15" width="20" height="15" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 - 25} y="35" width="50" height="15" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        {/* Trend Line */}
                                        <path d={`M ${node.width / 2 - 20},45 L ${node.width / 2 - 5},40 L ${node.width / 2 + 5},42 L ${node.width / 2 + 20},38`} fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                    </g>
                                )}
                                {node.type === 'bronze' && (
                                    <g transform="translate(0, 40)" opacity="0.9">
                                        {/* Raw Data Pile: Unstructured blocks */}
                                        <rect x={node.width / 2 - 20} y="20" width="15" height="15" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 + 5} y="15" width="12" height="12" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 - 10} y="38" width="18" height="10" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 + 10} y="30" width="10" height="18" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                    </g>
                                )}
                                {node.type === 'silver' && (
                                    <g transform="translate(0, 40)" opacity="0.9">
                                        {/* Structured Grid: Clean table rows */}
                                        <rect x={node.width / 2 - 25} y="15" width="50" height="40" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <line x1={node.width / 2 - 25} y1="28" x2={node.width / 2 + 25} y2="28" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <line x1={node.width / 2 - 25} y1="41" x2={node.width / 2 + 25} y2="41" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <line x1={node.width / 2} y1="15" x2={node.width / 2} y2="55" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                    </g>
                                )}
                                {node.type === 'gold' && (
                                    <g transform="translate(0, 40)" opacity="0.9">
                                        {/* Star Schema: Central fact with dimensions */}
                                        <rect x={node.width / 2 - 8} y="28" width="16" height="16" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1.5" />
                                        {/* Dimensions */}
                                        <rect x={node.width / 2 - 25} y="15" width="10" height="10" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 + 15} y="15" width="10" height="10" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 - 25} y="45" width="10" height="10" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        <rect x={node.width / 2 + 15} y="45" width="10" height="10" fill="none" stroke={getNodeColor(node.color, monochrome)} strokeWidth="1" />
                                        {/* Links */}
                                        <line x1={node.width / 2 - 8} y1="28" x2={node.width / 2 - 15} y2="25" stroke={getNodeColor(node.color, monochrome)} strokeWidth="0.5" />
                                        <line x1={node.width / 2 + 8} y1="28" x2={node.width / 2 + 15} y2="25" stroke={getNodeColor(node.color, monochrome)} strokeWidth="0.5" />
                                        <line x1={node.width / 2 - 8} y1="44" x2={node.width / 2 - 15} y2="45" stroke={getNodeColor(node.color, monochrome)} strokeWidth="0.5" />
                                        <line x1={node.width / 2 + 8} y1="44" x2={node.width / 2 + 15} y2="45" stroke={getNodeColor(node.color, monochrome)} strokeWidth="0.5" />
                                    </g>
                                )}
                            </g>
                        );
                    })}
                </svg>
            </div>

            {/* FLOATING TOOLTIP - Glassmorphism (Hidden on Mobile) */}
            <AnimatePresence>
                {hoveredNode && (
                    <motion.div
                        initial={{ opacity: 0, y: 10, scale: 0.95 }}
                        animate={{ opacity: 1, y: 0, scale: 1 }}
                        exit={{ opacity: 0, y: 10, scale: 0.95 }}
                        transition={{ duration: 0.2 }}
                        className="hidden md:block absolute z-50 w-80 bg-black/90 backdrop-blur-2xl border border-white/10 rounded-none p-6 shadow-[0_20px_50px_rgba(0,0,0,0.8)] pointer-events-none"
                        style={{
                            left: mousePos.x,
                            top: mousePos.y - 20,
                            transform: `translate(${hoveredNode === 'serve' ? '-100%' :
                                hoveredNode === 'source' ? '0%' :
                                    containerRef.current && mousePos.x > containerRef.current.offsetWidth * 0.6
                                        ? '-100%'
                                        : containerRef.current && mousePos.x < containerRef.current.offsetWidth * 0.4
                                            ? '0%'
                                            : '-50%'
                                }, -100%)`,
                            marginLeft: hoveredNode === 'serve' ? '-20px' : hoveredNode === 'source' ? '20px' : containerRef.current && mousePos.x > containerRef.current.offsetWidth * 0.6 ? '-20px' : containerRef.current && mousePos.x < containerRef.current.offsetWidth * 0.4 ? '20px' : '0'
                        }}
                    >
                        {(() => {
                            const node = nodes.find(n => n.id === hoveredNode);
                            if (!node) return null;
                            return (
                                <>
                                    <div className="flex items-center gap-3 mb-4 border-b border-white/5 pb-4">
                                        <div className={`w-2 h-2 rounded-none`} style={{ backgroundColor: getNodeColor(node.color, monochrome) }} />
                                        <h3 className="text-lg font-bold text-white font-display tracking-tight">{node.title}</h3>
                                    </div>
                                    <p className="text-white/60 text-sm mb-6 leading-relaxed font-light">
                                        {node.desc}
                                    </p>
                                    <div className="flex justify-between items-center">
                                        <span className="text-[10px] text-white/30 uppercase tracking-widest font-bold">Technology</span>
                                        <span className="text-xs bg-white/5 text-white/80 px-3 py-1.5 rounded-none font-mono border border-white/10">
                                            {node.tech}
                                        </span>
                                    </div>
                                </>
                            );
                        })()}
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}

function getNodeColor(color: string, monochrome: boolean = false) {
    if (monochrome) {
        switch (color) {
            case 'blue': return '#52525b'; // zinc-600
            case 'amber': return '#71717a'; // zinc-500
            case 'slate': return '#a1a1aa'; // zinc-400
            case 'yellow': return '#d4d4d8'; // zinc-300
            case 'emerald': return '#52525b';
            case 'indigo': return '#71717a';
            case 'violet': return '#a1a1aa';
            case 'zinc': return '#52525b';
            case 'neutral': return '#71717a';
            case 'stone': return '#a1a1aa';
            default: return '#71717a';
        }
    }

    // Original Colors (Restored)
    switch (color) {
        case 'blue': return '#3b82f6';
        case 'amber': return '#f59e0b';
        case 'slate': return '#64748b';
        case 'yellow': return '#eab308';
        case 'emerald': return '#10b981';
        case 'indigo': return '#6366f1';
        case 'violet': return '#8b5cf6';
        case 'zinc': return '#71717a';
        case 'neutral': return '#737373';
        case 'stone': return '#78716c';
        default: return '#71717a';
    }
}
