
import React, { useState, useEffect, useCallback } from 'react';
import {
    BarChart3, ExternalLink, RefreshCw, Loader2, AlertCircle,
    CheckCircle2, Maximize2, Minimize2
} from 'lucide-react';
import clsx from 'clsx';
import { Sidebar } from '@/components/Sidebar';

const SUPERSET_URL = 'http://localhost:8089';

export default function VisualizePage() {
    const [status, setStatus] = useState<'checking' | 'online' | 'offline'>('checking');
    const [isFullscreen, setIsFullscreen] = useState(false);
    const [iframeKey, setIframeKey] = useState(0);

    const checkHealth = useCallback(async () => {
        setStatus('checking');
        try {
            const res = await fetch(`${SUPERSET_URL}/health`, {
                mode: 'no-cors',
                signal: AbortSignal.timeout(5000),
            });
            // no-cors returns opaque response, but if it doesn't throw, server is up
            setStatus('online');
        } catch {
            setStatus('offline');
        }
    }, []);

    useEffect(() => { checkHealth(); }, [checkHealth]);

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>

            {/* Ambient Lighting */}
            <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
            <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 -translate-y-1/4 z-0" />

            {!isFullscreen && (
                <div className="relative z-10 shrink-0">
                    <Sidebar />
                </div>
            )}

            <main className="flex-1 flex flex-col min-w-0 bg-transparent relative z-10">
                {/* ─── Top Bar ─── */}
                <div className="h-9 bg-black/40 backdrop-blur-md border-b border-white/5 flex items-center px-4 justify-between shrink-0 mb-0">
                    <div className="flex items-center gap-3 text-[12px]">
                        <BarChart3 className="w-3.5 h-3.5 text-obsidian-warning drop-shadow-[0_0_8px_rgba(255,203,107,0.5)]" />
                        <span className="text-white font-bold tracking-wide">Data Visualization</span>
                        <span className="text-obsidian-muted/40">·</span>
                        <span className="text-obsidian-muted font-medium">Apache Superset</span>
                        <span className="text-obsidian-muted/40">·</span>

                        {/* Status Badge */}
                        {status === 'checking' ? (
                            <div className="flex items-center gap-1 text-obsidian-muted">
                                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                <span className="text-[10px]">Checking...</span>
                            </div>
                        ) : status === 'online' ? (
                            <div className="flex items-center gap-1 text-obsidian-success">
                                <CheckCircle2 className="w-3.5 h-3.5" />
                                <span className="text-[10px]">Connected</span>
                            </div>
                        ) : (
                            <div className="flex items-center gap-1 text-obsidian-danger">
                                <AlertCircle className="w-3.5 h-3.5" />
                                <span className="text-[10px]">Offline</span>
                            </div>
                        )}
                    </div>

                    <div className="flex items-center gap-1.5">
                        <button
                            onClick={checkHealth}
                            className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground transition-colors"
                            title="Check connection"
                        >
                            <RefreshCw className="w-3.5 h-3.5" />
                        </button>
                        <button
                            onClick={() => setIsFullscreen(!isFullscreen)}
                            className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground transition-colors"
                            title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
                        >
                            {isFullscreen ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
                        </button>
                        <a
                            href={SUPERSET_URL}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-1 hover:bg-obsidian-panel-hover rounded text-obsidian-muted hover:text-foreground transition-colors"
                            title="Open in new tab"
                        >
                            <ExternalLink className="w-3.5 h-3.5" />
                        </a>
                    </div>
                </div>

                {/* ─── Main Content ─── */}
                <div className="flex-1 relative">
                    {status === 'offline' ? (
                        <div className="absolute inset-0 flex items-center justify-center">
                            <div className="flex flex-col items-center gap-4 text-center max-w-md">
                                <div className="w-16 h-16 rounded-2xl bg-[#e5c07b]/10 flex items-center justify-center">
                                    <BarChart3 className="w-8 h-8 text-obsidian-warning" />
                                </div>
                                <div>
                                    <h2 className="text-[16px] font-semibold text-white mb-2">Superset is not running</h2>
                                    <p className="text-[12px] text-obsidian-muted mb-4">
                                        Start Apache Superset to create and view dashboards from your Iceberg lakehouse data.
                                    </p>
                                    <div className="bg-obsidian-panel border border-obsidian-border rounded-lg p-3 text-left">
                                        <p className="text-[10px] text-obsidian-muted uppercase tracking-wider mb-2">Start command</p>
                                        <code className="text-[11px] text-obsidian-warning font-mono bg-black/40 px-2 py-1 rounded inline-block">
                                            docker compose up -d superset
                                        </code>
                                    </div>
                                </div>
                                <button
                                    onClick={() => { checkHealth(); setIframeKey(k => k + 1); }}
                                    className="px-4 py-1.5 bg-[#e5c07b]/15 border border-[#e5c07b]/30 text-[#e5c07b] rounded text-[11px] font-bold tracking-wide hover:bg-[#e5c07b]/25 transition-colors active:scale-95 shadow-[0_0_12px_rgba(229,192,123,0.1)]"
                                >
                                    Retry Connection
                                </button>
                            </div>
                        </div>
                    ) : status === 'checking' ? (
                        <div className="absolute inset-0 flex items-center justify-center">
                            <Loader2 className="w-6 h-6 text-[#e5c07b] animate-spin drop-shadow-[0_0_8px_rgba(229,192,123,0.5)]" />
                        </div>
                    ) : (
                        <iframe
                            key={iframeKey}
                            src={SUPERSET_URL}
                            className="w-full h-full border-0 rounded-tl-xl overflow-hidden bg-black/20"
                            title="Apache Superset"
                            sandbox="allow-same-origin allow-scripts allow-popups allow-forms allow-modals"
                        />
                    )}
                </div>
            </main>
        </div>
    );
}
