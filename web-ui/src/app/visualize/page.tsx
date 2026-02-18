
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
        <div className="flex h-screen bg-[#1e1f22] text-[#bcbec4] font-sans overflow-hidden">
            {!isFullscreen && <Sidebar />}

            <div className="flex-1 flex flex-col min-w-0">
                {/* ─── Top Bar ─── */}
                <div className="h-9 bg-[#2b2d30] border-b border-[#393b40] flex items-center px-4 justify-between shrink-0">
                    <div className="flex items-center gap-3 text-[12px]">
                        <BarChart3 className="w-3.5 h-3.5 text-[#e5c07b]" />
                        <span className="text-[#bcbec4] font-medium">Data Visualization</span>
                        <span className="text-[#6c707e]">·</span>
                        <span className="text-[#6c707e]">Apache Superset</span>
                        <span className="text-[#6c707e]">·</span>

                        {/* Status Badge */}
                        {status === 'checking' ? (
                            <div className="flex items-center gap-1 text-[#6c707e]">
                                <Loader2 className="w-3 h-3 animate-spin" />
                                <span className="text-[10px]">Checking...</span>
                            </div>
                        ) : status === 'online' ? (
                            <div className="flex items-center gap-1 text-[#6aab73]">
                                <CheckCircle2 className="w-3 h-3" />
                                <span className="text-[10px]">Connected</span>
                            </div>
                        ) : (
                            <div className="flex items-center gap-1 text-[#ff5261]">
                                <AlertCircle className="w-3 h-3" />
                                <span className="text-[10px]">Offline</span>
                            </div>
                        )}
                    </div>

                    <div className="flex items-center gap-1.5">
                        <button
                            onClick={checkHealth}
                            className="p-1 hover:bg-[#393b40] rounded text-[#6c707e] hover:text-[#bcbec4] transition-colors"
                            title="Check connection"
                        >
                            <RefreshCw className="w-3.5 h-3.5" />
                        </button>
                        <button
                            onClick={() => setIsFullscreen(!isFullscreen)}
                            className="p-1 hover:bg-[#393b40] rounded text-[#6c707e] hover:text-[#bcbec4] transition-colors"
                            title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
                        >
                            {isFullscreen ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
                        </button>
                        <a
                            href={SUPERSET_URL}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="p-1 hover:bg-[#393b40] rounded text-[#6c707e] hover:text-[#bcbec4] transition-colors"
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
                                    <BarChart3 className="w-8 h-8 text-[#e5c07b]" />
                                </div>
                                <div>
                                    <h2 className="text-[16px] font-semibold text-white mb-2">Superset is not running</h2>
                                    <p className="text-[12px] text-[#6c707e] mb-4">
                                        Start Apache Superset to create and view dashboards from your Iceberg lakehouse data.
                                    </p>
                                    <div className="bg-[#2b2d30] border border-[#393b40] rounded-lg p-3 text-left">
                                        <p className="text-[10px] text-[#6c707e] uppercase tracking-wider mb-2">Start command</p>
                                        <code className="text-[11px] text-[#e5c07b] font-mono">
                                            docker compose up -d superset
                                        </code>
                                    </div>
                                </div>
                                <button
                                    onClick={() => { checkHealth(); setIframeKey(k => k + 1); }}
                                    className="px-4 py-1.5 bg-[#e5c07b]/15 text-[#e5c07b] rounded text-[11px] font-medium hover:bg-[#e5c07b]/25 transition-colors"
                                >
                                    Retry Connection
                                </button>
                            </div>
                        </div>
                    ) : status === 'checking' ? (
                        <div className="absolute inset-0 flex items-center justify-center">
                            <Loader2 className="w-6 h-6 text-[#e5c07b] animate-spin" />
                        </div>
                    ) : (
                        <iframe
                            key={iframeKey}
                            src={SUPERSET_URL}
                            className="w-full h-full border-0"
                            title="Apache Superset"
                            sandbox="allow-same-origin allow-scripts allow-popups allow-forms allow-modals"
                        />
                    )}
                </div>
            </div>
        </div>
    );
}
