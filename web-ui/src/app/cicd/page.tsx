
import React, { useState, useEffect, useCallback } from 'react';
import {
    GitPullRequest, ExternalLink, RefreshCw, Loader2, AlertCircle,
    CheckCircle2, Maximize2, Minimize2
} from 'lucide-react';
import clsx from 'clsx';
import { Sidebar } from '@/components/Sidebar';

const GITEA_URL = 'http://localhost:3030';
const GITEA_PROXY_URL = '/gitea-proxy';

export default function CICDPage() {
    const [status, setStatus] = useState<'checking' | 'online' | 'offline'>('checking');
    const [isFullscreen, setIsFullscreen] = useState(false);
    const [iframeKey, setIframeKey] = useState(0);

    const checkHealth = useCallback(async () => {
        setStatus('checking');
        try {
            await fetch(`${GITEA_URL}/api/v1/version`, {
                mode: 'no-cors',
                signal: AbortSignal.timeout(5000),
            });
            setStatus('online');
        } catch {
            setStatus('offline');
        }
    }, []);

    useEffect(() => { checkHealth(); }, [checkHealth]);

    const iframeRef = React.useRef<HTMLIFrameElement>(null);

    const injectStyles = useCallback(() => {
        if (!iframeRef.current) return;
        try {
            const doc = iframeRef.current.contentDocument;
            if (!doc) return;

            // Create style element
            if (!doc.getElementById('gitea-custom-style')) {
                const style = doc.createElement('style');
                style.id = 'gitea-custom-style';
                style.textContent = `
                    .page-footer, .ui.warning.message {
                        display: none !important;
                    }
                    /* Hide the Gitea Logo/Brand in the top left, but keep the rest of navbar */
                    .following.bar .brand, #navbar-logo {
                        display: none !important;
                    }
                    /* Remove padding from body to fit iframe perfectly */
                    body {
                        padding-top: 0 !important;
                    }
                    /* Adjust top padding since navbar is fixed/sticky */
                    .page-content {
                        padding-top: 0px !important;
                    }
                    /* Make navbar blend in or look seamless if needed */
                    .following.bar {
                        border-bottom: 1px solid #393b40 !important;
                    }
                `;
                doc.head.appendChild(style);
            }
        } catch (e) {
            console.error("Cannot inject styles into cross-origin iframe (or other error)", e);
        }
    }, []);

    // Re-inject styles when path changes (if using client navigation)
    useEffect(() => {
        const interval = setInterval(injectStyles, 1000);
        return () => clearInterval(interval);
    }, [injectStyles]);

    return (
        <div className="flex h-screen bg-[#1e1f22] text-[#bcbec4] font-sans overflow-hidden">
            {!isFullscreen && <Sidebar />}

            <div className="flex-1 flex flex-col min-w-0">
                {/* ─── Top Bar ─── */}
                <div className="h-9 bg-[#2b2d30] border-b border-[#393b40] flex items-center px-4 justify-between shrink-0">
                    <div className="flex items-center gap-3 text-[12px]">
                        <GitPullRequest className="w-3.5 h-3.5 text-[#e06c75]" />
                        <span className="text-[#bcbec4] font-medium">CI/CD</span>
                        <span className="text-[#6c707e]">·</span>
                        <span className="text-[#6c707e]">Gitea</span>
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
                            href={GITEA_URL}
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
                                <div className="w-16 h-16 rounded-2xl bg-[#e06c75]/10 flex items-center justify-center">
                                    <GitPullRequest className="w-8 h-8 text-[#e06c75]" />
                                </div>
                                <div>
                                    <h2 className="text-[16px] font-semibold text-white mb-2">Gitea is not running</h2>
                                    <p className="text-[12px] text-[#6c707e] mb-4">
                                        Start Gitea to manage Git repositories and CI/CD pipelines with GitHub-compatible Actions.
                                    </p>
                                    <div className="bg-[#2b2d30] border border-[#393b40] rounded-lg p-3 text-left">
                                        <p className="text-[10px] text-[#6c707e] uppercase tracking-wider mb-2">Start command</p>
                                        <code className="text-[11px] text-[#e06c75] font-mono">
                                            docker compose up -d gitea gitea-runner
                                        </code>
                                    </div>
                                </div>
                                <button
                                    onClick={() => { checkHealth(); setIframeKey(k => k + 1); }}
                                    className="px-4 py-1.5 bg-[#e06c75]/15 text-[#e06c75] rounded text-[11px] font-medium hover:bg-[#e06c75]/25 transition-colors"
                                >
                                    Retry Connection
                                </button>
                            </div>
                        </div>
                    ) : status === 'checking' ? (
                        <div className="absolute inset-0 flex items-center justify-center">
                            <Loader2 className="w-6 h-6 text-[#e06c75] animate-spin" />
                        </div>
                    ) : (
                        <iframe
                            ref={iframeRef}
                            key={iframeKey}
                            src={GITEA_PROXY_URL}
                            className="w-full h-full border-0"
                            title="Gitea"
                            onLoad={injectStyles}
                        />
                    )}
                </div>
            </div>
        </div>
    );
}
