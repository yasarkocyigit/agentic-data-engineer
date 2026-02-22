import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Sidebar } from '@/components/Sidebar';
import clsx from 'clsx';
import { LayoutPanelLeft, Plug, PlugZap, RefreshCw } from 'lucide-react';
import { Terminal } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';

type ConnState = 'disconnected' | 'connecting' | 'connected';

export default function DockerCliPage() {
    const terminalHostRef = useRef<HTMLDivElement | null>(null);
    const terminalRef = useRef<Terminal | null>(null);
    const fitAddonRef = useRef<FitAddon | null>(null);
    const wsRef = useRef<WebSocket | null>(null);
    const resizeObserverRef = useRef<ResizeObserver | null>(null);
    const dataDisposableRef = useRef<{ dispose: () => void } | null>(null);

    const [containerName, setContainerName] = useState('openclaw_api');
    const [shellName, setShellName] = useState('bash');
    const [state, setState] = useState<ConnState>('disconnected');

    const closeSocket = useCallback(() => {
        const ws = wsRef.current;
        wsRef.current = null;
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
            ws.close();
        }
    }, []);

    const sendResize = useCallback(() => {
        const ws = wsRef.current;
        const term = terminalRef.current;
        if (!ws || !term || ws.readyState !== WebSocket.OPEN) return;

        ws.send(JSON.stringify({
            type: 'resize',
            rows: term.rows,
            cols: term.cols,
        }));
    }, []);

    const connectTerminal = useCallback(() => {
        const term = terminalRef.current;
        const fitAddon = fitAddonRef.current;
        if (!term || !fitAddon) return;

        closeSocket();
        setState('connecting');

        fitAddon.fit();
        const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const params = new URLSearchParams();
        if (containerName.trim()) params.set('container', containerName.trim());
        if (shellName.trim()) params.set('shell', shellName.trim());
        params.set('rows', String(term.rows));
        params.set('cols', String(term.cols));

        const ws = new WebSocket(`${wsProtocol}://${window.location.host}/api/docker/pty?${params.toString()}`);
        wsRef.current = ws;

        ws.onopen = () => {
            setState('connected');
            term.writeln('\r\n[connected] interactive PTY ready');
            sendResize();
        };

        ws.onmessage = (event) => {
            if (typeof event.data === 'string') {
                term.write(event.data);
            }
        };

        ws.onerror = () => {
            term.writeln('\r\n[error] terminal websocket connection failed');
        };

        ws.onclose = (event) => {
            setState('disconnected');
            term.writeln(`\r\n[disconnected] code=${event.code}`);
            if (wsRef.current === ws) wsRef.current = null;
        };
    }, [closeSocket, containerName, sendResize, shellName]);

    useEffect(() => {
        if (!terminalHostRef.current || terminalRef.current) return;

        const terminal = new Terminal({
            cursorBlink: true,
            convertEol: false,
            fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
            fontSize: 13,
            lineHeight: 1.2,
            theme: {
                background: '#05070a',
                foreground: '#d6d8dd',
                cursor: '#64b5f6',
                black: '#111318',
                brightBlack: '#575b66',
                brightBlue: '#64b5f6',
                brightCyan: '#4dd0e1',
                brightGreen: '#81c784',
                brightMagenta: '#ba68c8',
                brightRed: '#ef5350',
                brightWhite: '#eceff1',
                brightYellow: '#ffd54f',
                blue: '#42a5f5',
                cyan: '#26c6da',
                green: '#66bb6a',
                magenta: '#ab47bc',
                red: '#e57373',
                white: '#b0bec5',
                yellow: '#ffca28',
            },
        });
        const fitAddon = new FitAddon();
        terminal.loadAddon(fitAddon);
        terminal.open(terminalHostRef.current);
        fitAddon.fit();
        terminal.writeln('OpenClaw Docker Terminal');
        terminal.writeln('Type commands directly. Press reconnect if connection drops.\r\n');

        const dataDisposable = terminal.onData((data) => {
            const ws = wsRef.current;
            if (!ws || ws.readyState !== WebSocket.OPEN) return;
            ws.send(data);
        });
        dataDisposableRef.current = dataDisposable;

        const observer = new ResizeObserver(() => {
            fitAddon.fit();
            sendResize();
        });
        observer.observe(terminalHostRef.current);
        resizeObserverRef.current = observer;

        terminalRef.current = terminal;
        fitAddonRef.current = fitAddon;

        return () => {
            closeSocket();
            dataDisposableRef.current?.dispose();
            dataDisposableRef.current = null;
            resizeObserverRef.current?.disconnect();
            resizeObserverRef.current = null;
            terminalRef.current?.dispose();
            terminalRef.current = null;
            fitAddonRef.current = null;
        };
    }, [closeSocket, sendResize]);

    useEffect(() => {
        if (!terminalRef.current) return;
        connectTerminal();
    }, [connectTerminal]);

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative">
            <Sidebar />
            <main className="flex-1 flex flex-col min-w-0 overflow-hidden relative z-10">
                <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-obsidian-purple/[0.04] rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
                <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-obsidian-info/[0.03] rounded-full blur-[100px] pointer-events-none translate-x-1/4 translate-y-1/4 z-0" />

                <header className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 w-full relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-obsidian-info/30 to-transparent opacity-50"></div>
                    <div className="flex items-center gap-3">
                        <button
                            onClick={() => window.dispatchEvent(new CustomEvent('openclaw:toggle-sidebar'))}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-obsidian-border/50"
                            title="Toggle Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]" />
                        </button>
                        <div className="w-[1px] h-4 bg-obsidian-border/50"></div>
                        <span className="text-[12px] font-bold text-foreground">Docker Terminal</span>
                        <span className="text-[10px] text-obsidian-muted ml-2">Interactive PTY over WebSocket</span>
                    </div>
                    <div className="flex items-center gap-2 text-[11px]">
                        <span className={clsx(
                            'inline-flex items-center gap-1 px-2 py-1 rounded border',
                            state === 'connected' && 'text-emerald-300 border-emerald-500/30 bg-emerald-500/10',
                            state === 'connecting' && 'text-amber-300 border-amber-500/30 bg-amber-500/10',
                            state === 'disconnected' && 'text-obsidian-muted border-white/10 bg-white/[0.02]'
                        )}>
                            {state === 'connected' ? <PlugZap className="w-3.5 h-3.5" /> : <Plug className="w-3.5 h-3.5" />}
                            {state}
                        </span>
                    </div>
                </header>

                <section className="flex-1 min-h-0 p-4 z-10 relative">
                    <div className="h-full rounded-md border border-white/10 bg-black/35 backdrop-blur-sm flex flex-col overflow-hidden">
                        <div className="p-3 border-b border-white/10 flex items-center gap-2">
                            <input
                                value={containerName}
                                onChange={(e) => setContainerName(e.target.value)}
                                placeholder="container name (leave empty for local shell)"
                                className="h-9 w-72 rounded border border-white/10 bg-black/40 px-3 text-[12px] font-mono text-foreground focus:outline-none focus:border-obsidian-info/60"
                            />
                            <input
                                value={shellName}
                                onChange={(e) => setShellName(e.target.value)}
                                placeholder="shell (bash/sh)"
                                className="h-9 w-36 rounded border border-white/10 bg-black/40 px-3 text-[12px] font-mono text-foreground focus:outline-none focus:border-obsidian-info/60"
                            />
                            <button
                                onClick={connectTerminal}
                                className="h-9 px-3 rounded border border-white/10 text-[12px] text-foreground/90 hover:bg-white/[0.05] transition-all inline-flex items-center gap-1.5"
                            >
                                <RefreshCw className="w-3.5 h-3.5" />
                                Reconnect
                            </button>
                        </div>

                        <div ref={terminalHostRef} className="flex-1 min-h-0 p-2 bg-[#05070a]" />
                    </div>
                </section>
            </main>
        </div>
    );
}
