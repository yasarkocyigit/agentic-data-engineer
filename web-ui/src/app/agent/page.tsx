
import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Sidebar } from '@/components/Sidebar';
import { Bot, Send, Terminal, Trash2, LayoutPanelLeft, Cpu, Zap, Database, GitBranch, Clock, Activity, CheckCircle2, Circle, Loader2, Sparkles, Wrench, Code2, BarChart3, FileSearch } from 'lucide-react';
import clsx from 'clsx';

/* ── Types ── */
interface ChatMessage {
    role: 'system' | 'user' | 'assistant';
    content: string;
    timestamp: string;
    toolCall?: { name: string; status: 'running' | 'done' | 'error'; output?: string };
}

/* ── Agent Capabilities ── */
const CAPABILITIES = [
    { icon: <GitBranch className="w-3.5 h-3.5" />, label: 'Deploy Pipelines', desc: 'Airflow DAGs' },
    { icon: <Database className="w-3.5 h-3.5" />, label: 'Query Data', desc: 'Trino / Spark SQL' },
    { icon: <FileSearch className="w-3.5 h-3.5" />, label: 'Data Lineage', desc: 'Marquez tracking' },
    { icon: <BarChart3 className="w-3.5 h-3.5" />, label: 'Build Reports', desc: 'Superset dashboards' },
    { icon: <Code2 className="w-3.5 h-3.5" />, label: 'Debug Issues', desc: 'Log analysis' },
    { icon: <Wrench className="w-3.5 h-3.5" />, label: 'Manage Infra', desc: 'Docker containers' },
];

export default function AgentPage() {
    const [messages, setMessages] = useState<ChatMessage[]>([
        {
            role: 'system',
            content: 'Hello! I am ClawdBot, your AI Data Engineer. I can deploy pipelines, optimize queries, manage infrastructure, and debug issues across your entire data stack.',
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        },
    ]);
    const [input, setInput] = useState('');
    const [isThinking, setIsThinking] = useState(false);
    const chatEndRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const scrollToBottom = useCallback(() => {
        chatEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, []);

    useEffect(() => {
        scrollToBottom();
    }, [messages, scrollToBottom]);

    const handleSend = useCallback(() => {
        const trimmed = input.trim();
        if (!trimmed || isThinking) return;

        const userMsg: ChatMessage = {
            role: 'user',
            content: trimmed,
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        };

        setMessages(prev => [...prev, userMsg]);
        setInput('');
        setIsThinking(true);

        // Simulate agent response
        setTimeout(() => {
            const response: ChatMessage = {
                role: 'assistant',
                content: `I'll look into "${trimmed}" for you. Let me check the relevant services and data sources.`,
                timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
                toolCall: { name: 'check_airflow_status', status: 'done', output: 'All DAGs healthy. 3 active runs.' },
            };
            setMessages(prev => [...prev, response]);
            setIsThinking(false);
        }, 1500);
    }, [input, isThinking]);

    const clearChat = useCallback(() => {
        setMessages([{
            role: 'system',
            content: 'Chat cleared. How can I help you?',
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        }]);
    }, []);

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>
            <Sidebar />
            <main className="flex-1 flex flex-col min-w-0 overflow-hidden bg-transparent relative z-10">

                {/* Background ambient light */}
                <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
                <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 translate-y-1/4 z-0" />

                {/* Header */}
                <header className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/60 border-b border-white/5 z-10 w-full relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-obsidian-info/30 to-obsidian-purple/30" />
                    <div className="flex items-center gap-3">
                        <button
                            onClick={() => window.dispatchEvent(new CustomEvent('openclaw:toggle-sidebar'))}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-white/10"
                            title="Toggle Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4" />
                        </button>
                        <div className="w-[1px] h-4 bg-white/10 mx-1" />
                        <Terminal className="w-4 h-4 text-obsidian-purple/80" />
                        <span className="text-[12px] font-bold text-foreground/90 uppercase tracking-widest">Agent Control Center</span>
                    </div>
                    <div className="flex items-center gap-3">
                        <span className="text-[10px] text-obsidian-muted/60 font-mono">
                            {messages.length} messages
                        </span>
                        <div className="flex items-center px-3 py-1 bg-white/[0.03] border border-white/10 rounded-md text-[11px] font-medium text-foreground/80 gap-2 transition-all">
                            <span className="w-1.5 h-1.5 rounded-full bg-obsidian-success animate-pulse" />
                            ClawdBot: ONLINE
                        </div>
                    </div>
                </header>

                {/* ─── Overview Cards ─── */}
                <div className="bg-black/20 backdrop-blur-xl border-b border-white/5 shrink-0 z-10 relative">
                    <div className="grid grid-cols-5 divide-x divide-white/5">
                        {/* Agent Status */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2 whitespace-nowrap">
                                <Bot className="w-3.5 h-3.5 shrink-0" /> Agent status
                            </div>
                            <div className="text-xl font-mono font-medium text-foreground flex items-center gap-2">
                                <Circle className="w-2.5 h-2.5 fill-obsidian-success text-obsidian-success" />
                                Active
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">ClawdBot v1.0</div>
                        </div>

                        {/* Model */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2 whitespace-nowrap">
                                <Sparkles className="w-3.5 h-3.5 shrink-0" /> Model
                            </div>
                            <div className="text-xl font-mono font-medium text-foreground">
                                Claude 4
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">Anthropic</div>
                        </div>

                        {/* Connected Services */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2 whitespace-nowrap">
                                <Database className="w-3.5 h-3.5 shrink-0" /> Connected services
                            </div>
                            <div className="text-xl font-mono font-medium text-foreground">
                                7
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">MCP tools available</div>
                        </div>

                        {/* Tasks Completed */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2 whitespace-nowrap">
                                <CheckCircle2 className="w-3.5 h-3.5 shrink-0" /> Tasks completed
                            </div>
                            <div className="text-xl font-mono font-medium text-foreground">
                                {messages.filter(m => m.role === 'assistant').length}
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">this session</div>
                        </div>

                        {/* Uptime */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2 whitespace-nowrap">
                                <Clock className="w-3.5 h-3.5 shrink-0" /> Session uptime
                            </div>
                            <div className="text-xl font-mono font-medium text-foreground">
                                Active
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">since page load</div>
                        </div>
                    </div>
                </div>

                {/* ─── Main Content ─── */}
                <div className="flex-1 flex min-h-0 z-10 relative">

                    {/* Chat Area */}
                    <div className="flex-[3] flex flex-col border-r border-white/5 relative">
                        <div className="flex-1 overflow-auto px-6 py-6 space-y-6 custom-scrollbar">
                            {messages.map((msg, i) => (
                                <div key={i} className={clsx(
                                    "flex items-start gap-4",
                                    msg.role === 'user' && "flex-row-reverse"
                                )}>
                                    {/* Avatar */}
                                    {msg.role === 'user' ? (
                                        <div className="w-7 h-7 rounded-md bg-gradient-to-br from-[#bcbec4]/80 to-[#6c707e]/80 flex items-center justify-center text-[10px] text-obsidian-bg font-bold shrink-0">YK</div>
                                    ) : (
                                        <Bot className="w-5 h-5 text-obsidian-info/70 mt-0.5 shrink-0" />
                                    )}

                                    <div className={clsx(
                                        "flex flex-col gap-1.5 max-w-[70%]",
                                        msg.role === 'user' && "items-end"
                                    )}>
                                        <div className="flex items-center gap-2">
                                            <span className="text-[10px] text-obsidian-muted/50 font-medium uppercase tracking-widest">
                                                {msg.role === 'user' ? 'You' : msg.role === 'system' ? 'System' : 'ClawdBot'}
                                            </span>
                                            <span className="text-[10px] text-obsidian-muted/30 font-mono">{msg.timestamp}</span>
                                        </div>

                                        <div className={clsx(
                                            "text-[13px] leading-relaxed rounded-lg px-4 py-3",
                                            msg.role === 'user'
                                                ? "bg-white/[0.04] border border-white/[0.06] text-foreground/90"
                                                : "text-foreground/80"
                                        )}>
                                            {msg.content}
                                        </div>

                                        {/* Tool Execution Block */}
                                        {msg.toolCall && (
                                            <div className="bg-white/[0.02] border border-white/5 rounded-lg px-3 py-2 mt-1 w-full">
                                                <div className="flex items-center gap-2 text-[10px]">
                                                    <Wrench className="w-3 h-3 text-obsidian-muted/50" />
                                                    <span className="text-obsidian-muted/70 font-mono">{msg.toolCall.name}</span>
                                                    <span className={clsx(
                                                        "ml-auto px-1.5 py-0.5 rounded text-[9px] font-medium",
                                                        msg.toolCall.status === 'done' ? "text-obsidian-success/80 bg-obsidian-success/10" :
                                                            msg.toolCall.status === 'running' ? "text-obsidian-warning/80 bg-obsidian-warning/10" :
                                                                "text-obsidian-danger/80 bg-obsidian-danger/10"
                                                    )}>
                                                        {msg.toolCall.status}
                                                    </span>
                                                </div>
                                                {msg.toolCall.output && (
                                                    <div className="text-[11px] text-obsidian-muted/60 font-mono mt-1.5 pl-5">
                                                        → {msg.toolCall.output}
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))}

                            {/* Thinking indicator */}
                            {isThinking && (
                                <div className="flex items-start gap-4">
                                    <Bot className="w-5 h-5 text-obsidian-info/70 mt-0.5 shrink-0" />
                                    <div className="flex items-center gap-2 text-foreground/40 text-[13px]">
                                        <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                        <span className="font-mono">Thinking...</span>
                                    </div>
                                </div>
                            )}

                            <div ref={chatEndRef} />
                        </div>

                        {/* Input Area */}
                        <div className="p-4">
                            <div className="h-12 bg-white/[0.03] backdrop-blur-md border border-white/[0.06] rounded-lg flex items-center px-4 gap-3 transition-all focus-within:border-white/15 focus-within:bg-white/[0.05]">
                                <div className="text-foreground/30 font-mono text-sm">{'>'}</div>
                                <input
                                    ref={inputRef}
                                    type="text"
                                    value={input}
                                    onChange={(e) => setInput(e.target.value)}
                                    onKeyDown={(e) => e.key === 'Enter' && handleSend()}
                                    className="flex-1 bg-transparent border-none outline-none text-foreground/90 font-mono text-[13px] placeholder-obsidian-muted/40"
                                    placeholder="Ask ClawdBot to deploy a pipeline, query data, or debug an issue..."
                                    disabled={isThinking}
                                />
                                <button
                                    onClick={handleSend}
                                    disabled={isThinking || !input.trim()}
                                    className={clsx(
                                        "p-2 rounded-md transition-all active:scale-90",
                                        input.trim() && !isThinking
                                            ? "hover:bg-white/10 text-foreground/70 hover:text-foreground"
                                            : "text-obsidian-muted/30 cursor-not-allowed"
                                    )}
                                >
                                    <Send className="w-4 h-4" />
                                </button>
                            </div>
                        </div>
                    </div>

                    {/* Right Panel */}
                    <div className="flex-[1.2] flex flex-col bg-black/20 backdrop-blur-xl relative">

                        {/* Agent Capabilities */}
                        <div className="border-b border-white/5">
                            <div className="h-10 bg-obsidian-bg/40 border-b border-white/5 flex items-center px-4">
                                <span className="text-[10px] font-medium text-obsidian-muted/70 uppercase tracking-widest">Capabilities</span>
                            </div>
                            <div className="p-3 grid grid-cols-2 gap-1.5">
                                {CAPABILITIES.map((cap) => (
                                    <div
                                        key={cap.label}
                                        className="p-2.5 rounded-md hover:bg-white/[0.03] transition-colors cursor-default group"
                                    >
                                        <div className="flex items-center gap-2 text-obsidian-muted/60 group-hover:text-foreground/70 transition-colors">
                                            {cap.icon}
                                            <span className="text-[11px] font-medium">{cap.label}</span>
                                        </div>
                                        <div className="text-[10px] text-obsidian-muted/35 mt-0.5 pl-[22px]">{cap.desc}</div>
                                    </div>
                                ))}
                            </div>
                        </div>

                        {/* Execution Logs */}
                        <div className="flex-1 flex flex-col">
                            <div className="h-10 bg-obsidian-bg/40 border-b border-white/5 flex items-center px-4 justify-between shrink-0">
                                <span className="text-[10px] font-medium text-obsidian-muted/70 uppercase tracking-widest">Activity Log</span>
                                <button
                                    onClick={clearChat}
                                    className="p-1.5 text-obsidian-muted/40 hover:text-obsidian-danger hover:bg-white/5 rounded-md transition-all active:scale-95"
                                    title="Clear chat"
                                >
                                    <Trash2 className="w-3.5 h-3.5" />
                                </button>
                            </div>
                            <div className="flex-1 overflow-auto p-4 font-mono text-[11px] text-foreground/60 space-y-1 custom-scrollbar">
                                {messages.filter(m => m.role !== 'user').map((msg, i) => (
                                    <div key={i} className="hover:bg-white/[0.02] px-2 py-1 -mx-2 rounded transition-colors">
                                        <span className="text-obsidian-muted/35">[{msg.timestamp}]</span>{' '}
                                        {msg.toolCall ? (
                                            <>
                                                <span className="text-obsidian-info/60 font-medium">[TOOL]</span>{' '}
                                                {msg.toolCall.name}: {msg.toolCall.output || msg.toolCall.status}
                                            </>
                                        ) : (
                                            <>
                                                <span className="text-obsidian-success/60 font-medium">[MSG]</span>{' '}
                                                {msg.content.substring(0, 60)}...
                                            </>
                                        )}
                                    </div>
                                ))}

                                {isThinking && (
                                    <div className="hover:bg-white/[0.02] px-2 py-1 -mx-2 rounded transition-colors">
                                        <span className="text-obsidian-muted/35">[now]</span>{' '}
                                        <span className="text-obsidian-warning/60 font-medium">[EXEC]</span>{' '}
                                        Processing request...
                                    </div>
                                )}

                                <div className="flex items-center text-foreground/30 mt-3 px-2 -mx-2">
                                    <span className="mr-2">❯</span>
                                    <span className="w-[2px] h-3.5 bg-foreground/15 animate-pulse" />
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* ─── Status Bar ─── */}
                <div className="h-8 bg-black/40 backdrop-blur-md border-t border-white/5 flex items-center px-4 text-[10px] text-obsidian-muted gap-5 shrink-0 z-10 relative">
                    <div className="flex items-center gap-1.5">
                        <Bot className="w-3 h-3 text-foreground/50" />
                        <span className="text-foreground/80 font-medium">ClawdBot</span> active
                    </div>
                    <div className="flex items-center gap-1.5">
                        <Zap className="w-3 h-3 text-obsidian-success/70" />
                        <span className="text-obsidian-success/80 font-medium">7</span> MCP tools
                    </div>
                    <div className="flex items-center gap-1.5">
                        <Activity className="w-3 h-3 text-obsidian-info/70" />
                        <span className="text-obsidian-info/80 font-medium">{messages.length}</span> messages
                    </div>
                    <div className="ml-auto text-obsidian-muted/40 font-mono">
                        model: claude-4 · provider: anthropic
                    </div>
                </div>
            </main>
        </div>
    );
}
