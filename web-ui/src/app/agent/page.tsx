
import React from 'react';
import { Sidebar } from '@/components/Sidebar';
import { Bot, Send, Terminal, Trash2 } from 'lucide-react';

export default function AgentPage() {
    return (
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            <Sidebar />
            <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
                {/* Header */}
                <header className="h-10 bg-black/60 border-b border-obsidian-border/30 flex items-center justify-between px-5 shrink-0 shadow-sm transition-all relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-obsidian-info to-obsidian-purple opacity-40"></div>
                    <div className="flex items-center gap-2">
                        <Terminal className="w-4 h-4 text-obsidian-purple drop-shadow-[0_0_8px_rgba(199,146,234,0.5)]" />
                        <span className="text-[12px] font-bold text-foreground uppercase tracking-widest">Agent Control Center</span>
                    </div>
                    <div className="flex items-center px-3 py-1 bg-obsidian-panel-hover border border-obsidian-border rounded-md text-[11px] font-medium text-foreground gap-2 transition-all shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]">
                        <span className="w-2 h-2 rounded-full bg-obsidian-success animate-pulse shadow-[0_0_8px_rgba(76,175,80,0.8)]"></span>
                        ClawdBot: ONLINE
                    </div>
                </header>

                <div className="flex-1 flex min-h-0 bg-obsidian-bg">

                    {/* Chat / Terminal Area */}
                    <div className="flex-[3] flex flex-col border-r border-obsidian-border/50 relative">
                        <div className="flex-1 p-6 font-mono text-[13px] overflow-auto space-y-6 custom-scrollbar">
                            {/* System Message */}
                            <div className="flex items-start gap-4 group">
                                <Bot className="w-6 h-6 text-obsidian-info mt-1 shrink-0 drop-shadow-[0_0_10px_rgba(34,211,238,0.5)] transition-all group-hover:scale-110" />
                                <div className="flex flex-col gap-1 max-w-[80%]">
                                    <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">System</span>
                                    <div className="text-foreground text-[14px] leading-relax">Hello! I am ready to manage your data platform. I can deploy pipelines, optimize Trino queries, or debug Airflow DAGs.</div>
                                </div>
                            </div>

                            {/* User Message */}
                            <div className="flex items-start gap-4 flex-row-reverse group border-transparent border-b hover:border-obsidian-border/20 pb-2 transition-all">
                                <div className="w-7 h-7 rounded-sm bg-gradient-to-br from-[#bcbec4] to-[#6c707e] flex items-center justify-center text-[10px] text-obsidian-bg font-bold shrink-0 shadow-[0_4px_12px_rgba(0,0,0,0.5)] transition-transform group-hover:scale-105">YK</div>
                                <div className="flex flex-col gap-1 max-w-[80%] items-end">
                                    <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">You</span>
                                    <div className="text-foreground bg-obsidian-panel/80 backdrop-blur-md px-4 py-3 rounded-lg border border-obsidian-border shadow-[0_8px_24px_rgba(0,0,0,0.4)] text-[14px]">Check the status of the bronze ingestion pipeline.</div>
                                </div>
                            </div>

                            {/* Response */}
                            <div className="flex items-start gap-4 group">
                                <Bot className="w-6 h-6 text-obsidian-info mt-1 shrink-0 drop-shadow-[0_0_10px_rgba(34,211,238,0.5)] transition-all group-hover:scale-110" />
                                <div className="flex flex-col gap-1 max-w-[80%]">
                                    <span className="text-[10px] text-obsidian-muted font-bold uppercase tracking-widest">ClawdBot</span>
                                    <div className="text-foreground text-[14px] leading-relaxed bg-black/20 p-4 rounded-lg border border-obsidian-border/30">
                                        Checking Airflow DAG <span className="text-obsidian-purple glow-text font-bold">data_ingestion_bronze</span>...<br /><br />
                                        Status: <span className="text-obsidian-info font-bold drop-shadow-[0_0_8px_rgba(34,211,238,0.6)]">Running</span><br />
                                        Duration: 4m 32s
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Input Area */}
                        <div className="p-4 bg-obsidian-bg">
                            <div className="h-14 bg-obsidian-panel/80 backdrop-blur-md border border-obsidian-border rounded-xl flex items-center px-4 gap-3 shadow-[0_8px_24px_rgba(0,0,0,0.5)] transition-all focus-within:border-obsidian-info focus-within:shadow-[0_8px_32px_rgba(34,211,238,0.2)]">
                                <div className="text-obsidian-info font-bold font-mono text-lg animate-pulse">{'>'}</div>
                                <input type="text" className="flex-1 bg-transparent border-none outline-none text-foreground font-mono text-[14px] placeholder-obsidian-muted/50" placeholder="Ask ClawdBot to deploy a pipeline..." />
                                <button className="p-2 rounded-lg bg-obsidian-panel-hover/50 hover:bg-obsidian-info/20 text-obsidian-muted hover:text-obsidian-info transition-all active:scale-90 group">
                                    <Send className="w-4 h-4 group-hover:drop-shadow-[0_0_8px_rgba(34,211,238,0.8)]" />
                                </button>
                            </div>
                        </div>
                    </div>

                    {/* Right Panel: Context/Logs */}
                    <div className="flex-[1.5] flex flex-col bg-black/40">
                        <div className="h-8 bg-black/60 border-b border-obsidian-border/30 flex items-center px-4 justify-between shrink-0 shadow-sm">
                            <span className="text-[10px] font-bold text-obsidian-muted uppercase tracking-widest">Execution Logs</span>
                            <button className="p-1.5 text-obsidian-muted hover:text-obsidian-danger hover:bg-white/5 rounded-md transition-all active:scale-95 group">
                                <Trash2 className="w-3.5 h-3.5 group-hover:drop-shadow-[0_0_8px_rgba(240,113,120,0.6)]" />
                            </button>
                        </div>
                        <div className="flex-1 overflow-auto p-4 font-mono text-[11px] text-foreground space-y-2 custom-scrollbar">
                            <div className="group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-[#546e7a]">[10:45:01]</span> <span className="text-obsidian-success glow-text font-bold">[DEBUG]</span> Connecting to Airflow API...</div>
                            <div className="group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-[#546e7a]">[10:45:02]</span> <span className="text-obsidian-success glow-text font-bold">[DEBUG]</span> Fetching DAG runs for 'bronze'...</div>
                            <div className="group hover:bg-white/5 px-2 -mx-2 rounded transition-colors"><span className="text-[#546e7a]">[10:45:03]</span> <span className="text-[var(--color-obsidian-info)] glow-text font-bold">[INFO]</span> Found 1 active run.</div>

                            <div className="flex items-center text-foreground mt-4 px-2 -mx-2">
                                <span className="mr-2 text-obsidian-info">❯</span>
                                <span className="w-2 h-3.5 bg-obsidian-info/80 animate-pulse shadow-[0_0_8px_rgba(34,211,238,0.5)]"></span>
                            </div>
                        </div>
                    </div>
                </div>
            </main>
        </div>
    );
}
